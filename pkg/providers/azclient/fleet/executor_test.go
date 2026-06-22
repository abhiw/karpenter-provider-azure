/*
Portions Copyright (c) Microsoft Corporation.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package fleet

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/computefleet/armcomputefleet/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"

	"github.com/Azure/karpenter-provider-azure/pkg/apis/v1beta1"
	"github.com/Azure/karpenter-provider-azure/pkg/providers/launchtemplate"
	"github.com/Azure/karpenter-provider-azure/pkg/utils/batcher"
)

// mockPollerHandler is a mock PollingHandler that immediately completes with a result or error.
type mockPollerHandler[T any] struct {
	result *T
	err    error
}

func (h mockPollerHandler[T]) Done() bool                                   { return true }
func (h mockPollerHandler[T]) Poll(_ context.Context) (*http.Response, error) { return nil, nil }
func (h mockPollerHandler[T]) Result(_ context.Context, out *T) error {
	if h.err != nil {
		return h.err
	}
	if h.result != nil {
		*out = *h.result
	}
	return nil
}

func newTestPoller[T any](result *T, err error) (*runtime.Poller[T], error) {
	return runtime.NewPoller(nil, runtime.Pipeline{}, &runtime.NewPollerOptions[T]{
		Handler: mockPollerHandler[T]{result: result, err: err},
	})
}

// --- Mock Fleet API for executor tests ---

type mockFleetAPI struct {
	mu            sync.Mutex
	createCalls   int32
	createDelay   time.Duration
	createErr     error
	createdFleets []string
	onCreateName  func(name string) // optional callback with the fleet name on each PUT
}

func (m *mockFleetAPI) BeginCreateOrUpdate(_ context.Context, _ string, fleetName string, _ armcomputefleet.Fleet, _ *armcomputefleet.FleetsClientBeginCreateOrUpdateOptions) (*runtime.Poller[armcomputefleet.FleetsClientCreateOrUpdateResponse], error) {
	m.mu.Lock()
	m.createCalls++
	m.createdFleets = append(m.createdFleets, fleetName)
	onCreateName := m.onCreateName
	m.mu.Unlock()

	if onCreateName != nil {
		onCreateName(fleetName)
	}

	if m.createDelay > 0 {
		time.Sleep(m.createDelay)
	}
	if m.createErr != nil {
		return nil, m.createErr
	}
	// Return a completed poller with an empty response
	poller, err := newTestPoller(&armcomputefleet.FleetsClientCreateOrUpdateResponse{}, nil)
	if err != nil {
		return nil, err
	}
	return poller, nil
}

func (m *mockFleetAPI) Get(_ context.Context, _ string, _ string, _ *armcomputefleet.FleetsClientGetOptions) (armcomputefleet.FleetsClientGetResponse, error) {
	return armcomputefleet.FleetsClientGetResponse{}, nil
}

func (m *mockFleetAPI) BeginDelete(_ context.Context, _ string, _ string, _ *armcomputefleet.FleetsClientBeginDeleteOptions) (*runtime.Poller[armcomputefleet.FleetsClientDeleteResponse], error) {
	return nil, nil
}

func (m *mockFleetAPI) NewListByResourceGroupPager(_ string, _ *armcomputefleet.FleetsClientListByResourceGroupOptions) *runtime.Pager[armcomputefleet.FleetsClientListByResourceGroupResponse] {
	return nil
}

// mockVMAPIForExecutor returns VMs with the expected fleet-name tag
type mockVMAPIForExecutor struct {
	vmsToReturn     []*armcompute.VirtualMachine
	vmsToReturnFunc func() []*armcompute.VirtualMachine // dynamic alternative
}

func (m *mockVMAPIForExecutor) BeginUpdate(_ context.Context, _ string, _ string, _ armcompute.VirtualMachineUpdate, _ *armcompute.VirtualMachinesClientBeginUpdateOptions) (*runtime.Poller[armcompute.VirtualMachinesClientUpdateResponse], error) {
	return nil, nil
}

func (m *mockVMAPIForExecutor) BeginDelete(_ context.Context, _ string, _ string, _ *armcompute.VirtualMachinesClientBeginDeleteOptions) (*runtime.Poller[armcompute.VirtualMachinesClientDeleteResponse], error) {
	return nil, nil
}

func (m *mockVMAPIForExecutor) NewListPager(_ string, _ *armcompute.VirtualMachinesClientListOptions) *runtime.Pager[armcompute.VirtualMachinesClientListResponse] {
	return runtime.NewPager(runtime.PagingHandler[armcompute.VirtualMachinesClientListResponse]{
		More: func(_ armcompute.VirtualMachinesClientListResponse) bool { return false },
		Fetcher: func(_ context.Context, _ *armcompute.VirtualMachinesClientListResponse) (armcompute.VirtualMachinesClientListResponse, error) {
			vms := m.vmsToReturn
			if m.vmsToReturnFunc != nil {
				vms = m.vmsToReturnFunc()
			}
			return armcompute.VirtualMachinesClientListResponse{
				VirtualMachineListResult: armcompute.VirtualMachineListResult{
					Value: vms,
				},
			}, nil
		},
	})
}

// --- Helper to build a test batch ---

func makeBatch(key string, nodeClaimNames ...string) *batcher.Batch[FleetVMProvisionRequest, FleetBatchResponse] {
	batch := &batcher.Batch[FleetVMProvisionRequest, FleetBatchResponse]{
		ID:  "test-batch",
		Key: key,
	}
	for _, name := range nodeClaimNames {
		req := &batcher.BatchedRequest[FleetVMProvisionRequest, FleetBatchResponse]{
			Key: key,
			Payload: FleetVMProvisionRequest{
				NodeClaimName:   name,
				CapacityType:    karpv1.CapacityTypeOnDemand,
				AcceptableSKUs:  []string{"Standard_D2s_v3"},
				AcceptableZones: []string{"1"},
				InstanceTypes:   map[string]*cloudprovider.InstanceType{"Standard_D2s_v3": {Name: "Standard_D2s_v3"}},
				NodeClaim: &karpv1.NodeClaim{},
				NodeClass: &v1beta1.AKSNodeClass{},
				LaunchTemplate: &launchtemplate.Template{
					ImageID:  "/sub/rg/image",
					SubnetID: "/sub/rg/subnet",
				},
			},
			ResponseChan: make(chan *batcher.Response[FleetBatchResponse], 1),
		}
		batch.Requests = append(batch.Requests, req)
	}
	return batch
}

func collectResponses(batch *batcher.Batch[FleetVMProvisionRequest, FleetBatchResponse]) []FleetBatchResponse {
	var responses []FleetBatchResponse
	for _, req := range batch.Requests {
		select {
		case resp := <-req.ResponseChan:
			responses = append(responses, resp.Payload)
		case <-time.After(5 * time.Second):
			responses = append(responses, FleetBatchResponse{})
		}
	}
	return responses
}

// --- Tests ---

// TestExecutor_Coalescing_SecondBatchCoalesces verifies that a second batch
// for the same key that arrives while the first is in-flight gets coalesced
// (receives ErrFleetCoalesced instead of creating a new Fleet).
func TestExecutor_Coalescing_SecondBatchCoalesces(t *testing.T) {
	g := NewWithT(t)

	fleetName := "test-fleet"
	fleetAPI := &mockFleetAPI{createDelay: 200 * time.Millisecond}
	vmAPI := &mockVMAPIForExecutor{
		vmsToReturn: []*armcompute.VirtualMachine{
			{
				Name: lo.ToPtr("aks_abc_0"),
				Tags: map[string]*string{FleetNameTagKey: &fleetName},
				Properties: &armcompute.VirtualMachineProperties{
					HardwareProfile: &armcompute.HardwareProfile{VMSize: lo.ToPtr(armcompute.VirtualMachineSizeTypes("Standard_D2s_v3"))},
				},
				Zones: []*string{lo.ToPtr("1")},
			},
		},
	}

	exec := &executor{
		fleetClient:      fleetAPI,
		vmClient:         vmAPI,
		clusterName:      "test",
		resourceGroup:    "rg",
		location:         "eastus",
		maxFleetCapacity: 200,
	}

	batchKey := "default/on-demand/abcdef0123456789"

	// Launch batch 1 (the one that should proceed normally)
	batch1 := makeBatch(batchKey, "nc-1")
	// Launch batch 2 with SAME name (simulates provisioner re-trigger for same pod)
	batch2 := makeBatch(batchKey, "nc-1")

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		exec.executeBatch(context.Background(), batch1)
	}()

	// Small delay to ensure batch1 registers as inflight first
	time.Sleep(50 * time.Millisecond)

	go func() {
		defer wg.Done()
		exec.executeBatch(context.Background(), batch2)
	}()

	wg.Wait()

	// Batch 1 should have succeeded (got shared state)
	responses1 := collectResponses(batch1)
	g.Expect(responses1).To(HaveLen(1))
	g.Expect(responses1[0].Error).To(BeNil(), "batch1 should succeed")
	g.Expect(responses1[0].SharedState).NotTo(BeNil())

	// Batch 2 should have been coalesced (got ErrFleetCoalesced)
	responses2 := collectResponses(batch2)
	g.Expect(responses2).To(HaveLen(1))
	g.Expect(responses2[0].Error).NotTo(BeNil(), "batch2 should get error")
	g.Expect(IsFleetCoalescedError(responses2[0].Error)).To(BeTrue(), "error should be ErrFleetCoalesced, got: %v", responses2[0].Error)

	// Only 1 Fleet should have been created
	fleetAPI.mu.Lock()
	g.Expect(fleetAPI.createCalls).To(Equal(int32(1)), "only 1 Fleet PUT should happen")
	fleetAPI.mu.Unlock()
}

// TestExecutor_Coalescing_NewClaimsProceedWhileDuplicatesWait verifies that
// when a batch contains a mix of duplicate NodeClaim names (already inflight)
// and genuinely new names, only the duplicates coalesce — the new ones proceed
// with their own Fleet creation.
func TestExecutor_Coalescing_NewClaimsProceedWhileDuplicatesWait(t *testing.T) {
	g := NewWithT(t)

	// Track fleet names created so vmAPI returns VMs with correct tags
	var createdFleetNames sync.Map

	fleetAPI := &mockFleetAPI{
		createDelay: 200 * time.Millisecond,
		onCreateName: func(name string) {
			createdFleetNames.Store(name, true)
		},
	}
	vmAPI := &mockVMAPIForExecutor{
		vmsToReturnFunc: func() []*armcompute.VirtualMachine {
			// Return VMs for each created fleet
			var vms []*armcompute.VirtualMachine
			i := 0
			createdFleetNames.Range(func(key, _ any) bool {
				name := key.(string)
				vms = append(vms, &armcompute.VirtualMachine{
					Name: lo.ToPtr(fmt.Sprintf("aks_abc_%d", i)),
					Tags: map[string]*string{FleetNameTagKey: lo.ToPtr(name)},
					Properties: &armcompute.VirtualMachineProperties{
						HardwareProfile: &armcompute.HardwareProfile{VMSize: lo.ToPtr(armcompute.VirtualMachineSizeTypes("Standard_D2s_v3"))},
					},
					Zones: []*string{lo.ToPtr("1")},
				})
				i++
				return true
			})
			return vms
		},
	}

	exec := &executor{
		fleetClient:      fleetAPI,
		vmClient:         vmAPI,
		clusterName:      "test",
		resourceGroup:    "rg",
		location:         "eastus",
		maxFleetCapacity: 200,
	}

	batchKey := "default/on-demand/abcdef0123456789"

	// Batch 1: claims nc-1, nc-2, nc-3
	batch1 := makeBatch(batchKey, "nc-1", "nc-2", "nc-3")

	// Batch 2: nc-1, nc-2 are duplicates (re-triggers), nc-4, nc-5 are new
	batch2 := makeBatch(batchKey, "nc-1", "nc-2", "nc-4", "nc-5")

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		exec.executeBatch(context.Background(), batch1)
	}()

	// Small delay to ensure batch1 registers its names first
	time.Sleep(50 * time.Millisecond)

	go func() {
		defer wg.Done()
		exec.executeBatch(context.Background(), batch2)
	}()

	wg.Wait()

	// Batch 1: all 3 requests should succeed
	responses1 := collectResponses(batch1)
	g.Expect(responses1).To(HaveLen(3))
	for i, r := range responses1 {
		g.Expect(r.Error).To(BeNil(), "batch1 request %d should succeed", i)
	}

	// Batch 2: should have 4 responses
	responses2 := collectResponses(batch2)
	g.Expect(responses2).To(HaveLen(4))

	// Categorize: duplicates get ErrFleetCoalesced, new ones get SharedState
	var coalesced, succeeded int
	for _, r := range responses2 {
		if r.Error != nil && IsFleetCoalescedError(r.Error) {
			coalesced++
		} else if r.Error == nil && r.SharedState != nil {
			succeeded++
		}
	}
	g.Expect(coalesced).To(Equal(2), "2 duplicate NodeClaims (nc-1, nc-2) should coalesce")
	g.Expect(succeeded).To(Equal(2), "2 new NodeClaims (nc-4, nc-5) should proceed with new Fleet")

	// 2 Fleet PUTs total: one for batch1, one for the new claims in batch2
	fleetAPI.mu.Lock()
	g.Expect(fleetAPI.createCalls).To(Equal(int32(2)), "2 Fleet PUTs: original + new claims")
	fleetAPI.mu.Unlock()
}

// TestExecutor_Coalescing_AfterLROCompletes_CooldownBlocksDuplicates verifies
// that after a successful LRO, the inflight entry stays for cooldown and new
// batches for the same key get coalesced (ICE'd) to prevent duplicate VMs.
func TestExecutor_Coalescing_AfterLROCompletes_CooldownBlocksDuplicates(t *testing.T) {
	g := NewWithT(t)

	// Use a dynamic VM mock that returns VMs with the correct fleet tag
	var vmMu sync.Mutex
	var vmTag string

	fleetAPI := &mockFleetAPI{
		createDelay: 50 * time.Millisecond,
		onCreateName: func(name string) {
			vmMu.Lock()
			vmTag = name
			vmMu.Unlock()
		},
	}
	vmAPI := &mockVMAPIForExecutor{
		vmsToReturnFunc: func() []*armcompute.VirtualMachine {
			vmMu.Lock()
			tag := vmTag
			vmMu.Unlock()
			if tag == "" {
				return nil
			}
			return []*armcompute.VirtualMachine{
				{
					Name: lo.ToPtr("aks_abc_0"),
					Tags: map[string]*string{FleetNameTagKey: lo.ToPtr(tag)},
					Properties: &armcompute.VirtualMachineProperties{
						HardwareProfile: &armcompute.HardwareProfile{VMSize: lo.ToPtr(armcompute.VirtualMachineSizeTypes("Standard_D2s_v3"))},
					},
					Zones: []*string{lo.ToPtr("1")},
				},
			}
		},
	}

	exec := &executor{
		fleetClient:      fleetAPI,
		vmClient:         vmAPI,
		clusterName:      "test",
		resourceGroup:    "rg",
		location:         "eastus",
		maxFleetCapacity: 200,
	}

	batchKey := "default/on-demand/abcdef0123456789"

	// Run batch 1 to completion (fully fulfilled: 1 VM for 1 request)
	batch1 := makeBatch(batchKey, "nc-1")
	exec.executeBatch(context.Background(), batch1)

	responses1 := collectResponses(batch1)
	g.Expect(responses1[0].Error).To(BeNil())

	// After batch1 succeeds with full fulfillment, cooldown is active.
	// batch2 with SAME name should be coalesced (prevents duplicate VMs).
	batch2 := makeBatch(batchKey, "nc-1")
	exec.executeBatch(context.Background(), batch2)

	responses2 := collectResponses(batch2)
	g.Expect(responses2[0].Error).NotTo(BeNil(), "batch2 should be coalesced during cooldown")
	g.Expect(IsFleetCoalescedError(responses2[0].Error)).To(BeTrue())

	// Only 1 Fleet should have been created
	fleetAPI.mu.Lock()
	g.Expect(fleetAPI.createCalls).To(Equal(int32(1)), "only 1 Fleet PUT during cooldown")
	fleetAPI.mu.Unlock()
}

// TestExecutor_Coalescing_DifferentKeysRunInParallel verifies that batches
// with different keys are NOT coalesced — they run independently in parallel.
func TestExecutor_Coalescing_DifferentKeysRunInParallel(t *testing.T) {
	g := NewWithT(t)

	var createCount atomic.Int32
	fleetAPI := &mockFleetAPI{createDelay: 100 * time.Millisecond}
	vmAPI := &mockVMAPIForExecutor{
		vmsToReturn: []*armcompute.VirtualMachine{}, // empty — no VMs assigned
	}

	exec := &executor{
		fleetClient:      fleetAPI,
		vmClient:         vmAPI,
		clusterName:      "test",
		resourceGroup:    "rg",
		location:         "eastus",
		maxFleetCapacity: 200,
	}

	batch1 := makeBatch("poolA/on-demand/aaaa0000aaaa0000", "nc-1")
	batch2 := makeBatch("poolB/on-demand/bbbb1111bbbb1111", "nc-2")

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		exec.executeBatch(context.Background(), batch1)
	}()
	go func() {
		defer wg.Done()
		exec.executeBatch(context.Background(), batch2)
	}()

	wg.Wait()
	_ = createCount

	// Both should have proceeded — 2 Fleet PUTs
	fleetAPI.mu.Lock()
	g.Expect(fleetAPI.createCalls).To(Equal(int32(2)), "different keys should create separate Fleets")
	fleetAPI.mu.Unlock()
}

// TestExecutor_BatchSplitting verifies that a batch larger than maxFleetCapacity
// is split into multiple parallel sub-batches, each creating its own Fleet.
func TestExecutor_BatchSplitting(t *testing.T) {
	g := NewWithT(t)

	fleetAPI := &mockFleetAPI{}
	vmAPI := &mockVMAPIForExecutor{
		vmsToReturn: []*armcompute.VirtualMachine{}, // empty — surplus will be 0
	}

	exec := &executor{
		fleetClient:      fleetAPI,
		vmClient:         vmAPI,
		clusterName:      "test",
		resourceGroup:    "rg",
		location:         "eastus",
		maxFleetCapacity: 3, // small for testing
	}

	batchKey := "default/on-demand/abcdef0123456789"

	// Create a batch with 7 requests → should split into 3 sub-batches (3+3+1)
	names := []string{"nc-1", "nc-2", "nc-3", "nc-4", "nc-5", "nc-6", "nc-7"}
	batch := makeBatch(batchKey, names...)

	exec.executeBatch(context.Background(), batch)

	// Should have created 3 Fleet resources
	fleetAPI.mu.Lock()
	g.Expect(fleetAPI.createCalls).To(Equal(int32(3)), "7 requests / maxCapacity 3 = 3 Fleet PUTs")
	fleetAPI.mu.Unlock()

	// All 7 requests should have received responses (shared state, even if empty)
	responses := collectResponses(batch)
	g.Expect(responses).To(HaveLen(7))
}

// TestExecutor_ErrFleetCoalesced_IsDetectable verifies the sentinel error can be
// detected via IsFleetCoalescedError after wrapping with fmt.Errorf.
func TestExecutor_ErrFleetCoalesced_IsDetectable(t *testing.T) {
	g := NewWithT(t)

	wrapped := fmt.Errorf("batch key X: %w", ErrFleetCoalesced)
	g.Expect(IsFleetCoalescedError(wrapped)).To(BeTrue())
	g.Expect(IsFleetCoalescedError(ErrFleetCoalesced)).To(BeTrue())
	g.Expect(IsFleetCoalescedError(fmt.Errorf("unrelated error"))).To(BeFalse())
	g.Expect(IsFleetCoalescedError(nil)).To(BeFalse())
}
