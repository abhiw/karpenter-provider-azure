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
	"net/http"
	"sync"
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
	"github.com/Azure/karpenter-provider-azure/pkg/utils/zones"
)

// mockPollerHandler is a mock PollingHandler that immediately completes with a result or error.
type mockPollerHandler[T any] struct {
	result *T
	err    error
}

func (h mockPollerHandler[T]) Done() bool                                     { return true }
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
	onCreateName  func(name string)                 // optional callback with the fleet name on each PUT
	listVMs       []*armcomputefleet.VirtualMachine // VMs returned by NewListVirtualMachinesPager, if set
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
	return runtime.NewPager(runtime.PagingHandler[armcomputefleet.FleetsClientListByResourceGroupResponse]{
		More: func(_ armcomputefleet.FleetsClientListByResourceGroupResponse) bool { return false },
		Fetcher: func(_ context.Context, _ *armcomputefleet.FleetsClientListByResourceGroupResponse) (armcomputefleet.FleetsClientListByResourceGroupResponse, error) {
			return armcomputefleet.FleetsClientListByResourceGroupResponse{}, nil
		},
	})
}

func (m *mockFleetAPI) NewListVirtualMachinesPager(_ string, _ string, _ *armcomputefleet.FleetsClientListVirtualMachinesOptions) *runtime.Pager[armcomputefleet.FleetsClientListVirtualMachinesResponse] {
	return runtime.NewPager(runtime.PagingHandler[armcomputefleet.FleetsClientListVirtualMachinesResponse]{
		More: func(_ armcomputefleet.FleetsClientListVirtualMachinesResponse) bool { return false },
		Fetcher: func(_ context.Context, _ *armcomputefleet.FleetsClientListVirtualMachinesResponse) (armcomputefleet.FleetsClientListVirtualMachinesResponse, error) {
			return armcomputefleet.FleetsClientListVirtualMachinesResponse{
				VirtualMachineListResult: armcomputefleet.VirtualMachineListResult{
					Value: m.listVMs,
				},
			}, nil
		},
	})
}

// mockVMAPIForExecutor returns VMs with the expected fleet-name tag
type mockVMAPIForExecutor struct {
	vmsToReturn     []*armcompute.VirtualMachine
	vmsToReturnFunc func() []*armcompute.VirtualMachine // dynamic alternative
}

func (m *mockVMAPIForExecutor) BeginUpdate(_ context.Context, _ string, _ string, _ armcompute.VirtualMachineUpdate, _ *armcompute.VirtualMachinesClientBeginUpdateOptions) (*runtime.Poller[armcompute.VirtualMachinesClientUpdateResponse], error) {
	return nil, nil
}

func (m *mockVMAPIForExecutor) Get(_ context.Context, _ string, _ string, _ *armcompute.VirtualMachinesClientGetOptions) (armcompute.VirtualMachinesClientGetResponse, error) {
	return armcompute.VirtualMachinesClientGetResponse{}, nil
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
				NodeClaim:       &karpv1.NodeClaim{},
				NodeClass:       &v1beta1.AKSNodeClass{},
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

// TestExecutor_DifferentKeysRunInParallel verifies that batches
// with different keys run independently in parallel.
func TestExecutor_DifferentKeysRunInParallel(t *testing.T) {
	g := NewWithT(t)

	fleetAPI := &mockFleetAPI{createDelay: 100 * time.Millisecond}
	vmAPI := &mockVMAPIForExecutor{
		vmsToReturn: []*armcompute.VirtualMachine{},
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

// --- Tests for getVMWaitingForVisibility (retry-on-404 for ARM propagation lag) ---

// TestListFleetVMs_UsesFleetSDKFields verifies that listFleetVMs converts Fleet SDK
// VirtualMachine fields (ID, Name, VMSize, Zone) directly to armcompute.VirtualMachine
// without any compute GET calls, and that Location is populated from the executor's
// own location field so that zonal VMs resolve to valid AKS zone labels.
func TestListFleetVMs_UsesFleetSDKFields(t *testing.T) {
	g := NewWithT(t)

	fleetAPI := &mockFleetAPI{
		listVMs: []*armcomputefleet.VirtualMachine{
			{
				Name:   lo.ToPtr("vm-1"),
				ID:     lo.ToPtr("/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/vm-1"),
				VMSize: lo.ToPtr("Standard_D2s_v3"),
				Zone:   lo.ToPtr("1"),
			},
			{
				Name:   lo.ToPtr("vm-2"),
				ID:     lo.ToPtr("/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/vm-2"),
				VMSize: lo.ToPtr("Standard_D4s_v3"),
				// No zone — non-zonal VM
			},
		},
	}

	exec := &executor{
		fleetClient:   fleetAPI,
		resourceGroup: "rg",
		location:      "southcentralus",
	}

	vms, err := exec.listFleetVMs(context.Background(), "some-fleet")
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(vms).To(HaveLen(2))

	// VM 1: zonal, with VMSize
	g.Expect(lo.FromPtr(vms[0].Name)).To(Equal("vm-1"))
	g.Expect(lo.FromPtr(vms[0].ID)).To(Equal("/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/vm-1"))
	g.Expect(string(lo.FromPtr(vms[0].Properties.HardwareProfile.VMSize))).To(Equal("Standard_D2s_v3"))
	g.Expect(vms[0].Zones).To(HaveLen(1))
	g.Expect(lo.FromPtr(vms[0].Zones[0])).To(Equal("1"))
	g.Expect(lo.FromPtr(vms[0].Location)).To(Equal("southcentralus"), "Location must come from executor")

	// Verify zone label resolves correctly
	zone, err := zones.MakeAKSLabelZoneFromVM(vms[0])
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(zone).To(Equal("southcentralus-1"))

	// VM 2: non-zonal, different VMSize
	g.Expect(lo.FromPtr(vms[1].Name)).To(Equal("vm-2"))
	g.Expect(string(lo.FromPtr(vms[1].Properties.HardwareProfile.VMSize))).To(Equal("Standard_D4s_v3"))
	g.Expect(vms[1].Zones).To(BeEmpty())
	g.Expect(lo.FromPtr(vms[1].Location)).To(Equal("southcentralus"))
}
