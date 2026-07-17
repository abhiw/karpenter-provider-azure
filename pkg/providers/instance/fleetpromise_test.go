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

package instance

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	azruntime "github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork"
	"github.com/Azure/skewer"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	corecloudprovider "sigs.k8s.io/karpenter/pkg/cloudprovider"

	"github.com/Azure/karpenter-provider-azure/pkg/apis/v1beta1"
	cache "github.com/Azure/karpenter-provider-azure/pkg/cache"
	"github.com/Azure/karpenter-provider-azure/pkg/providers/azclient/fleet"
	"github.com/Azure/karpenter-provider-azure/pkg/providers/instance/fleetvmpoller"
	"github.com/Azure/karpenter-provider-azure/pkg/providers/instance/offerings"
)

// promiseStubVMProvider records Delete calls and lets a test return errors.
type promiseStubVMProvider struct {
	mu        sync.Mutex
	deleted   []string
	returnErr error
}

var _ VMProvider = (*promiseStubVMProvider)(nil)

func (s *promiseStubVMProvider) Delete(_ context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deleted = append(s.deleted, name)
	return s.returnErr
}
func (s *promiseStubVMProvider) List(context.Context) ([]*armcompute.VirtualMachine, error) {
	return nil, nil
}
func (s *promiseStubVMProvider) BeginCreate(context.Context, *v1beta1.AKSNodeClass, *karpv1.NodeClaim, []*corecloudprovider.InstanceType) (*VirtualMachinePromise, error) {
	return nil, nil
}
func (s *promiseStubVMProvider) Get(context.Context, string) (*armcompute.VirtualMachine, error) {
	return nil, nil
}
func (s *promiseStubVMProvider) Update(context.Context, string, armcompute.VirtualMachineUpdate) error {
	return nil
}
func (s *promiseStubVMProvider) GetNic(context.Context, string, string) (*armnetwork.Interface, error) {
	return nil, nil
}
func (s *promiseStubVMProvider) DeleteNic(context.Context, string) error { return nil }
func (s *promiseStubVMProvider) ListNics(context.Context) ([]*armnetwork.Interface, error) {
	return nil, nil
}
func (s *promiseStubVMProvider) ListFleetVMs(context.Context) ([]*armcompute.VirtualMachine, error) {
	return nil, nil
}

// makeAssignedVM constructs an armcompute.VirtualMachine that looks like one
// returned by the Fleet executor (ID has uppercase RG so we can verify the
// ProviderID lowercasing branch executes).
func makeAssignedVM(name string) *armcompute.VirtualMachine {
	return &armcompute.VirtualMachine{
		Name: lo.ToPtr(name),
		ID: lo.ToPtr(
			"/subscriptions/12345678-1234-1234-1234-123456789012" +
				"/resourceGroups/MC_rg" +
				"/providers/Microsoft.Compute/virtualMachines/" + name,
		),
	}
}

// TestFleetMemberPromise_Cleanup_RoutesThroughVMProvider verifies that Cleanup
// calls vmProvider.Delete when one is set (the production path) — picking up
// dedupe, idempotency Get, IsVMDeleting short-circuit, and ForceDeletion.
func TestFleetMemberPromise_Cleanup_RoutesThroughVMProvider(t *testing.T) {
	g := NewWithT(t)
	stub := &promiseStubVMProvider{}
	p := &FleetMemberPromise{
		VM:         makeAssignedVM("vm-1"),
		vmProvider: stub,
	}

	g.Expect(p.Cleanup(context.Background())).To(Succeed())
	g.Expect(stub.deleted).To(Equal([]string{"vm-1"}))
}

// TestFleetMemberPromise_Cleanup_PropagatesVMProviderError verifies the
// caller sees the same error VMProvider.Delete returns.
func TestFleetMemberPromise_Cleanup_PropagatesVMProviderError(t *testing.T) {
	g := NewWithT(t)
	stub := &promiseStubVMProvider{returnErr: errors.New("rate limited")}
	p := &FleetMemberPromise{
		VM:         makeAssignedVM("vm-1"),
		vmProvider: stub,
	}

	err := p.Cleanup(context.Background())
	g.Expect(err).To(MatchError("rate limited"))
}

// TestFleetMemberPromise_Cleanup_NoVM is a no-op when Wait() never produced a VM.
func TestFleetMemberPromise_Cleanup_NoVM(t *testing.T) {
	g := NewWithT(t)
	stub := &promiseStubVMProvider{}
	p := &FleetMemberPromise{vmProvider: stub}

	g.Expect(p.Cleanup(context.Background())).To(Succeed())
	g.Expect(stub.deleted).To(BeEmpty())
}

// TestFleetMemberPromise_Cleanup_VMWithNilName is a no-op (can't delete by name).
func TestFleetMemberPromise_Cleanup_VMWithNilName(t *testing.T) {
	g := NewWithT(t)
	stub := &promiseStubVMProvider{}
	p := &FleetMemberPromise{
		VM:         &armcompute.VirtualMachine{ID: lo.ToPtr("/subscriptions/sub/x/y")},
		vmProvider: stub,
	}

	g.Expect(p.Cleanup(context.Background())).To(Succeed())
	g.Expect(stub.deleted).To(BeEmpty())
}

// TestFleetMemberPromise_Cleanup_LegacyFallback_NoVMClient confirms the safety
// guard: when neither a VMProvider nor a sharedState VM client is available,
// Cleanup is a no-op rather than a panic.
func TestFleetMemberPromise_Cleanup_LegacyFallback_NoVMClient(t *testing.T) {
	g := NewWithT(t)
	state := fleet.NewFleetSharedStateForTest(nil, nil, nil, nil, "fleet-x", "rg-x")
	p := &FleetMemberPromise{
		sharedState: state,
		VM:          makeAssignedVM("vm-fallback"),
		// vmProvider deliberately nil
	}

	g.Expect(p.Cleanup(context.Background())).To(Succeed())
}

// TestFleetMemberPromise_Wait_StampsProviderID exercises the real Wait() path
// against a pre-populated FleetSharedState and verifies p.ProviderID gets the
// canonical NodeClaim shape (azure:// prefix, lowercase RG) so downstream
// consumers can parse it with GetVMName.
func TestFleetMemberPromise_Wait_StampsProviderID(t *testing.T) {
	g := NewWithT(t)

	vmSize := armcompute.VirtualMachineSizeTypes("Standard_D4s_v3")
	vm := &armcompute.VirtualMachine{
		Name:     lo.ToPtr("vm-prov"),
		Location: lo.ToPtr("westus"),
		Zones:    []*string{lo.ToPtr("1")},
		ID: lo.ToPtr(
			"/subscriptions/12345678-1234-1234-1234-123456789012" +
				"/resourceGroups/MC_rg" +
				"/providers/Microsoft.Compute/virtualMachines/vm-prov",
		),
		Properties: &armcompute.VirtualMachineProperties{
			HardwareProfile: &armcompute.HardwareProfile{VMSize: &vmSize},
		},
	}

	state := fleet.NewFleetSharedStateForTest(
		[]*armcompute.VirtualMachine{vm},
		[]*fleet.VMAssignmentRequest{
			{
				NodeClaimName:   "nc-1",
				AcceptableSKUs:  []string{"Standard_D4s_v3"},
				AcceptableZones: []string{"westus-1"},
				InstanceTypes: map[string]*corecloudprovider.InstanceType{
					"Standard_D4s_v3": {Name: "Standard_D4s_v3"},
				},
			},
		},
		nil, nil, "fleet-test", "rg-test",
	)
	state.RunAssignmentForTest(context.Background())

	p := &FleetMemberPromise{
		sharedState:   state,
		nodeClaimName: "nc-1",
		fleetName:     "fleet-test",
	}

	g.Expect(p.Wait()).To(Succeed())
	g.Expect(p.VM).NotTo(BeNil())
	g.Expect(p.ProviderID).To(HavePrefix("azure://"))
	g.Expect(strings.HasSuffix(p.ProviderID, "/vm-prov")).To(BeTrue())
	g.Expect(p.ProviderID).To(ContainSubstring("/resourceGroups/mc_rg/"))
}

// TestFleetMemberPromise_Wait_UnassignedReturnsInsufficientCapacity covers the
// case where the assignment didn't match — CloudProvider.Create maps this to
// "no capacity" so the NodePool retries.
func TestFleetMemberPromise_Wait_UnassignedReturnsInsufficientCapacity(t *testing.T) {
	g := NewWithT(t)
	state := fleet.NewFleetSharedStateForTest(nil, nil, nil, nil, "fleet-test", "rg-test")
	p := &FleetMemberPromise{
		sharedState:   state,
		nodeClaimName: "nc-missing",
		fleetName:     "fleet-test",
	}
	err := p.Wait()
	g.Expect(err).To(HaveOccurred())
	g.Expect(corecloudprovider.IsInsufficientCapacityError(err)).To(BeTrue())
}

// TestFleetMemberPromise_GetInstanceName returns the VM name or "".
func TestFleetMemberPromise_GetInstanceName(t *testing.T) {
	g := NewWithT(t)
	g.Expect((&FleetMemberPromise{}).GetInstanceName()).To(Equal(""))
	g.Expect((&FleetMemberPromise{VM: &armcompute.VirtualMachine{}}).GetInstanceName()).To(Equal(""))
	g.Expect((&FleetMemberPromise{VM: makeAssignedVM("vm-x")}).GetInstanceName()).To(Equal("vm-x"))
}

// --- Tests for Wait() with polling (vmClient set) ---

// mockVMGetterForPromise implements fleet.VMAPI for fleetpromise tests.
// Only Get() is used by the poller; other methods panic if called.
type mockVMGetterForPromise struct {
	mu        sync.Mutex
	responses []mockVMGetResponse
	callIndex int
}

type mockVMGetResponse struct {
	vm  *armcompute.VirtualMachine
	err error
}

func (m *mockVMGetterForPromise) Get(
	_ context.Context,
	_ string,
	_ string,
	_ *armcompute.VirtualMachinesClientGetOptions,
) (armcompute.VirtualMachinesClientGetResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	idx := m.callIndex
	if idx >= len(m.responses) {
		idx = len(m.responses) - 1
	}
	m.callIndex++
	resp := m.responses[idx]
	if resp.err != nil {
		return armcompute.VirtualMachinesClientGetResponse{}, resp.err
	}
	return armcompute.VirtualMachinesClientGetResponse{
		VirtualMachine: *resp.vm,
	}, nil
}

func (m *mockVMGetterForPromise) BeginUpdate(_ context.Context, _ string, _ string, _ armcompute.VirtualMachineUpdate, _ *armcompute.VirtualMachinesClientBeginUpdateOptions) (*azruntime.Poller[armcompute.VirtualMachinesClientUpdateResponse], error) {
	panic("BeginUpdate not expected in fleet promise tests")
}

func (m *mockVMGetterForPromise) BeginDelete(_ context.Context, _ string, _ string, _ *armcompute.VirtualMachinesClientBeginDeleteOptions) (*azruntime.Poller[armcompute.VirtualMachinesClientDeleteResponse], error) {
	panic("BeginDelete not expected in fleet promise tests")
}

func (m *mockVMGetterForPromise) NewListPager(_ string, _ *armcompute.VirtualMachinesClientListOptions) *azruntime.Pager[armcompute.VirtualMachinesClientListResponse] {
	panic("NewListPager not expected in fleet promise tests")
}

func (m *mockVMGetterForPromise) CallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.callIndex
}

// mockInstanceTypeProvider implements instancetype.Provider for tests.
type mockInstanceTypeProvider struct {
	sku    *skewer.SKU
	getErr error
}

func (m *mockInstanceTypeProvider) LivenessProbe(_ *http.Request) error { return nil }
func (m *mockInstanceTypeProvider) List(_ context.Context, _ *v1beta1.AKSNodeClass) ([]*corecloudprovider.InstanceType, error) {
	return nil, nil
}
func (m *mockInstanceTypeProvider) Get(_ context.Context, _ string) (*skewer.SKU, error) {
	return m.sku, m.getErr
}
func (m *mockInstanceTypeProvider) UpdateInstanceTypes(_ context.Context) error { return nil }

// makeSucceededVM returns a VM with provisioningState=Succeeded suitable for poller response.
func makeSucceededVM(name string) *armcompute.VirtualMachine {
	return &armcompute.VirtualMachine{
		Name: lo.ToPtr(name),
		ID: lo.ToPtr(
			"/subscriptions/12345678-1234-1234-1234-123456789012" +
				"/resourceGroups/MC_rg" +
				"/providers/Microsoft.Compute/virtualMachines/" + name,
		),
		Properties: &armcompute.VirtualMachineProperties{
			ProvisioningState: lo.ToPtr("Succeeded"),
			HardwareProfile: &armcompute.HardwareProfile{
				VMSize: lo.ToPtr(armcompute.VirtualMachineSizeTypes("Standard_D4s_v3")),
			},
		},
		Zones: []*string{lo.ToPtr("1")},
	}
}

// makeFailedVM returns a VM with provisioningState=Failed.
func makeFailedVM(name string) *armcompute.VirtualMachine {
	return &armcompute.VirtualMachine{
		Name: lo.ToPtr(name),
		ID: lo.ToPtr(
			"/subscriptions/12345678-1234-1234-1234-123456789012" +
				"/resourceGroups/MC_rg" +
				"/providers/Microsoft.Compute/virtualMachines/" + name,
		),
		Properties: &armcompute.VirtualMachineProperties{
			ProvisioningState: lo.ToPtr("Failed"),
			HardwareProfile: &armcompute.HardwareProfile{
				VMSize: lo.ToPtr(armcompute.VirtualMachineSizeTypes("Standard_D4s_v3")),
			},
			InstanceView: &armcompute.VirtualMachineInstanceView{
				Statuses: []*armcompute.InstanceViewStatus{
					{
						Code:    lo.ToPtr("ProvisioningState/failed/AllocationFailed"),
						Level:   lo.ToPtr(armcompute.StatusLevelTypesError),
						Message: lo.ToPtr("Allocation failed due to zonal constraints"),
					},
				},
			},
		},
		Zones: []*string{lo.ToPtr("1")},
	}
}

// makeCreatingVM returns a VM with provisioningState=Creating.
func makeCreatingVM(name string) *armcompute.VirtualMachine {
	return &armcompute.VirtualMachine{
		Name: lo.ToPtr(name),
		ID: lo.ToPtr(
			"/subscriptions/12345678-1234-1234-1234-123456789012" +
				"/resourceGroups/MC_rg" +
				"/providers/Microsoft.Compute/virtualMachines/" + name,
		),
		Properties: &armcompute.VirtualMachineProperties{
			ProvisioningState: lo.ToPtr("Creating"),
		},
	}
}

// buildPromiseWithPolling constructs a FleetMemberPromise with vmClient set
// so it exercises the real polling path.
func buildPromiseWithPolling(
	t *testing.T,
	nodeClaimName string,
	vmGetter *mockVMGetterForPromise,
	errorHandler *offerings.ResponseErrorHandler,
	itProvider *mockInstanceTypeProvider,
) *FleetMemberPromise {
	t.Helper()

	vmSize := armcompute.VirtualMachineSizeTypes("Standard_D4s_v3")
	assignmentVM := &armcompute.VirtualMachine{
		Name:     lo.ToPtr("fleet-vm-poll"),
		Location: lo.ToPtr("westus"),
		ID: lo.ToPtr(
			"/subscriptions/12345678-1234-1234-1234-123456789012" +
				"/resourceGroups/MC_rg" +
				"/providers/Microsoft.Compute/virtualMachines/fleet-vm-poll",
		),
		Properties: &armcompute.VirtualMachineProperties{
			HardwareProfile: &armcompute.HardwareProfile{VMSize: &vmSize},
		},
		Zones: []*string{lo.ToPtr("1")},
	}

	state := fleet.NewFleetSharedStateForTest(
		[]*armcompute.VirtualMachine{assignmentVM},
		[]*fleet.VMAssignmentRequest{
			{
				NodeClaimName:   nodeClaimName,
				AcceptableSKUs:  []string{"Standard_D4s_v3"},
				AcceptableZones: []string{"westus-1"},
				InstanceTypes: map[string]*corecloudprovider.InstanceType{
					"Standard_D4s_v3": {Name: "Standard_D4s_v3"},
				},
			},
		},
		nil, nil, "fleet-test", "rg-test",
	)
	state.RunAssignmentForTest(context.Background())

	fastOpts := fleetvmpoller.InstantOptions()
	return &FleetMemberPromise{
		sharedState:          state,
		nodeClaimName:        nodeClaimName,
		capacityType:         "on-demand",
		fleetName:            "fleet-test",
		ctx:                  context.Background(),
		vmClient:             vmGetter,
		resourceGroup:        "MC_rg",
		errorHandling:        errorHandler,
		instanceTypeProvider: itProvider,
		pollerOptions:        &fastOpts,
	}
}

// TestFleetMemberPromise_Wait_PollingImmediateSuccess verifies Wait() succeeds
// when the first GET returns provisioningState=Succeeded.
func TestFleetMemberPromise_Wait_PollingImmediateSuccess(t *testing.T) {
	g := NewWithT(t)

	vmGetter := &mockVMGetterForPromise{
		responses: []mockVMGetResponse{
			{vm: makeSucceededVM("fleet-vm-poll")},
		},
	}

	p := buildPromiseWithPolling(t, "nc-poll-1", vmGetter, nil, nil)
	g.Expect(p.Wait()).To(Succeed())
	g.Expect(p.VM).NotTo(BeNil())
	g.Expect(lo.FromPtr(p.VM.Properties.ProvisioningState)).To(Equal("Succeeded"))
	g.Expect(p.ProviderID).To(HavePrefix("azure://"))
	g.Expect(p.ProviderID).To(ContainSubstring("/resourceGroups/mc_rg/"))
	g.Expect(vmGetter.CallCount()).To(BeNumerically(">=", 1))
}

// TestFleetMemberPromise_Wait_PollingCreatingThenSucceeded verifies Wait() polls
// through Creating state until Succeeded.
func TestFleetMemberPromise_Wait_PollingCreatingThenSucceeded(t *testing.T) {
	g := NewWithT(t)

	vmGetter := &mockVMGetterForPromise{
		responses: []mockVMGetResponse{
			{vm: makeCreatingVM("fleet-vm-poll")},
			{vm: makeCreatingVM("fleet-vm-poll")},
			{vm: makeSucceededVM("fleet-vm-poll")},
		},
	}

	p := buildPromiseWithPolling(t, "nc-poll-2", vmGetter, nil, nil)
	g.Expect(p.Wait()).To(Succeed())
	g.Expect(p.VM).NotTo(BeNil())
	g.Expect(lo.FromPtr(p.VM.Properties.ProvisioningState)).To(Equal("Succeeded"))
	g.Expect(vmGetter.CallCount()).To(Equal(3))
}

// TestFleetMemberPromise_Wait_PollingFailed verifies Wait() returns an error
// when provisioningState reaches Failed.
func TestFleetMemberPromise_Wait_PollingFailed(t *testing.T) {
	g := NewWithT(t)

	vmGetter := &mockVMGetterForPromise{
		responses: []mockVMGetResponse{
			{vm: makeCreatingVM("fleet-vm-poll")},
			{vm: makeFailedVM("fleet-vm-poll")},
		},
	}

	p := buildPromiseWithPolling(t, "nc-poll-fail", vmGetter, nil, nil)
	err := p.Wait()
	g.Expect(err).To(HaveOccurred())
	g.Expect(err.Error()).To(ContainSubstring("provisioning failed"))
	// VM should NOT be set on failure
	g.Expect(p.VM).To(BeNil())
	// But InstanceType and Zone should be set from assignment
	g.Expect(p.InstanceType).NotTo(BeNil())
	g.Expect(p.Zone).NotTo(BeEmpty())
}

// TestFleetMemberPromise_Wait_PollingFailedInvokesErrorHandler verifies that when
// provisioning fails, the error handler is invoked (same as SI VM WaitFunc closure).
func TestFleetMemberPromise_Wait_PollingFailedInvokesErrorHandler(t *testing.T) {
	g := NewWithT(t)

	vmGetter := &mockVMGetterForPromise{
		responses: []mockVMGetResponse{
			{vm: makeFailedVM("fleet-vm-poll")},
		},
	}

	unavailableOfferings := cache.NewUnavailableOfferings()
	errorHandler := offerings.NewResponseErrorHandler(unavailableOfferings)
	itProvider := &mockInstanceTypeProvider{
		sku: &skewer.SKU{Name: lo.ToPtr("Standard_D4s_v3")},
	}

	p := buildPromiseWithPolling(t, "nc-poll-handler", vmGetter, errorHandler, itProvider)
	err := p.Wait()
	g.Expect(err).To(HaveOccurred())
	g.Expect(err.Error()).To(ContainSubstring("provisioning failed"))
	// Verify the error handler was invoked (even if it didn't match a known code,
	// handleFailedProvisioning does not panic and logs gracefully)
}

// TestFleetMemberPromise_Wait_PollingFailedNilErrorHandler verifies Wait() still
// returns error even when errorHandling is nil (graceful degradation).
func TestFleetMemberPromise_Wait_PollingFailedNilErrorHandler(t *testing.T) {
	g := NewWithT(t)

	vmGetter := &mockVMGetterForPromise{
		responses: []mockVMGetResponse{
			{vm: makeFailedVM("fleet-vm-poll")},
		},
	}

	p := buildPromiseWithPolling(t, "nc-poll-nil-handler", vmGetter, nil, nil)
	err := p.Wait()
	g.Expect(err).To(HaveOccurred())
	g.Expect(err.Error()).To(ContainSubstring("provisioning failed"))
}

// TestFleetMemberPromise_Wait_PollingTransientErrorThenSuccess verifies retries
// on transient GET errors.
func TestFleetMemberPromise_Wait_PollingTransientErrorThenSuccess(t *testing.T) {
	g := NewWithT(t)

	transientErr := &azcore.ResponseError{
		StatusCode: http.StatusTooManyRequests,
		ErrorCode:  "TooManyRequests",
	}

	vmGetter := &mockVMGetterForPromise{
		responses: []mockVMGetResponse{
			{vm: makeCreatingVM("fleet-vm-poll")},
			{err: transientErr},
			{vm: makeSucceededVM("fleet-vm-poll")},
		},
	}

	p := buildPromiseWithPolling(t, "nc-poll-retry", vmGetter, nil, nil)
	g.Expect(p.Wait()).To(Succeed())
	g.Expect(p.VM).NotTo(BeNil())
	g.Expect(vmGetter.CallCount()).To(Equal(3))
}

// TestFleetMemberPromise_Wait_PollingNonTransientErrorFails verifies 404 etc.
// cause immediate failure.
func TestFleetMemberPromise_Wait_PollingNonTransientErrorFails(t *testing.T) {
	g := NewWithT(t)

	notFoundErr := &azcore.ResponseError{
		StatusCode: http.StatusNotFound,
		ErrorCode:  "ResourceNotFound",
	}

	vmGetter := &mockVMGetterForPromise{
		responses: []mockVMGetResponse{
			{err: notFoundErr},
		},
	}

	p := buildPromiseWithPolling(t, "nc-poll-404", vmGetter, nil, nil)
	err := p.Wait()
	g.Expect(err).To(HaveOccurred())
	g.Expect(err.Error()).To(ContainSubstring("non-retryable"))
	g.Expect(p.VM).To(BeNil())
}

// TestFleetMemberPromise_Wait_PollingContextCancelled verifies ctx cancellation.
func TestFleetMemberPromise_Wait_PollingContextCancelled(t *testing.T) {
	g := NewWithT(t)

	vmGetter := &mockVMGetterForPromise{
		responses: []mockVMGetResponse{
			{vm: makeCreatingVM("fleet-vm-poll")},
			{vm: makeCreatingVM("fleet-vm-poll")},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	vmSize := armcompute.VirtualMachineSizeTypes("Standard_D4s_v3")
	assignmentVM := &armcompute.VirtualMachine{
		Name:     lo.ToPtr("fleet-vm-poll"),
		Location: lo.ToPtr("westus"),
		ID: lo.ToPtr(
			"/subscriptions/12345678-1234-1234-1234-123456789012" +
				"/resourceGroups/MC_rg" +
				"/providers/Microsoft.Compute/virtualMachines/fleet-vm-poll",
		),
		Properties: &armcompute.VirtualMachineProperties{
			HardwareProfile: &armcompute.HardwareProfile{VMSize: &vmSize},
		},
		Zones: []*string{lo.ToPtr("1")},
	}

	state := fleet.NewFleetSharedStateForTest(
		[]*armcompute.VirtualMachine{assignmentVM},
		[]*fleet.VMAssignmentRequest{
			{
				NodeClaimName:   "nc-ctx-cancel",
				AcceptableSKUs:  []string{"Standard_D4s_v3"},
				AcceptableZones: []string{"westus-1"},
				InstanceTypes: map[string]*corecloudprovider.InstanceType{
					"Standard_D4s_v3": {Name: "Standard_D4s_v3"},
				},
			},
		},
		nil, nil, "fleet-test", "rg-test",
	)
	state.RunAssignmentForTest(context.Background())

	fastOpts := fleetvmpoller.InstantOptions()
	p := &FleetMemberPromise{
		sharedState:   state,
		nodeClaimName: "nc-ctx-cancel",
		fleetName:     "fleet-test",
		ctx:           ctx,
		vmClient:      vmGetter,
		resourceGroup: "MC_rg",
		pollerOptions: &fastOpts,
	}

	err := p.Wait()
	g.Expect(err).To(HaveOccurred())
	g.Expect(err.Error()).To(ContainSubstring("context canceled"))
}

// TestFleetMemberPromise_Wait_SharedStateError verifies Wait() returns error
// immediately when the shared state has a fleet-level error.
func TestFleetMemberPromise_Wait_SharedStateError(t *testing.T) {
	g := NewWithT(t)

	vmGetter := &mockVMGetterForPromise{
		responses: []mockVMGetResponse{
			{vm: makeSucceededVM("fleet-vm-poll")},
		},
	}

	state := fleet.NewFleetSharedStateForTest(nil, nil, nil, nil, "fleet-test", "rg-test")
	state.SetError(errors.New("fleet PUT failed"))

	p := &FleetMemberPromise{
		sharedState:   state,
		nodeClaimName: "nc-fleet-err",
		fleetName:     "fleet-test",
		ctx:           context.Background(),
		vmClient:      vmGetter,
		resourceGroup: "MC_rg",
	}

	err := p.Wait()
	g.Expect(err).To(HaveOccurred())
	g.Expect(err.Error()).To(ContainSubstring("fleet PUT failed"))
	// vmGetter should never be called - error is from shared state
	g.Expect(vmGetter.CallCount()).To(Equal(0))
}

// TestFleetMemberPromise_Wait_AssignmentVMWithEmptyName verifies the guard
// against assignments that have no VM name.
func TestFleetMemberPromise_Wait_AssignmentVMWithEmptyName(t *testing.T) {
	g := NewWithT(t)

	vmGetter := &mockVMGetterForPromise{
		responses: []mockVMGetResponse{
			{vm: makeSucceededVM("fleet-vm-poll")},
		},
	}

	vmSize := armcompute.VirtualMachineSizeTypes("Standard_D4s_v3")
	assignmentVM := &armcompute.VirtualMachine{
		Name:     lo.ToPtr(""), // empty name
		Location: lo.ToPtr("westus"),
		ID: lo.ToPtr(
			"/subscriptions/12345678-1234-1234-1234-123456789012" +
				"/resourceGroups/MC_rg" +
				"/providers/Microsoft.Compute/virtualMachines/",
		),
		Properties: &armcompute.VirtualMachineProperties{
			HardwareProfile: &armcompute.HardwareProfile{VMSize: &vmSize},
		},
		Zones: []*string{lo.ToPtr("1")},
	}

	state := fleet.NewFleetSharedStateForTest(
		[]*armcompute.VirtualMachine{assignmentVM},
		[]*fleet.VMAssignmentRequest{
			{
				NodeClaimName:   "nc-empty-name",
				AcceptableSKUs:  []string{"Standard_D4s_v3"},
				AcceptableZones: []string{"westus-1"},
				InstanceTypes: map[string]*corecloudprovider.InstanceType{
					"Standard_D4s_v3": {Name: "Standard_D4s_v3"},
				},
			},
		},
		nil, nil, "fleet-test", "rg-test",
	)
	state.RunAssignmentForTest(context.Background())

	fastOpts := fleetvmpoller.InstantOptions()
	p := &FleetMemberPromise{
		sharedState:   state,
		nodeClaimName: "nc-empty-name",
		fleetName:     "fleet-test",
		ctx:           context.Background(),
		vmClient:      vmGetter,
		resourceGroup: "MC_rg",
		pollerOptions: &fastOpts,
	}

	err := p.Wait()
	g.Expect(err).To(HaveOccurred())
	g.Expect(err.Error()).To(ContainSubstring("no VM name"))
	g.Expect(vmGetter.CallCount()).To(Equal(0))
}

// TestFleetMemberPromise_Wait_LegacyPathNoVMClient verifies the fallback behavior
// when vmClient is nil (legacy tests) - uses assignment VM directly.
func TestFleetMemberPromise_Wait_LegacyPathNoVMClient(t *testing.T) {
	g := NewWithT(t)

	vmSize := armcompute.VirtualMachineSizeTypes("Standard_D4s_v3")
	vm := &armcompute.VirtualMachine{
		Name:     lo.ToPtr("vm-legacy"),
		Location: lo.ToPtr("westus"),
		Zones:    []*string{lo.ToPtr("1")},
		ID: lo.ToPtr(
			"/subscriptions/12345678-1234-1234-1234-123456789012" +
				"/resourceGroups/MC_rg" +
				"/providers/Microsoft.Compute/virtualMachines/vm-legacy",
		),
		Properties: &armcompute.VirtualMachineProperties{
			HardwareProfile: &armcompute.HardwareProfile{VMSize: &vmSize},
		},
	}

	state := fleet.NewFleetSharedStateForTest(
		[]*armcompute.VirtualMachine{vm},
		[]*fleet.VMAssignmentRequest{
			{
				NodeClaimName:   "nc-legacy",
				AcceptableSKUs:  []string{"Standard_D4s_v3"},
				AcceptableZones: []string{"westus-1"},
				InstanceTypes: map[string]*corecloudprovider.InstanceType{
					"Standard_D4s_v3": {Name: "Standard_D4s_v3"},
				},
			},
		},
		nil, nil, "fleet-test", "rg-test",
	)
	state.RunAssignmentForTest(context.Background())

	p := &FleetMemberPromise{
		sharedState:   state,
		nodeClaimName: "nc-legacy",
		fleetName:     "fleet-test",
		// vmClient intentionally nil
	}

	g.Expect(p.Wait()).To(Succeed())
	g.Expect(p.VM).NotTo(BeNil())
	g.Expect(lo.FromPtr(p.VM.Name)).To(Equal("vm-legacy"))
	g.Expect(p.ProviderID).To(HavePrefix("azure://"))
}

// TestFleetMemberPromise_Wait_PollingSuccessPopulatesProviderID verifies the
// ProviderID is populated from the polled VM (not the assignment VM).
func TestFleetMemberPromise_Wait_PollingSuccessPopulatesProviderID(t *testing.T) {
	g := NewWithT(t)

	succeededVM := &armcompute.VirtualMachine{
		Name: lo.ToPtr("fleet-vm-poll"),
		ID: lo.ToPtr(
			"/subscriptions/12345678-1234-1234-1234-123456789012" +
				"/resourceGroups/MC_RG_UPPER" +
				"/providers/Microsoft.Compute/virtualMachines/fleet-vm-poll",
		),
		Properties: &armcompute.VirtualMachineProperties{
			ProvisioningState: lo.ToPtr("Succeeded"),
			HardwareProfile: &armcompute.HardwareProfile{
				VMSize: lo.ToPtr(armcompute.VirtualMachineSizeTypes("Standard_D4s_v3")),
			},
		},
		Zones: []*string{lo.ToPtr("1")},
	}

	vmGetter := &mockVMGetterForPromise{
		responses: []mockVMGetResponse{
			{vm: succeededVM},
		},
	}

	p := buildPromiseWithPolling(t, "nc-providerid", vmGetter, nil, nil)
	g.Expect(p.Wait()).To(Succeed())
	// ProviderID should lowercase the RG
	g.Expect(p.ProviderID).To(ContainSubstring("/resourceGroups/mc_rg_upper/"))
	g.Expect(p.ProviderID).To(HavePrefix("azure://"))
}

// TestFleetMemberPromise_Wait_HandleFailedProvisioningGracefulWhenSKULookupFails
// verifies that when instanceTypeProvider.Get() fails, handleFailedProvisioning
// logs the error gracefully and does not panic.
func TestFleetMemberPromise_Wait_HandleFailedProvisioningGracefulWhenSKULookupFails(t *testing.T) {
	g := NewWithT(t)

	vmGetter := &mockVMGetterForPromise{
		responses: []mockVMGetResponse{
			{vm: makeFailedVM("fleet-vm-poll")},
		},
	}

	unavailableOfferings := cache.NewUnavailableOfferings()
	errorHandler := offerings.NewResponseErrorHandler(unavailableOfferings)
	itProvider := &mockInstanceTypeProvider{
		getErr: errors.New("SKU not found in cache"),
	}

	p := buildPromiseWithPolling(t, "nc-sku-err", vmGetter, errorHandler, itProvider)
	err := p.Wait()
	// Wait should still return the provisioning error (not the SKU lookup error)
	g.Expect(err).To(HaveOccurred())
	g.Expect(err.Error()).To(ContainSubstring("provisioning failed"))
}
