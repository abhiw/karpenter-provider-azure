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
	"strings"
	"sync"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	corecloudprovider "sigs.k8s.io/karpenter/pkg/cloudprovider"

	"github.com/Azure/karpenter-provider-azure/pkg/apis/v1beta1"
	"github.com/Azure/karpenter-provider-azure/pkg/providers/azclient/fleet"
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
