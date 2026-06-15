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

package cloudprovider

import (
	"context"
	stderrors "errors"
	"fmt"
	"testing"

	//nolint:SA1019 // deprecated package — the provider still uses v7
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork"
	"github.com/samber/lo"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	corecloudprovider "sigs.k8s.io/karpenter/pkg/cloudprovider"

	"github.com/Azure/karpenter-provider-azure/pkg/apis/v1beta1"
	"github.com/Azure/karpenter-provider-azure/pkg/consts"
	"github.com/Azure/karpenter-provider-azure/pkg/operator/options"
	"github.com/Azure/karpenter-provider-azure/pkg/providers/instance"
)

// stubVMProvider satisfies instance.VMProvider but only meaningfully
// implements the two methods CloudProvider.Delete exercises. Other methods
// panic to make accidental usage loud.
type stubVMProvider struct {
	deleteCalls []string
	deleteErr   error

	findResult *armcompute.VirtualMachine
	findErr    error
	findCalls  int
}

var _ instance.VMProvider = (*stubVMProvider)(nil)

func (s *stubVMProvider) Delete(_ context.Context, vmName string) error {
	s.deleteCalls = append(s.deleteCalls, vmName)
	return s.deleteErr
}

func (s *stubVMProvider) FindVMByNodeClaimTag(_ context.Context, _ string) (*armcompute.VirtualMachine, error) {
	s.findCalls++
	return s.findResult, s.findErr
}

// Unused-in-test methods — present only to satisfy the interface.
func (s *stubVMProvider) BeginCreate(context.Context, *v1beta1.AKSNodeClass, *karpv1.NodeClaim, []*corecloudprovider.InstanceType) (*instance.VirtualMachinePromise, error) {
	panic("not used")
}
func (s *stubVMProvider) Get(context.Context, string) (*armcompute.VirtualMachine, error) {
	panic("not used")
}
func (s *stubVMProvider) List(context.Context) ([]*armcompute.VirtualMachine, error) {
	panic("not used")
}
func (s *stubVMProvider) Update(context.Context, string, armcompute.VirtualMachineUpdate) error {
	panic("not used")
}
func (s *stubVMProvider) GetNic(context.Context, string, string) (*armnetwork.Interface, error) {
	panic("not used")
}
func (s *stubVMProvider) DeleteNic(context.Context, string) error { panic("not used") }
func (s *stubVMProvider) ListNics(context.Context) ([]*armnetwork.Interface, error) {
	panic("not used")
}

func fleetCtx(t *testing.T) context.Context {
	t.Helper()
	return options.ToContext(context.Background(), &options.Options{ProvisionMode: consts.ProvisionModeFleet})
}

func nonFleetCtx(t *testing.T) context.Context {
	t.Helper()
	return options.ToContext(context.Background(), &options.Options{ProvisionMode: ""})
}

func ncWithProviderID(name, providerID string) *karpv1.NodeClaim {
	nc := &karpv1.NodeClaim{}
	nc.Name = name
	nc.Status.ProviderID = providerID
	return nc
}

// 1. Happy path: providerID is populated → use it; tag lookup MUST NOT happen.
func TestDelete_VM_UsesProviderID(t *testing.T) {
	stub := &stubVMProvider{}
	cp := &CloudProvider{vmInstanceProvider: stub}

	nc := ncWithProviderID("nc-1", "azure:///subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/aks-default-x9k2d")
	if err := cp.Delete(fleetCtx(t), nc); err != nil {
		t.Fatalf("Delete error: %v", err)
	}
	if want, got := []string{"aks-default-x9k2d"}, stub.deleteCalls; !equalSlices(got, want) {
		t.Fatalf("Delete calls = %v, want %v", got, want)
	}
	if stub.findCalls != 0 {
		t.Fatalf("FindVMByNodeClaimTag should NOT be called when providerID is valid; got %d calls", stub.findCalls)
	}
}

// 2. Fleet-mode fallback: empty providerID + tagged VM exists → delete by found name.
func TestDelete_VM_FleetFallback_FindsTaggedVM(t *testing.T) {
	stub := &stubVMProvider{
		findResult: &armcompute.VirtualMachine{Name: lo.ToPtr("aks-default-recovered")},
	}
	cp := &CloudProvider{vmInstanceProvider: stub}

	if err := cp.Delete(fleetCtx(t), ncWithProviderID("nc-orphan", "")); err != nil {
		t.Fatalf("Delete error: %v", err)
	}
	if stub.findCalls != 1 {
		t.Fatalf("FindVMByNodeClaimTag calls = %d, want 1", stub.findCalls)
	}
	if want, got := []string{"aks-default-recovered"}, stub.deleteCalls; !equalSlices(got, want) {
		t.Fatalf("Delete calls = %v, want %v", got, want)
	}
}

// 3. Fleet-mode fallback: no VM tagged → returns NewNodeClaimNotFoundError (NOT a generic error).
func TestDelete_VM_FleetFallback_NotFound(t *testing.T) {
	stub := &stubVMProvider{
		findErr: corecloudprovider.NewNodeClaimNotFoundError(stderrors.New("no VM tagged with nodeclaim-name")),
	}
	cp := &CloudProvider{vmInstanceProvider: stub}

	err := cp.Delete(fleetCtx(t), ncWithProviderID("nc-orphan", ""))
	if err == nil {
		t.Fatal("expected NodeClaimNotFoundError, got nil")
	}
	if !corecloudprovider.IsNodeClaimNotFoundError(err) {
		t.Fatalf("expected NodeClaimNotFoundError, got %v (type %T)", err, err)
	}
	if len(stub.deleteCalls) != 0 {
		t.Fatalf("no Delete should have been called on miss; got %v", stub.deleteCalls)
	}
}

// 4. Fleet-mode fallback: lookup propagates a non-not-found error (e.g., ARM throttle).
func TestDelete_VM_FleetFallback_LookupError(t *testing.T) {
	stub := &stubVMProvider{findErr: fmt.Errorf("ARM 429 throttled")}
	cp := &CloudProvider{vmInstanceProvider: stub}

	err := cp.Delete(fleetCtx(t), ncWithProviderID("nc-orphan", ""))
	if err == nil {
		t.Fatal("expected error from lookup failure")
	}
	if corecloudprovider.IsNodeClaimNotFoundError(err) {
		t.Fatalf("transient lookup errors must not be classified as NotFound: %v", err)
	}
}

// 5. Non-Fleet mode: empty providerID returns the original "getting VM name" error — no fallback.
func TestDelete_VM_NonFleetMode_NoFallback(t *testing.T) {
	stub := &stubVMProvider{}
	cp := &CloudProvider{vmInstanceProvider: stub}

	err := cp.Delete(nonFleetCtx(t), ncWithProviderID("nc-orphan", ""))
	if err == nil {
		t.Fatal("expected error in non-Fleet mode with empty providerID")
	}
	if stub.findCalls != 0 {
		t.Fatalf("FindVMByNodeClaimTag must NOT be called in non-Fleet mode; got %d calls", stub.findCalls)
	}
	if len(stub.deleteCalls) != 0 {
		t.Fatalf("Delete must NOT be called in non-Fleet mode; got %v", stub.deleteCalls)
	}
}

func equalSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
