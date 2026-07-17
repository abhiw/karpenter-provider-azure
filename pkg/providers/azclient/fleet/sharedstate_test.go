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
	"testing"

	. "github.com/onsi/gomega"
	"github.com/samber/lo"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
)

// --- Tests ---

// TestFleetSharedState_SingleRunAssignment verifies that runAssignment produces the
// expected assignment on a single call. With sync.Once removed, the executor is the
// single caller — concurrent calls are no longer part of the contract.
func TestFleetSharedState_SingleRunAssignment(t *testing.T) {
	g := NewWithT(t)

	vmSize := armcompute.VirtualMachineSizeTypes("Standard_D4s_v3")
	// Realistic ARM payload: numeric zone + region location. The matcher must
	// convert these to AKS-label format ("westus-1") for the assignment record.
	vm := &armcompute.VirtualMachine{
		Name:     lo.ToPtr("vm-1"),
		Location: lo.ToPtr("westus"),
		Zones:    []*string{lo.ToPtr("1")},
		Properties: &armcompute.VirtualMachineProperties{
			HardwareProfile: &armcompute.HardwareProfile{VMSize: &vmSize},
		},
	}

	state := NewFleetSharedStateForTest(
		[]*armcompute.VirtualMachine{vm},
		[]*VMAssignmentRequest{
			{NodeClaimName: "nc-1", AcceptableSKUs: []string{"Standard_D4s_v3"}, AcceptableZones: []string{"westus-1"},
				InstanceTypes: map[string]*cloudprovider.InstanceType{"Standard_D4s_v3": {Name: "Standard_D4s_v3"}}},
		},
		nil, nil, "fleet-test", "rg-test",
	)

	state.runAssignment(context.Background())

	g.Expect(state.GetAssignment("nc-1")).NotTo(BeNil())
}

// TestFleetSharedState_AllRequestsAssigned verifies that when VMs match all requests,
// every NodeClaim gets a non-nil assignment.
func TestFleetSharedState_AllRequestsAssigned(t *testing.T) {
	g := NewWithT(t)

	state := NewFleetSharedStateForTest(
		[]*armcompute.VirtualMachine{
			mkVM("Standard_D4s_v3", "westus-1"),
			mkVM("Standard_D8s_v3", "westus-2"),
		},
		[]*VMAssignmentRequest{
			{NodeClaimName: "nc-1", AcceptableSKUs: []string{"Standard_D4s_v3"}, AcceptableZones: []string{"westus-1"},
				InstanceTypes: map[string]*cloudprovider.InstanceType{"Standard_D4s_v3": {Name: "Standard_D4s_v3"}}},
			{NodeClaimName: "nc-2", AcceptableSKUs: []string{"Standard_D8s_v3"}, AcceptableZones: []string{"westus-2"},
				InstanceTypes: map[string]*cloudprovider.InstanceType{"Standard_D8s_v3": {Name: "Standard_D8s_v3"}}},
		},
		nil, nil, "fleet-test", "rg-test",
	)

	state.runAssignment(context.Background())

	g.Expect(state.GetError()).To(BeNil())
	g.Expect(state.GetAssignment("nc-1")).NotTo(BeNil())
	g.Expect(state.GetAssignment("nc-2")).NotTo(BeNil())
	g.Expect(state.GetAssignment("nc-1").Zone).To(Equal("westus-1"))
}

// TestFleetSharedState_PartialAssignment verifies that when fewer VMs than requests,
// unmatched NodeClaims get nil from GetAssignment.
func TestFleetSharedState_PartialAssignment(t *testing.T) {
	g := NewWithT(t)

	state := NewFleetSharedStateForTest(
		[]*armcompute.VirtualMachine{
			mkVM("Standard_D4s_v3", "westus-1"),
		},
		[]*VMAssignmentRequest{
			{NodeClaimName: "nc-1", AcceptableSKUs: []string{"Standard_D4s_v3"}, AcceptableZones: []string{"westus-1"},
				InstanceTypes: map[string]*cloudprovider.InstanceType{"Standard_D4s_v3": {Name: "Standard_D4s_v3"}}},
			{NodeClaimName: "nc-2", AcceptableSKUs: []string{"Standard_D4s_v3"}, AcceptableZones: []string{"westus-1"},
				InstanceTypes: map[string]*cloudprovider.InstanceType{"Standard_D4s_v3": {Name: "Standard_D4s_v3"}}},
		},
		nil, nil, "fleet-test", "rg-test",
	)

	state.runAssignment(context.Background())

	g.Expect(state.GetError()).To(BeNil())
	g.Expect(state.GetAssignment("nc-1")).NotTo(BeNil())
	g.Expect(state.GetAssignment("nc-2")).To(BeNil()) // unmatched
}

// TestFleetSharedState_CreateError verifies that when SetError is called before
// runAssignment, GetError returns the error for all promises.
func TestFleetSharedState_CreateError(t *testing.T) {
	g := NewWithT(t)

	state := NewFleetSharedStateForTest(
		nil,
		[]*VMAssignmentRequest{
			{NodeClaimName: "nc-1", AcceptableSKUs: []string{"Standard_D4s_v3"}, AcceptableZones: []string{"westus-1"},
				InstanceTypes: map[string]*cloudprovider.InstanceType{"Standard_D4s_v3": {Name: "Standard_D4s_v3"}}},
		},
		nil, nil, "fleet-test", "rg-test",
	)
	state.SetError(fmt.Errorf("fleet create failed: timeout"))

	state.runAssignment(context.Background())

	g.Expect(state.GetError()).To(MatchError(ContainSubstring("fleet create failed")))
	g.Expect(state.GetAssignment("nc-1")).To(BeNil())
}

// TestFleetSharedState_EmptyBatch verifies zero requests + zero VMs doesn't panic.
func TestFleetSharedState_EmptyBatch(t *testing.T) {
	g := NewWithT(t)

	state := NewFleetSharedStateForTest(
		nil, nil, nil, nil, "fleet-test", "rg-test",
	)

	state.runAssignment(context.Background())

	g.Expect(state.GetError()).To(BeNil())
}

// TestFleetSharedState_GetAssignment_Unknown verifies asking for a non-existent
// NodeClaim returns nil.
func TestFleetSharedState_GetAssignment_Unknown(t *testing.T) {
	g := NewWithT(t)

	state := NewFleetSharedStateForTest(
		[]*armcompute.VirtualMachine{mkVM("Standard_D4s_v3", "westus-1")},
		[]*VMAssignmentRequest{
			{NodeClaimName: "nc-1", AcceptableSKUs: []string{"Standard_D4s_v3"}, AcceptableZones: []string{"westus-1"},
				InstanceTypes: map[string]*cloudprovider.InstanceType{"Standard_D4s_v3": {Name: "Standard_D4s_v3"}}},
		},
		nil, nil, "fleet-test", "rg-test",
	)

	state.runAssignment(context.Background())
	g.Expect(state.GetAssignment("nc-unknown")).To(BeNil())
}
