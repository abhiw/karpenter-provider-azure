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

package fleettag

import (
	"context"
	"testing"

	. "github.com/onsi/gomega"
	"github.com/samber/lo"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"

	"github.com/Azure/karpenter-provider-azure/pkg/providers/azclient/fleet"
	"github.com/Azure/karpenter-provider-azure/pkg/providers/launchtemplate"
	"github.com/Azure/karpenter-provider-azure/pkg/utils"
)

const (
	testClusterName = "test-cluster"
	testVMID        = "/subscriptions/1234/resourceGroups/mc-rg/providers/Microsoft.Compute/virtualMachines/vm-1"
)

func mkFleetVM(name, id string, tags map[string]string) *armcompute.VirtualMachine {
	tagPtrs := map[string]*string{}
	for k, v := range tags {
		tagPtrs[k] = lo.ToPtr(v)
	}
	return &armcompute.VirtualMachine{
		Name: lo.ToPtr(name),
		ID:   lo.ToPtr(id),
		Tags: tagPtrs,
	}
}

// TestBuildTagWork_TagsAssignedVM verifies that a Fleet VM with a matching NodeClaim
// ProviderID and no existing nodeclaim-name tag is selected for tagging.
func TestBuildTagWork_TagsAssignedVM(t *testing.T) {
	g := NewWithT(t)
	c := &Controller{clusterName: testClusterName}

	vm := mkFleetVM("vm-1", testVMID, map[string]string{
		fleet.FleetNameTagKey:                 "fleet-abc",
		launchtemplate.KarpenterManagedTagKey: testClusterName,
	})
	providerID := utils.VMResourceIDToProviderID(context.Background(), testVMID)

	work := c.buildTagWork(context.Background(), []*armcompute.VirtualMachine{vm}, map[string]string{providerID: "nc-1"})

	g.Expect(work).To(HaveLen(1))
	g.Expect(work[0].vmName).To(Equal("vm-1"))
	g.Expect(work[0].ncName).To(Equal("nc-1"))
	g.Expect(work[0].tags).To(HaveKeyWithValue(NodeClaimNameTagKey, lo.ToPtr("nc-1")))
	// Existing tags must be preserved in the merged set.
	g.Expect(work[0].tags).To(HaveKey(fleet.FleetNameTagKey))
}

// TestBuildTagWork_SkipsAlreadyTagged verifies that a VM which already carries the
// nodeclaim-name tag is not re-tagged.
func TestBuildTagWork_SkipsAlreadyTagged(t *testing.T) {
	g := NewWithT(t)
	c := &Controller{clusterName: testClusterName}

	vm := mkFleetVM("vm-1", testVMID, map[string]string{
		fleet.FleetNameTagKey:                 "fleet-abc",
		launchtemplate.KarpenterManagedTagKey: testClusterName,
		NodeClaimNameTagKey:                   "nc-1",
	})
	providerID := utils.VMResourceIDToProviderID(context.Background(), testVMID)

	work := c.buildTagWork(context.Background(), []*armcompute.VirtualMachine{vm}, map[string]string{providerID: "nc-1"})
	g.Expect(work).To(BeEmpty())
}

// TestBuildTagWork_SkipsUnassigned verifies that a Fleet VM without a matching
// NodeClaim ProviderID (surplus/unassigned) is left untagged for instance GC.
func TestBuildTagWork_SkipsUnassigned(t *testing.T) {
	g := NewWithT(t)
	c := &Controller{clusterName: testClusterName}

	vm := mkFleetVM("vm-1", testVMID, map[string]string{
		fleet.FleetNameTagKey:                 "fleet-abc",
		launchtemplate.KarpenterManagedTagKey: testClusterName,
	})

	// Empty ProviderID map: no NodeClaim owns this VM.
	work := c.buildTagWork(context.Background(), []*armcompute.VirtualMachine{vm}, map[string]string{})
	g.Expect(work).To(BeEmpty())
}

// TestBuildTagWork_SkipsNonFleetVM verifies that a VM without the fleet-name tag is ignored.
func TestBuildTagWork_SkipsNonFleetVM(t *testing.T) {
	g := NewWithT(t)
	c := &Controller{clusterName: testClusterName}

	vm := mkFleetVM("vm-1", testVMID, map[string]string{
		launchtemplate.KarpenterManagedTagKey: testClusterName,
	})
	providerID := utils.VMResourceIDToProviderID(context.Background(), testVMID)

	work := c.buildTagWork(context.Background(), []*armcompute.VirtualMachine{vm}, map[string]string{providerID: "nc-1"})
	g.Expect(work).To(BeEmpty())
}

// TestBuildTagWork_SkipsOtherCluster verifies the defense-in-depth guard: a Fleet VM
// managed by a different cluster is never tagged.
func TestBuildTagWork_SkipsOtherCluster(t *testing.T) {
	g := NewWithT(t)
	c := &Controller{clusterName: testClusterName}

	vm := mkFleetVM("vm-1", testVMID, map[string]string{
		fleet.FleetNameTagKey:                 "fleet-abc",
		launchtemplate.KarpenterManagedTagKey: "some-other-cluster",
	})
	providerID := utils.VMResourceIDToProviderID(context.Background(), testVMID)

	work := c.buildTagWork(context.Background(), []*armcompute.VirtualMachine{vm}, map[string]string{providerID: "nc-1"})
	g.Expect(work).To(BeEmpty())
}
