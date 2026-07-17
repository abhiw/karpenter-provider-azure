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
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"

	"github.com/Azure/karpenter-provider-azure/pkg/utils/batcher"
	"github.com/Azure/karpenter-provider-azure/pkg/utils/zones"
)

// FleetAssignment represents a VM assigned to a specific NodeClaim.
type FleetAssignment struct {
	VM           *armcompute.VirtualMachine
	InstanceType *cloudprovider.InstanceType
	Zone         string
}

// VMAssignmentRequest is the per-NodeClaim entry used during the assignment phase.
// It describes what SKUs/zones a NodeClaim can accept and where to send the result.
type VMAssignmentRequest struct {
	NodeClaimName   string
	AcceptableSKUs  []string
	AcceptableZones []string
	InstanceTypes   map[string]*cloudprovider.InstanceType
	ResponseChan    chan *batcher.Response[FleetBatchResponse]
}

// AssignVMsToNodeClaims matches Fleet-created VMs to NodeClaim requests in pure
// FIFO order: the Nth request (in slice order) is assigned the Nth well-formed VM
// (in slice order), regardless of SKU or zone. Fleet already selected the SKU/zone
// per its allocation strategy, so the provider does not re-match on (SKU, zone) —
// the assignment simply records whatever SKU/zone the VM actually has.
//
// The function is pure: it does not modify the input slices and has no external side effects.
//
// Returns:
//   - assigned: nodeClaimName → FleetAssignment for every request that received a VM
//   - unmatched: requests left over when there are fewer VMs than requests
//   - surplus: VMs left over when there are more VMs than requests, plus any
//     malformed VMs (missing SKU) that can never be assigned
//
// instanceTypes is the merged SKU → InstanceType map; it (falling back to the
// request's own InstanceTypes) is used to populate FleetAssignment.InstanceType.
func AssignVMsToNodeClaims(
	requests []*VMAssignmentRequest,
	vms []*armcompute.VirtualMachine,
	instanceTypes map[string]*cloudprovider.InstanceType,
) (assigned map[string]*FleetAssignment, unmatched []*VMAssignmentRequest, surplus []*armcompute.VirtualMachine) {
	if len(requests) == 0 && len(vms) == 0 {
		return nil, nil, nil
	}

	// 1. Split VMs into assignable (well-formed) and malformed. Malformed VMs can
	//    never map to a NodeClaim, so they go straight to surplus.
	assignable := make([]*armcompute.VirtualMachine, 0, len(vms))
	for _, vm := range vms {
		if _, _, ok := skuAndZone(vm); !ok {
			surplus = append(surplus, vm)
			continue
		}
		assignable = append(assignable, vm)
	}

	// 2. Pair requests with assignable VMs in FIFO order.
	assigned = make(map[string]*FleetAssignment, len(requests))
	next := 0
	for _, req := range requests {
		if req == nil {
			continue
		}
		if next >= len(assignable) {
			unmatched = append(unmatched, req)
			continue
		}
		vm := assignable[next]
		next++
		sku, zone, _ := skuAndZone(vm)
		assigned[req.NodeClaimName] = &FleetAssignment{
			VM:           vm,
			InstanceType: instanceTypeForSKU(sku, req, instanceTypes),
			Zone:         zone,
		}
	}

	// 3. Any assignable VMs beyond the number of requests are surplus.
	if next < len(assignable) {
		surplus = append(surplus, assignable[next:]...)
	}
	return assigned, unmatched, surplus
}

// instanceTypeForSKU resolves the InstanceType for a VM's SKU, preferring the
// request's own InstanceTypes map and falling back to the merged map. Returns
// nil if the SKU is not present in either (the assignment is still valid).
func instanceTypeForSKU(sku string, req *VMAssignmentRequest, merged map[string]*cloudprovider.InstanceType) *cloudprovider.InstanceType {
	if req != nil && req.InstanceTypes != nil {
		if it, ok := req.InstanceTypes[sku]; ok {
			return it
		}
	}
	if merged != nil {
		return merged[sku]
	}
	return nil
}

// skuAndZone extracts the SKU string and AKS-label zone from a Fleet-created VM.
// The returned zone is in AKS-label format (e.g. "southcentralus-3" or "0" for
// regional) so it can be compared against VMAssignmentRequest.AcceptableZones,
// which the scheduler populates from corev1.LabelTopologyZone (also AKS-label format).
//
// IMPORTANT: ARM returns vm.Zones as numeric strings ("1", "2", "3") and a separate
// vm.Location. Using vm.Zones[0] directly would never match the request's AKS-label
// zones — every VM would be routed to surplus and deleted.
//
// Returns ok=false if any required field is missing or zone conversion fails.
func skuAndZone(vm *armcompute.VirtualMachine) (string, string, bool) {
	if vm == nil || vm.Properties == nil ||
		vm.Properties.HardwareProfile == nil ||
		vm.Properties.HardwareProfile.VMSize == nil {
		return "", "", false
	}
	sku := string(*vm.Properties.HardwareProfile.VMSize)
	zone, err := zones.MakeAKSLabelZoneFromVM(vm)
	if err != nil {
		// Malformed (e.g. zonal VM with no Location, or multiple zones) — treat as surplus.
		return "", "", false
	}
	return sku, zone, true
}
