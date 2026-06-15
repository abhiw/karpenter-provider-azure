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
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Azure/azure-kusto-go/kusto/kql"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork"
	"github.com/Azure/karpenter-provider-azure/pkg/providers/launchtemplate"
	"github.com/samber/lo"
)

const (
	vmResourceType  = "microsoft.compute/virtualmachines"
	nicResourceType = "microsoft.network/networkinterfaces"
)

// getResourceListQueryBuilder returns a KQL query builder for listing resources with nodepool tags
// but excluding AKS machine-created resources.
//
// TODO(fleet-arg-exclusion): when running in Fleet mode, newly-created Fleet
// VMs (and NICs, which inherit the same Fleet tags) pass through a brief
// window between the Fleet LRO completing and per-VM nodeclaim-name tagging
// finishing (see pkg/providers/azclient/fleet/sharedstate.go::tagAssignedVMs).
// During that window the existing nodeclaim instance GC
// (pkg/controllers/nodeclaim/garbagecollection/instance_garbagecollection.go)
// can see those resources as orphans because no NodeClaim yet references
// them, and delete them after its 1-minute cascadeDeletion timer.
//
// The fix is to exclude in-flight Fleet resources from this list by adding:
//
//	| where not(tags has_cs "karpenter.azure.com_batch-key-hash"
//	     and not(tags has_cs "karpenter.azure.com_nodeclaim-name"))
//
// (equivalently, using fleet.BatchKeyHashTagKey and fleet.NodeClaimNameTagKey
// constants and kql.Builder.AddString). It applies to both the VM and NIC
// builders below since both call this function and Fleet VMs/NICs share tags.
//
// The batch-key-hash tag is already propagated to Fleet VMs/NICs as of
// pkg/providers/azclient/fleet/executor.go::executeBatch, so adding the
// exclusion clause is a single KQL edit with no other coordination needed.
// Tracked separately from this PR; see the design doc for the follow-up.
func getResourceListQueryBuilder(rg string, resourceType string) *kql.Builder {
	return kql.New(`Resources`).
		AddLiteral(` | where type == `).AddString(resourceType).
		AddLiteral(` | where resourceGroup == `).AddString(strings.ToLower(rg)). // ARG resources appear to have lowercase RG
		AddLiteral(` | where tags has_cs `).AddString(launchtemplate.NodePoolTagKey).
		AddLiteral(` | where not(tags has_cs `).AddString(launchtemplate.KarpenterAKSMachineNodeClaimTagKey).AddLiteral(`)`)
}

// GetVMListQueryBuilder returns a KQL query builder for listing VMs with nodepool tags
func GetVMListQueryBuilder(rg string) *kql.Builder {
	return getResourceListQueryBuilder(rg, vmResourceType)
}

// GetNICListQueryBuilder returns a KQL query builder for listing NICs with nodepool tags
func GetNICListQueryBuilder(rg string) *kql.Builder {
	return getResourceListQueryBuilder(rg, nicResourceType)
}

// createVMFromQueryResponseData converts ARG query response data into a VirtualMachine object
func createVMFromQueryResponseData(data map[string]interface{}) (*armcompute.VirtualMachine, error) {
	jsonString, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	vm := armcompute.VirtualMachine{}
	err = json.Unmarshal(jsonString, &vm)
	if err != nil {
		return nil, err
	}
	if vm.ID == nil {
		return nil, fmt.Errorf("virtual machine is missing id")
	}
	if vm.Name == nil {
		return nil, fmt.Errorf("virtual machine is missing name")
	}
	if vm.Tags == nil {
		return nil, fmt.Errorf("virtual machine is missing tags")
	}
	// We see inconsistent casing being returned by ARG for the last segment
	// of the vm.ID string. This forces it to be lowercase.
	parts := strings.Split(lo.FromPtr(vm.ID), "/")
	parts[len(parts)-1] = strings.ToLower(parts[len(parts)-1])
	vm.ID = lo.ToPtr(strings.Join(parts, "/"))
	return &vm, nil
}

// createNICFromQueryResponseData converts ARG query response data into a Network Interface object
func createNICFromQueryResponseData(data map[string]interface{}) (*armnetwork.Interface, error) {
	jsonString, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	nic := armnetwork.Interface{}
	err = json.Unmarshal(jsonString, &nic)
	if err != nil {
		return nil, err
	}
	if nic.ID == nil {
		return nil, fmt.Errorf("network interface is missing id")
	}
	if nic.Name == nil {
		return nil, fmt.Errorf("network interface is missing name")
	}
	if nic.Tags == nil {
		return nil, fmt.Errorf("network interface is missing tags")
	}
	// We see inconsistent casing being returned by ARG for the last segment
	// of the nic.ID string. This forces it to be lowercase.
	parts := strings.Split(lo.FromPtr(nic.ID), "/")
	parts[len(parts)-1] = strings.ToLower(parts[len(parts)-1])
	nic.ID = lo.ToPtr(strings.Join(parts, "/"))
	return &nic, nil
}
