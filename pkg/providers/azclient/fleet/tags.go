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

// Package fleet — canonical Karpenter tag keys applied to Azure Compute Fleet
// resources and the VMs they create.
//
// All Karpenter-set tag keys on Fleets / Fleet-VMs live here so that the
// executor, sharedstate, and any future ARG/instance queries reference one
// place. When propagation behavior or value formats change, this file is the
// single point of edit.
package fleet

const (
	// FleetNameTagKey is applied to the Fleet body. Compute Fleet propagates
	// Fleet.Tags to its backing VMSS and onto each VM, so this tag is also
	// visible on the individual VMs. Used to discover VMs created by a Fleet
	// after the LRO completes (executor.listFleetVMs).
	FleetNameTagKey = "karpenter.azure.com_fleet-name"

	// ManagedByTagKey identifies Karpenter-owned Fleet resources. Value is
	// always ManagedByTagValue ("karpenter").
	ManagedByTagKey   = "karpenter.azure.com_managed-by"
	ManagedByTagValue = "karpenter"

	// ClusterNameTagKey identifies the owning AKS cluster.
	ClusterNameTagKey = "karpenter.azure.com_cluster-name"

	// BatchKeyHashTagKey is the 8-char hash that uniquely identifies the
	// batch a Fleet (and its VMs) came from. Propagated to VMs via the same
	// Fleet → VMSS → VM tag inheritance as FleetNameTagKey.
	//
	// FUTURE USE — not yet read by any production code path. Set now so the
	// follow-up ARG in-flight exclusion fix (see TODO(fleet-arg-exclusion) in
	// pkg/providers/instance/azureresourcegraphlist.go) is a single KQL clause
	// with no executor change required at that time. Conceptually: a VM that
	// has BatchKeyHashTagKey but does NOT yet have NodeClaimNameTagKey is in
	// the window between Fleet LRO completion and per-VM nodeclaim tagging,
	// and must be excluded from CloudProvider.List so the existing nodeclaim
	// GC does not race the Fleet assignment.
	BatchKeyHashTagKey = "karpenter.azure.com_batch-key-hash"

	// NodeClaimNameTagKey is applied to a VM by sharedstate.tagAssignedVMs
	// once the VM has been matched to a specific NodeClaim. Used by the
	// CloudProvider.Delete tag-based fallback to find a VM when the
	// NodeClaim's Status.ProviderID is empty (e.g., Karpenter crashed mid-Create).
	//
	// FUTURE USE — also the second half of the deferred ARG in-flight
	// exclusion rule documented on BatchKeyHashTagKey above.
	NodeClaimNameTagKey = "karpenter.azure.com_nodeclaim-name"
)
