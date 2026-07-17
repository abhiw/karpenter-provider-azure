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

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
)

// NewFleetSharedState creates a shared state for a batch. Called by the executor
// after submitting the Fleet create. The VMs field is populated by the executor
// after listing Fleet VMs (via ARG or VMSS list).
func NewFleetSharedState(
	requests []*VMAssignmentRequest,
	instanceTypes map[string]*cloudprovider.InstanceType,
	vmClient VMAPI,
	fleetName,
	resourceGroup string,
) *FleetSharedState {
	return &FleetSharedState{
		requests:      requests,
		instanceTypes: instanceTypes,
		vmClient:      vmClient,
		fleetName:     fleetName,
		resourceGroup: resourceGroup,
	}
}

// NewFleetSharedStateForTest creates a shared state with pre-resolved VMs (no LRO).
// Used in unit tests to bypass the poller and directly test the assign→tag→delete flow.
func NewFleetSharedStateForTest(
	vms []*armcompute.VirtualMachine,
	requests []*VMAssignmentRequest,
	instanceTypes map[string]*cloudprovider.InstanceType,
	vmClient VMAPI,
	fleetName, resourceGroup string,
) *FleetSharedState {
	return &FleetSharedState{
		injectedVMs:   vms,
		requests:      requests,
		instanceTypes: instanceTypes,
		vmClient:      vmClient,
		fleetName:     fleetName,
		resourceGroup: resourceGroup,
	}
}

// RunAssignmentForTest exposes runAssignment to external-package tests
// (e.g. the instance package) that construct a state via NewFleetSharedStateForTest
// and need assignments populated before exercising the read-only promise path.
func (s *FleetSharedState) RunAssignmentForTest(ctx context.Context) {
	s.runAssignment(ctx)
}

// SetVMs allows the executor to inject listed VMs before promises call Wait().
// This is the production path (vs injectedVMs set at construction for tests).
func (s *FleetSharedState) SetVMs(vms []*armcompute.VirtualMachine) {
	s.injectedVMs = vms
}

// SetError allows the executor to set a batch-wide error (e.g., LRO failure).
func (s *FleetSharedState) SetError(err error) {
	s.err = err
}

// runAssignment is called by the executor exactly once, AFTER SetVMs and BEFORE
// distributeSharedState. It only does the assignment matching (fast, in-memory)
// so that promises receive providerIDs as quickly as possible.
//
// Tagging of assigned VMs is performed out-of-band by the fleettag controller.
// Surplus VMs (created but never assigned to a NodeClaim) are reclaimed by the
// generic instance garbage collector via ProviderID, so this function does not
// delete them.
func (s *FleetSharedState) runAssignment(ctx context.Context) {
	// If executor already set an error (create failure), short-circuit.
	if s.err != nil {
		return
	}

	vms := s.injectedVMs
	if len(vms) == 0 && len(s.requests) > 0 {
		s.err = fmt.Errorf("no VMs available for fleet %s", s.fleetName)
		return
	}

	// Run assignment (in-memory, fast).
	assigned, _, surplus := AssignVMsToNodeClaims(s.requests, vms, s.instanceTypes)
	s.assignments = assigned
	if len(surplus) > 0 {
		log.FromContext(ctx).Info("fleet produced surplus VMs; leaving them for instance GC",
			"fleet", s.fleetName, "surplusCount", len(surplus))
	}
}

// GetAssignment returns the assignment for a NodeClaim, or nil if unmatched.
func (s *FleetSharedState) GetAssignment(nodeClaimName string) *FleetAssignment {
	if s.assignments == nil {
		return nil
	}
	return s.assignments[nodeClaimName]
}

// GetError returns the batch-wide error, or nil if the fleet create succeeded.
func (s *FleetSharedState) GetError() error {
	return s.err
}

// GetVMClient returns the VM API client for cleanup operations.
func (s *FleetSharedState) GetVMClient() VMAPI {
	return s.vmClient
}

// GetResourceGroup returns the resource group name.
func (s *FleetSharedState) GetResourceGroup() string {
	return s.resourceGroup
}
