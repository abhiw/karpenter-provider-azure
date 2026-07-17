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
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"github.com/samber/lo"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"

	"github.com/Azure/karpenter-provider-azure/pkg/providers/azclient/fleet"
	"github.com/Azure/karpenter-provider-azure/pkg/providers/instance/fleetvmpoller"
	"github.com/Azure/karpenter-provider-azure/pkg/providers/instance/offerings"
	"github.com/Azure/karpenter-provider-azure/pkg/providers/instancetype"
	"github.com/Azure/karpenter-provider-azure/pkg/utils"
)

// FleetMemberPromise implements the Promise interface for Fleet-provisioned VMs.
// Unlike VirtualMachinePromise, VM identity is unknown at construction time —
// fields are populated lazily inside Wait() after the batch completes.
type FleetMemberPromise struct {
	sharedState   *fleet.FleetSharedState
	nodeClaimName string
	capacityType  string
	fleetName     string
	vmProvider    VMProvider

	// Fields for GET-based provisioning state polling and error handling.
	// These are plumbed from DefaultFleetProvider.BeginCreate() at construction.
	ctx                  context.Context
	vmClient             fleet.VMAPI
	resourceGroup        string
	errorHandling        *offerings.ResponseErrorHandler
	instanceTypeProvider instancetype.Provider
	pollerOptions        *fleetvmpoller.Options // nil means use DefaultOptions()

	// Populated after Wait() completes successfully
	VM           *armcompute.VirtualMachine
	InstanceType *cloudprovider.InstanceType
	Zone         string
	// ProviderID is the canonical NodeClaim ProviderID (azure:// prefixed, lowercase RG).
	ProviderID string
}

// Ensure FleetMemberPromise implements Promise.
var _ Promise = (*FleetMemberPromise)(nil)

// Wait blocks until the fleet batch completes, a VM is assigned to this NodeClaim,
// and the VM's provisioningState reaches a terminal state (Succeeded or Failed).
//
// If the VM reaches Failed, Wait invokes the error handler to mark offerings as
// unavailable in the cache (same pattern as SI VM's WaitFunc closure) and returns
// an error so the background goroutine triggers cleanup.
func (p *FleetMemberPromise) Wait() error {
	if err := p.sharedState.GetError(); err != nil {
		return err
	}

	assignment := p.sharedState.GetAssignment(p.nodeClaimName)
	if assignment == nil {
		return cloudprovider.NewInsufficientCapacityError(
			fmt.Errorf("no VM assigned for NodeClaim %s in fleet %s", p.nodeClaimName, p.fleetName))
	}

	p.InstanceType = assignment.InstanceType
	p.Zone = assignment.Zone

	// The assignment carries a minimal VM (ID, Name, VMSize, Zone, ProvisioningState)
	// from listFleetVMs. We need to poll until provisioningState is terminal.
	vmName := lo.FromPtr(assignment.VM.Name)
	if vmName == "" {
		return fmt.Errorf("fleet assignment for NodeClaim %s has no VM name", p.nodeClaimName)
	}

	// Poll compute GET until provisioningState reaches Succeeded or Failed.
	vm, pollErr := p.pollVMProvisioning(vmName)
	if pollErr != nil {
		p.handleFailedProvisioning(pollErr)
		return pollErr
	}

	// Provisioning succeeded - populate promise with the full VM object.
	p.VM = vm
	if p.VM != nil && p.VM.ID != nil {
		p.ProviderID = utils.VMResourceIDToProviderID(p.ctx, *p.VM.ID)
	}
	return nil
}

// pollVMProvisioning polls GET /virtualMachines/{name} until provisioningState is terminal.
// Uses the same polling pattern as aksmachinepoller (5s interval, exponential backoff).
func (p *FleetMemberPromise) pollVMProvisioning(vmName string) (*armcompute.VirtualMachine, error) {
	if p.vmClient == nil || p.ctx == nil {
		// Legacy/test path without polling dependencies - fall back to assignment VM as-is.
		assignment := p.sharedState.GetAssignment(p.nodeClaimName)
		if assignment != nil && assignment.VM != nil {
			return assignment.VM, nil
		}
		return nil, fmt.Errorf("no VM client available to poll fleet VM %q", vmName)
	}

	opts := fleetvmpoller.DefaultOptions()
	if p.pollerOptions != nil {
		opts = *p.pollerOptions
	}
	poller := fleetvmpoller.NewPoller(opts, p.vmClient, p.resourceGroup, vmName)
	return poller.PollUntilDone(p.ctx)
}

// handleFailedProvisioning invokes the error handler to mark offerings as unavailable
// in the cache. This is the Fleet equivalent of SI VM's error handling in the WaitFunc
// closure (vminstance.go:891-900).
func (p *FleetMemberPromise) handleFailedProvisioning(pollErr error) {
	if p.errorHandling == nil || p.instanceTypeProvider == nil || p.InstanceType == nil {
		return
	}

	ctx := p.ctx
	if ctx == nil {
		ctx = context.TODO()
	}

	sku, err := p.instanceTypeProvider.Get(ctx, p.InstanceType.Name)
	if err != nil {
		log.FromContext(ctx).Error(err, "failed to resolve SKU for failed fleet VM",
			"sku", p.InstanceType.Name, "nodeClaimName", p.nodeClaimName)
		return
	}

	// Feed the error to the same handler chain used by SI VM.
	handledErr := p.errorHandling.Handle(ctx, sku, p.InstanceType, p.Zone, p.capacityType, pollErr)
	if handledErr != nil {
		log.FromContext(ctx).Info("fleet VM provisioning failure handled, offerings marked unavailable",
			"nodeClaimName", p.nodeClaimName, "sku", p.InstanceType.Name,
			"zone", p.Zone, "capacityType", p.capacityType)
	} else {
		log.FromContext(ctx).Info("fleet VM provisioning failed with unhandled error code",
			"nodeClaimName", p.nodeClaimName, "error", pollErr.Error())
	}
}

// Cleanup deletes the assigned VM if one exists. No-op if Wait() wasn't called or no VM was assigned.
// Routes through VMProvider.Delete so it inherits the same in-process dedupe,
// pre-Get idempotency check, IsVMDeleting short-circuit, and ForceDeletion=true semantics
// used by user-initiated CloudProvider.Delete.
func (p *FleetMemberPromise) Cleanup(ctx context.Context) error {
	if p.VM == nil || p.VM.Name == nil {
		return nil
	}
	if p.vmProvider != nil {
		return p.vmProvider.Delete(ctx, *p.VM.Name)
	}
	// Fallback for paths that did not plumb a VMProvider (e.g. legacy test constructors).
	vmClient := p.sharedState.GetVMClient()
	if vmClient == nil {
		return nil
	}
	poller, err := vmClient.BeginDelete(ctx, p.sharedState.GetResourceGroup(), *p.VM.Name, nil)
	if err != nil {
		return fmt.Errorf("cleanup VM %s: %w", *p.VM.Name, err)
	}
	_, err = poller.PollUntilDone(ctx, nil)
	return err
}

// GetInstanceName returns the assigned VM name, or empty string if not yet assigned.
func (p *FleetMemberPromise) GetInstanceName() string {
	if p.VM != nil && p.VM.Name != nil {
		return *p.VM.Name
	}
	return ""
}
