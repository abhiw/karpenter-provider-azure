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

// ResolveAssignment reads the batch assignment and populates .VM, .InstanceType,
// .Zone, and .ProviderID from the stub VM returned by listFleetVMs. This is a
// fast, in-memory operation (no network calls) and must be called synchronously
// before handing the promise to handleInstancePromise for async polling.
//
// After ResolveAssignment succeeds, the promise carries enough data to construct
// a NodeClaim (vmInstanceToNodeClaim reads ID, Name, VMSize, Zone, Location from
// .VM). Fields not available from the Fleet SDK (Tags, TimeCreated, ImageReference,
// ProvisioningState) have acceptable fallbacks in core — they are filled later
// either by the Registration controller or CloudProvider.Get().
func (p *FleetMemberPromise) ResolveAssignment() error {
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
	p.VM = assignment.VM

	if p.VM == nil || p.VM.Name == nil {
		return fmt.Errorf("fleet assignment for NodeClaim %s has no VM", p.nodeClaimName)
	}
	if p.VM.ID != nil {
		p.ProviderID = utils.VMResourceIDToProviderID(p.ctx, *p.VM.ID)
	}
	return nil
}

// Wait polls the VM's provisioningState until it reaches a terminal state
// (Succeeded or Failed). This is designed to run asynchronously inside
// handleInstancePromise's goroutine — it provides fast failure detection
// (~5-60s) instead of relying on core's 15-min liveness timeout.
//
// If the VM reaches Failed, Wait invokes the error handler to mark offerings as
// unavailable in the cache (same pattern as SI VM's WaitFunc closure) and returns
// an error so the background goroutine triggers cleanup + NodeClaim deletion.
//
// On success, Wait updates .VM with the full VM object from the compute GET
// (which includes Tags, TimeCreated, ImageReference, etc.) — though the NodeClaim
// was already created from the stub VM by ResolveAssignment, these fields are
// informational and not required for correctness.
func (p *FleetMemberPromise) Wait() error {
	vmName := lo.FromPtr(p.VM.Name)
	if vmName == "" {
		return fmt.Errorf("fleet promise for NodeClaim %s has no VM name (call ResolveAssignment first)", p.nodeClaimName)
	}

	// Poll compute GET until provisioningState reaches Succeeded or Failed.
	vm, pollErr := p.pollVMProvisioning(vmName)
	if pollErr != nil {
		p.handleFailedProvisioning(pollErr)
		return pollErr
	}

	// Provisioning succeeded — update .VM with the full object.
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
