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

// Package fleettag applies the nodeclaim-name owner tag to Fleet-provisioned VMs
// once they have been assigned to a NodeClaim.
//
// The Fleet executor lists the VMs produced by a batch and hands each one to a
// NodeClaim synchronously (no LRO polling), but it does not tag the VMs on the
// critical path. This controller reconciles the association out-of-band: it lists
// Fleet-provisioned VMs (those carrying fleet.FleetNameTagKey), matches each to a
// NodeClaim by ProviderID, and PATCHes the nodeclaim-name tag onto VMs that have an
// owner but are not yet tagged. Surplus VMs that are never assigned to a NodeClaim
// carry no NodeClaim ProviderID and are reclaimed by the generic instance garbage
// collector; this controller simply leaves them untagged.
package fleettag

import (
	"context"
	"fmt"
	"time"

	"github.com/awslabs/operatorpkg/reconciler"
	"github.com/awslabs/operatorpkg/singleton"
	"github.com/samber/lo"
	"go.uber.org/multierr"
	"k8s.io/client-go/util/workqueue"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/operator/injection"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"

	"github.com/Azure/karpenter-provider-azure/pkg/providers/azclient/fleet"
	"github.com/Azure/karpenter-provider-azure/pkg/providers/instance"
	"github.com/Azure/karpenter-provider-azure/pkg/providers/launchtemplate"
	"github.com/Azure/karpenter-provider-azure/pkg/utils"
)

const (
	// NodeClaimNameTagKey is the per-VM owner tag that records which NodeClaim a
	// Fleet-provisioned VM belongs to. It is applied by this controller after the
	// VM has been matched to a NodeClaim ProviderID.
	NodeClaimNameTagKey = "karpenter.azure.com_nodeclaim-name"

	// ControllerName is the metric/log name for this controller.
	ControllerName = "fleetvm.tagging"

	// tagWorkerParallelism caps concurrent BeginUpdate calls per reconcile.
	tagWorkerParallelism = 10
)

// Controller is a singleton controller that periodically lists Fleet-provisioned VMs,
// matches each to a NodeClaim by ProviderID, and PATCHes the nodeclaim-name tag onto
// VMs that have an owner but are not yet tagged.
type Controller struct {
	kubeClient  client.Client
	vmProvider  instance.VMProvider
	vmClient    fleet.VMAPI
	clusterName string
	rg          string
	interval    time.Duration
}

// NewController constructs the Fleet VM tagging controller.
//
// interval is the reconcile cadence.
func NewController(
	kubeClient client.Client,
	vmProvider instance.VMProvider,
	vmClient fleet.VMAPI,
	clusterName string,
	resourceGroup string,
	interval time.Duration,
) *Controller {
	return &Controller{
		kubeClient:  kubeClient,
		vmProvider:  vmProvider,
		vmClient:    vmClient,
		clusterName: clusterName,
		rg:          resourceGroup,
		interval:    interval,
	}
}

// tagWork pairs a VM name with the NodeClaim name to record on it, along with the
// merged tag set to PATCH.
type tagWork struct {
	vmName string
	ncName string
	tags   map[string]*string
}

// Reconcile runs one tagging cycle.
func (c *Controller) Reconcile(ctx context.Context) (reconciler.Result, error) {
	ctx = injection.WithControllerName(ctx, ControllerName)
	logger := log.FromContext(ctx)

	vms, err := c.vmProvider.ListFleetVMs(ctx)
	if err != nil {
		return reconciler.Result{}, fmt.Errorf("listing fleet VMs: %w", err)
	}

	nodeClaimList := &karpv1.NodeClaimList{}
	if err := c.kubeClient.List(ctx, nodeClaimList); err != nil {
		return reconciler.Result{}, fmt.Errorf("listing nodeclaims: %w", err)
	}
	ncByProviderID := make(map[string]string, len(nodeClaimList.Items))
	for i := range nodeClaimList.Items {
		nc := &nodeClaimList.Items[i]
		if nc.Status.ProviderID != "" {
			ncByProviderID[nc.Status.ProviderID] = nc.Name
		}
	}

	work := c.buildTagWork(ctx, vms, ncByProviderID)
	if len(work) == 0 {
		return reconciler.Result{RequeueAfter: c.interval}, nil
	}

	logger.Info("tagging fleet VMs with nodeclaim-name", "count", len(work))

	errs := make([]error, len(work))
	workqueue.ParallelizeUntil(ctx, tagWorkerParallelism, len(work), func(i int) {
		w := work[i]
		if err := c.tagVM(ctx, w); err != nil {
			errs[i] = err
			return
		}
		logger.V(1).Info("tagged fleet VM", "vmName", w.vmName, "nodeclaim", w.ncName)
	})

	if err := multierr.Combine(errs...); err != nil {
		return reconciler.Result{}, err
	}
	return reconciler.Result{RequeueAfter: c.interval}, nil
}

// buildTagWork identifies Fleet-provisioned VMs that have a NodeClaim owner but are
// not yet tagged with nodeclaim-name. A VM is eligible iff all of the following hold:
//  1. It carries fleet.FleetNameTagKey (it was created by the Fleet executor).
//  2. It does NOT already carry NodeClaimNameTagKey (it is not yet tagged).
//  3. It carries launchtemplate.KarpenterManagedTagKey set to this cluster's name
//     (defense-in-depth: never touch another cluster's VMs even in a shared RG).
//  4. Its ProviderID maps to a live NodeClaim (it has been assigned an owner).
func (c *Controller) buildTagWork(ctx context.Context, vms []*armcompute.VirtualMachine, ncByProviderID map[string]string) []tagWork {
	var work []tagWork
	for _, vm := range vms {
		if vm == nil || vm.Name == nil || vm.ID == nil || vm.Tags == nil {
			continue
		}
		if !hasTag(vm.Tags, fleet.FleetNameTagKey) {
			continue
		}
		if hasTag(vm.Tags, NodeClaimNameTagKey) {
			continue
		}
		if !tagEquals(vm.Tags, launchtemplate.KarpenterManagedTagKey, c.clusterName) {
			continue
		}
		providerID := utils.VMResourceIDToProviderID(ctx, *vm.ID)
		ncName, ok := ncByProviderID[providerID]
		if !ok {
			// No NodeClaim owner yet (unassigned or surplus). Leave untagged;
			// instance GC reclaims surplus VMs by ProviderID.
			continue
		}
		// Merge: start with the VM's existing tags (inherited from Fleet), then add
		// the nodeclaim-name tag so we never drop tags on the PATCH.
		mergedTags := make(map[string]*string, len(vm.Tags)+1)
		for k, v := range vm.Tags {
			mergedTags[k] = v
		}
		mergedTags[NodeClaimNameTagKey] = lo.ToPtr(ncName)
		work = append(work, tagWork{vmName: *vm.Name, ncName: ncName, tags: mergedTags})
	}
	return work
}

// tagVM PATCHes the merged tag set onto a single VM.
func (c *Controller) tagVM(ctx context.Context, w tagWork) error {
	update := armcompute.VirtualMachineUpdate{Tags: w.tags}
	poller, err := c.vmClient.BeginUpdate(ctx, c.rg, w.vmName, update, nil)
	if err != nil {
		return fmt.Errorf("tagging VM %q: %w", w.vmName, err)
	}
	if poller == nil {
		return nil
	}
	if _, err := poller.PollUntilDone(ctx, nil); err != nil {
		return fmt.Errorf("waiting for tag update on VM %q: %w", w.vmName, err)
	}
	return nil
}

func hasTag(tags map[string]*string, key string) bool {
	v, ok := tags[key]
	return ok && v != nil && *v != ""
}

func tagEquals(tags map[string]*string, key, expected string) bool {
	v, ok := tags[key]
	return ok && v != nil && *v == expected
}

// Register registers the controller with the manager using the singleton pattern.
func (c *Controller) Register(_ context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named(ControllerName).
		WatchesRawSource(singleton.Source()).
		Complete(singleton.AsReconciler(c))
}
