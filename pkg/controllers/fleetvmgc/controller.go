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

// Package fleetvmgc implements per-VM garbage collection for Fleet-provisioned VMs
// that exist in Azure but were never assigned to a NodeClaim.
//
// Such VMs carry the fleet-name tag (applied by the Fleet executor on every member VM)
// but lack the nodeclaim-name tag (applied by the shared poll after assignment).
// They are produced when the controller crashes between LRO completion and tag PATCH,
// when surplus deletion fails, or when assignment fails after VMs are already provisioned.
// They are excluded from Instance GC because they have no live NodeClaim ProviderID
// (the assignment never happened), and Fleet ARM GC will not clean their parent Fleet
// because they still count as members.
package fleetvmgc

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
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	corecloudprovider "sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/operator/injection"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"

	"github.com/Azure/karpenter-provider-azure/pkg/providers/azclient/fleet"
	"github.com/Azure/karpenter-provider-azure/pkg/providers/instance"
	"github.com/Azure/karpenter-provider-azure/pkg/providers/launchtemplate"
)

const (
	// NodeClaimNameTagKey is the per-VM owner tag applied by the shared poll after assignment.
	// Its absence on a Fleet-provisioned VM (one that carries fleet.FleetNameTagKey) is the
	// signal that the VM is an orphan.
	NodeClaimNameTagKey = "karpenter.azure.com_nodeclaim-name"

	// ControllerName is the metric/log name for this controller.
	ControllerName = "fleetvm.garbagecollection"

	// gcWorkerParallelism caps concurrent BeginDelete calls per reconcile.
	gcWorkerParallelism = 10
)

// Controller is a singleton controller that periodically lists Fleet-provisioned VMs
// in the node resource group, identifies orphans (have fleet-name tag, lack nodeclaim-name
// tag, older than the grace period, owned by this cluster), and deletes them via the
// shared VMProvider.Delete primitive.
type Controller struct {
	vmProvider  instance.VMProvider
	clusterName string
	interval    time.Duration
	grace       time.Duration
}

// NewController constructs the per-VM Fleet GC controller.
//
// interval is the reconcile cadence (typically 5 min, matching NRP rate-limit windows).
// grace is the minimum age before an untagged Fleet VM is considered an orphan
// (typically 15 min, ~30× the natural in-flight window).
func NewController(
	vmProvider instance.VMProvider,
	clusterName string,
	interval time.Duration,
	grace time.Duration,
) *Controller {
	return &Controller{
		vmProvider:  vmProvider,
		clusterName: clusterName,
		interval:    interval,
		grace:       grace,
	}
}

// Reconcile runs one GC cycle.
func (c *Controller) Reconcile(ctx context.Context) (reconciler.Result, error) {
	ctx = injection.WithControllerName(ctx, ControllerName)
	logger := log.FromContext(ctx)

	vms, err := c.vmProvider.List(ctx)
	if err != nil {
		return reconciler.Result{}, fmt.Errorf("listing VMs: %w", err)
	}

	orphans := c.filterOrphans(vms, time.Now())
	if len(orphans) == 0 {
		return reconciler.Result{RequeueAfter: c.interval}, nil
	}

	logger.Info("garbage collecting orphan fleet VMs", "count", len(orphans))

	errs := make([]error, len(orphans))
	workqueue.ParallelizeUntil(ctx, gcWorkerParallelism, len(orphans), func(i int) {
		vmName := lo.FromPtr(orphans[i].Name)
		if err := c.vmProvider.Delete(ctx, vmName); err != nil {
			errs[i] = corecloudprovider.IgnoreNodeClaimNotFoundError(err)
			return
		}
		logger.V(1).Info("garbage collected orphan fleet VM", "vmName", vmName)
	})

	if err := multierr.Combine(errs...); err != nil {
		return reconciler.Result{}, err
	}
	return reconciler.Result{RequeueAfter: c.interval}, nil
}

// filterOrphans applies the orphan predicate to a list of VMs.
// A VM is an orphan iff all of the following are true:
//  1. It carries fleet.FleetNameTagKey (it was created by the Fleet executor).
//  2. It does NOT carry NodeClaimNameTagKey (it was never assigned to a NodeClaim).
//  3. It carries launchtemplate.KarpenterManagedTagKey set to this cluster's name
//     (defense-in-depth: never touch another cluster's VMs even in a shared RG).
//  4. Its Properties.TimeCreated is at least `grace` in the past (skip in-flight VMs).
//     A nil TimeCreated is treated as "unknown" and excluded — safer than guessing.
func (c *Controller) filterOrphans(vms []*armcompute.VirtualMachine, now time.Time) []*armcompute.VirtualMachine {
	var orphans []*armcompute.VirtualMachine
	for _, vm := range vms {
		if vm == nil || vm.Name == nil || vm.Tags == nil {
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
		if vm.Properties == nil || vm.Properties.TimeCreated == nil {
			continue
		}
		if now.Sub(*vm.Properties.TimeCreated) < c.grace {
			continue
		}
		orphans = append(orphans, vm)
	}
	return orphans
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
