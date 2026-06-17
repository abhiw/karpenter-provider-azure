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

package fleet_test

import (
	"context"
	"fmt"
	"time"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/computefleet/armcomputefleet/v2"
	"github.com/Azure/karpenter-provider-azure/pkg/apis/v1beta1"
	"github.com/Azure/karpenter-provider-azure/test/pkg/environment/azure"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"sigs.k8s.io/karpenter/pkg/test"
)

var _ = Describe("Fleet", func() {
	Describe("On-Demand Provisioning", func() {
		It("should provision a single on-demand node via Fleet", func() {
			nodePool = test.ReplaceRequirements(nodePool, karpv1.NodeSelectorRequirementWithMinValues{
				Key:      karpv1.CapacityTypeLabelKey,
				Operator: corev1.NodeSelectorOpIn,
				Values:   []string{karpv1.CapacityTypeOnDemand},
			})
			env.ExpectCreated(nodePool, nodeClass)

			podLabels := map[string]string{"app": "fleet-ondemand-test"}
			dep := test.Deployment(test.DeploymentOptions{
				Replicas: 1,
				PodOptions: test.PodOptions{
					ObjectMeta: metav1.ObjectMeta{Labels: podLabels},
					ResourceRequirements: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("100m"),
							corev1.ResourceMemory: resource.MustParse("128Mi"),
						},
					},
					TerminationGracePeriodSeconds: lo.ToPtr(int64(0)),
				},
			})
			env.ExpectCreated(dep)

			// Verify pod becomes healthy
			env.EventuallyExpectHealthyPodCount(labels.SelectorFromSet(dep.Spec.Selector.MatchLabels), 1)

			// Verify node is created with on-demand label
			nodes := env.ExpectCreatedNodeCount("==", 1)
			Expect(nodes[0].Labels).To(HaveKeyWithValue(karpv1.CapacityTypeLabelKey, karpv1.CapacityTypeOnDemand))

			// Verify a Fleet resource exists in the node RG (proves Fleet path was used)
			fleets := listFleets(env)
			Expect(len(fleets)).To(BeNumerically(">=", 1))
			found := lo.ContainsBy(fleets, func(f *armcomputefleet.Fleet) bool {
				return f.Name != nil && len(*f.Name) > 0
			})
			Expect(found).To(BeTrue(), "expected at least one Fleet resource")

			env.ExpectDeleted(dep)
		})
	})

	Describe("Spot Provisioning", func() {
		It("should provision a spot node via Fleet", func() {
			nodePool = test.ReplaceRequirements(nodePool, karpv1.NodeSelectorRequirementWithMinValues{
				Key:      karpv1.CapacityTypeLabelKey,
				Operator: corev1.NodeSelectorOpIn,
				Values:   []string{karpv1.CapacityTypeSpot},
			})
			env.ExpectCreated(nodePool, nodeClass)

			podLabels := map[string]string{"app": "fleet-spot-test"}
			dep := test.Deployment(test.DeploymentOptions{
				Replicas: 1,
				PodOptions: test.PodOptions{
					ObjectMeta: metav1.ObjectMeta{Labels: podLabels},
					NodeSelector: map[string]string{
						karpv1.CapacityTypeLabelKey: karpv1.CapacityTypeSpot,
					},
					Tolerations: []corev1.Toleration{
						{
							Key:      "kubernetes.azure.com/scalesetpriority",
							Operator: corev1.TolerationOpEqual,
							Value:    "spot",
							Effect:   corev1.TaintEffectNoSchedule,
						},
					},
					ResourceRequirements: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("100m"),
							corev1.ResourceMemory: resource.MustParse("128Mi"),
						},
					},
					TerminationGracePeriodSeconds: lo.ToPtr(int64(0)),
				},
			})
			env.ExpectCreated(dep)

			// Verify pod is healthy
			env.EventuallyExpectHealthyPodCount(labels.SelectorFromSet(dep.Spec.Selector.MatchLabels), 1)

			// Verify node labels
			nodes := env.ExpectCreatedNodeCount("==", 1)
			Expect(nodes[0].Labels).To(HaveKeyWithValue(karpv1.CapacityTypeLabelKey, karpv1.CapacityTypeSpot))
			Expect(nodes[0].Labels).To(HaveKeyWithValue(v1beta1.AKSLabelScaleSetPriority, v1beta1.ScaleSetPrioritySpot))

			env.ExpectDeleted(dep)
		})
	})

	Describe("Batch Multiple NodeClaims", func() {
		It("should batch 3 NodeClaims into a single Fleet PUT", func() {
			nodePool = test.ReplaceRequirements(nodePool, karpv1.NodeSelectorRequirementWithMinValues{
				Key:      karpv1.CapacityTypeLabelKey,
				Operator: corev1.NodeSelectorOpIn,
				Values:   []string{karpv1.CapacityTypeOnDemand},
			})
			// Snapshot fleet IDs that already exist in the node RG BEFORE we create the
			// workload. The node RG is shared (test re-runs, parallel suites, leaked fleets
			// from prior aborted runs), so an absolute count like Equal(1) is brittle; we
			// must assert on the delta produced by THIS test only.
			fleetsBefore := fleetIDSet(env)

			env.ExpectCreated(nodePool, nodeClass)

			podLabels := map[string]string{"app": "fleet-batch-test"}
			dep := test.Deployment(test.DeploymentOptions{
				Replicas: 3,
				PodOptions: test.PodOptions{
					ObjectMeta: metav1.ObjectMeta{Labels: podLabels},
					ResourceRequirements: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("100m"),
							corev1.ResourceMemory: resource.MustParse("128Mi"),
						},
					},
					// Force 1 pod per node so 3 replicas => 3 NodeClaims (the batch under test),
					// regardless of which SKU Karpenter happens to pick. Relying on CPU/memory
					// alone is unreliable: a large SKU (e.g. D8s) trivially packs all 3 pods on
					// one node and the test asserts 1 node instead of 3.
					PodAntiRequirements: []corev1.PodAffinityTerm{{
						LabelSelector: &metav1.LabelSelector{MatchLabels: podLabels},
						TopologyKey:   corev1.LabelHostname,
					}},
					TerminationGracePeriodSeconds: lo.ToPtr(int64(0)),
				},
			})
			env.ExpectCreated(dep)

			// Verify all 3 pods are healthy
			env.EventuallyExpectHealthyPodCount(labels.SelectorFromSet(dep.Spec.Selector.MatchLabels), 3)

			// Verify 3 nodes created
			nodes := env.EventuallyExpectCreatedNodeCount("==", 3)
			for _, node := range nodes {
				Expect(node.Labels).To(HaveKeyWithValue(karpv1.CapacityTypeLabelKey, karpv1.CapacityTypeOnDemand))
			}

			// Verify batching: exactly 1 NEW Fleet resource should have appeared during
			// this test. We compare the post-test set against the snapshot taken before
			// ExpectCreated, instead of asserting an absolute count over the whole RG.
			newFleets := fleetIDSet(env).Difference(fleetsBefore)
			Expect(newFleets.Len()).To(Equal(1),
				"expected exactly 1 new Fleet resource for batched requests, got %d (new IDs: %v)",
				newFleets.Len(), newFleets.UnsortedList())

			env.ExpectDeleted(dep)
		})
	})

	Describe("Zone Constraints", func() {
		It("should respect zone requirements in Fleet provisioning", func() {
			if !env.SupportsZones() {
				Skip("region does not support availability zones")
			}

			zones := env.GetAvailableZones()
			if len(zones) == 0 {
				Skip("no available zones")
			}
			targetZone := zones[0] // Pick first available zone

			nodePool = test.ReplaceRequirements(nodePool,
				karpv1.NodeSelectorRequirementWithMinValues{
					Key:      karpv1.CapacityTypeLabelKey,
					Operator: corev1.NodeSelectorOpIn,
					Values:   []string{karpv1.CapacityTypeOnDemand},
				},
				karpv1.NodeSelectorRequirementWithMinValues{
					Key:      corev1.LabelTopologyZone,
					Operator: corev1.NodeSelectorOpIn,
					Values:   []string{fmt.Sprintf("%s-%s", env.Region, targetZone)},
				},
			)
			env.ExpectCreated(nodePool, nodeClass)

			podLabels := map[string]string{"app": "fleet-zone-test"}
			dep := test.Deployment(test.DeploymentOptions{
				Replicas: 1,
				PodOptions: test.PodOptions{
					ObjectMeta: metav1.ObjectMeta{Labels: podLabels},
					ResourceRequirements: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("100m"),
							corev1.ResourceMemory: resource.MustParse("128Mi"),
						},
					},
					TerminationGracePeriodSeconds: lo.ToPtr(int64(0)),
				},
			})
			env.ExpectCreated(dep)

			// Verify pod is healthy
			env.EventuallyExpectHealthyPodCount(labels.SelectorFromSet(dep.Spec.Selector.MatchLabels), 1)

			// Verify node is in the correct zone
			nodes := env.ExpectCreatedNodeCount("==", 1)
			expectedZoneLabel := fmt.Sprintf("%s-%s", env.Region, targetZone)
			Expect(nodes[0].Labels).To(HaveKeyWithValue(corev1.LabelTopologyZone, expectedZoneLabel))

			env.ExpectDeleted(dep)
		})
	})

	// Verifies the Fleet-specific delete path:
	//   NodeClaim deletion -> CloudProvider.Delete -> GetVMName(ProviderID) -> VMProvider.Delete.
	// Relies on the FleetMemberPromise.Wait() ProviderID fix (must be prefixed "azure://"):
	// without it, the regex in GetVMName fails and the underlying Azure VM is leaked.
	Describe("NodeClaim Delete", func() {
		It("should delete the Azure VM when its NodeClaim is deleted", func() {
			nodePool = test.ReplaceRequirements(nodePool, karpv1.NodeSelectorRequirementWithMinValues{
				Key:      karpv1.CapacityTypeLabelKey,
				Operator: corev1.NodeSelectorOpIn,
				Values:   []string{karpv1.CapacityTypeOnDemand},
			})
			env.ExpectCreated(nodePool, nodeClass)

			podLabels := map[string]string{"app": "fleet-delete-test"}
			dep := test.Deployment(test.DeploymentOptions{
				Replicas: 1,
				PodOptions: test.PodOptions{
					ObjectMeta: metav1.ObjectMeta{Labels: podLabels},
					ResourceRequirements: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("100m"),
							corev1.ResourceMemory: resource.MustParse("128Mi"),
						},
					},
					TerminationGracePeriodSeconds: lo.ToPtr(int64(0)),
				},
			})
			env.ExpectCreated(dep)

			env.EventuallyExpectHealthyPodCount(labels.SelectorFromSet(dep.Spec.Selector.MatchLabels), 1)
			nodes := env.ExpectCreatedNodeCount("==", 1)
			node := nodes[0]

			// Capture VM name BEFORE we delete the NodeClaim so we can still assert in Azure
			// after the K8s objects are gone.
			vmName := env.ExpectParsedProviderID(node.Spec.ProviderID)
			Expect(vmName).ToNot(BeEmpty(), "expected non-empty VM name from ProviderID %q", node.Spec.ProviderID)
			// Sanity: ProviderID must carry the azure:// scheme (the fleet-promise fix). Without
			// this, the cloudprovider regex would fail to extract a VM name and Delete would not
			// reach the VMProvider.
			Expect(node.Spec.ProviderID).To(HavePrefix("azure://"),
				"Fleet-provisioned node missing azure:// ProviderID prefix; fleet promise Wait() regressed")
			// Confirm the VM exists in Azure right now.
			env.GetVMByName(vmName)

			// Drop the workload first so termination doesn't stall on pod drain.
			env.ExpectDeleted(dep)

			ncList := env.EventuallyExpectCreatedNodeClaimCount("==", 1)
			env.ExpectDeleted(ncList[0])

			// Underlying Azure VM must be gone within the standard scale-down window.
			env.EventuallyExpectVMNotFound(vmName, 10*time.Minute)
		})
	})

	// Verifies the per-VM Fleet garbage collector (fleetvmgc):
	// A VM that carries the fleet-name tag but is missing the nodeclaim-name tag is a leak
	// candidate. Once it is older than FLEET_VM_GC_GRACE_PERIOD, fleetvmgc must delete it.
	//
	// PRE-REQUISITE: the in-cluster controller should be running with a moderate grace
	// (e.g. FLEET_VM_GC_GRACE_PERIOD=10m, FLEET_VM_GC_INTERVAL=30s) on the karpenter
	// deployment. Anything < ~5m races the normal Wait()/tag flow and will delete healthy
	// in-flight VMs (FALSE POSITIVES). Anything > ~15m makes the 20m EventuallyExpect
	// budget below too tight.
	Describe("Per-VM Fleet GC", func() {
		It("should delete a Fleet VM that lost its nodeclaim-name tag", func() {
			nodePool = test.ReplaceRequirements(nodePool, karpv1.NodeSelectorRequirementWithMinValues{
				Key:      karpv1.CapacityTypeLabelKey,
				Operator: corev1.NodeSelectorOpIn,
				Values:   []string{karpv1.CapacityTypeOnDemand},
			})
			env.ExpectCreated(nodePool, nodeClass)

			podLabels := map[string]string{"app": "fleet-gc-test"}
			dep := test.Deployment(test.DeploymentOptions{
				Replicas: 1,
				PodOptions: test.PodOptions{
					ObjectMeta: metav1.ObjectMeta{Labels: podLabels},
					ResourceRequirements: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("100m"),
							corev1.ResourceMemory: resource.MustParse("128Mi"),
						},
					},
					TerminationGracePeriodSeconds: lo.ToPtr(int64(0)),
				},
			})
			env.ExpectCreated(dep)

			env.EventuallyExpectHealthyPodCount(labels.SelectorFromSet(dep.Spec.Selector.MatchLabels), 1)
			nodes := env.ExpectCreatedNodeCount("==", 1)
			vmName := env.ExpectParsedProviderID(nodes[0].Spec.ProviderID)

			// Confirm the production code did stamp both fleet-name and nodeclaim-name tags;
			// if it didn't, the GC scenario below would be testing the wrong thing.
			vm := env.GetVMByName(vmName)
			Expect(vm.Tags).To(HaveKey("karpenter.azure.com_fleet-name"),
				"Fleet-provisioned VM is missing fleet-name tag; executor.go regressed")
			Expect(vm.Tags).To(HaveKey("karpenter.azure.com_nodeclaim-name"),
				"Fleet-provisioned VM is missing nodeclaim-name tag; sharedstate.go regressed")

			// Simulate the "orphan" state. Either GC controller (instance.gc on ProviderID-mismatch
			// or fleetvmgc on tag-mismatch) is acceptable here: from the e2e perspective, the
			// orphan VM must eventually be reaped.
			env.ExpectRemoveVMTag(vmName, "karpenter.azure.com_nodeclaim-name")

			// Allow extra wall-clock time so fleetvmgc grace + reconcile interval can elapse.
			env.EventuallyExpectVMNotFound(vmName, 20*time.Minute)

			env.ExpectDeleted(dep)
		})
	})
})

// listFleets returns all Fleet resources in the node resource group.
func listFleets(env *azure.Environment) []*armcomputefleet.Fleet {
	cred := env.GetDefaultCredential()
	client, err := armcomputefleet.NewFleetsClient(env.SubscriptionID, cred, nil)
	Expect(err).ToNot(HaveOccurred())

	pager := client.NewListByResourceGroupPager(env.NodeResourceGroup, nil)
	var fleets []*armcomputefleet.Fleet
	for pager.More() {
		page, err := pager.NextPage(context.Background())
		Expect(err).ToNot(HaveOccurred())
		fleets = append(fleets, page.Value...)
	}
	return fleets
}

// fleetIDSet returns the set of Fleet resource IDs currently in the node RG.
// Used by tests that need to assert on a delta (new fleets created by THIS test)
// rather than an absolute count, since the node RG is shared with prior runs
// and may contain leaked fleets from aborted tests.
func fleetIDSet(env *azure.Environment) sets.Set[string] {
	out := sets.New[string]()
	for _, f := range listFleets(env) {
		if f != nil && f.ID != nil {
			out.Insert(lo.FromPtr(f.ID))
		}
	}
	return out
}
