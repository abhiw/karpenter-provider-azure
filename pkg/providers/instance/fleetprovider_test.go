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
	"fmt"
	"testing"

	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/scheduling"

	"github.com/Azure/karpenter-provider-azure/pkg/apis/v1beta1"
	"github.com/Azure/karpenter-provider-azure/pkg/consts"
	"github.com/Azure/karpenter-provider-azure/pkg/providers/allocationstrategy"
	"github.com/Azure/karpenter-provider-azure/pkg/providers/launchtemplate"
)

func TestFleetAdditionalLabels_MarksNodeUnmanagedForCCM(t *testing.T) {
	g := NewWithT(t)
	nodeClaim := &karpv1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{karpv1.CapacityTypeLabelKey: karpv1.CapacityTypeOnDemand}},
	}
	it := &cloudprovider.InstanceType{Name: "Standard_D2s_v5", Requirements: scheduling.NewRequirements()}

	got := fleetAdditionalLabels(nodeClaim, it)

	g.Expect(got).To(HaveKeyWithValue("kubernetes.azure.com/managed", "false"))
	g.Expect(got).To(HaveKeyWithValue(karpv1.CapacityTypeLabelKey, karpv1.CapacityTypeOnDemand))
}

func TestBuildFleetExtensions_OnlyBootstrapExtensions(t *testing.T) {
	for _, script := range []string{"", "echo bootstrap"} {
		t.Run(fmt.Sprintf("hasCSE=%t", script != ""), func(t *testing.T) {
			g := NewWithT(t)
			exts := buildFleetExtensions(&launchtemplate.Template{CustomScriptsCSE: script}, "eastus2euap", nil)
			if script == "" {
				g.Expect(exts).To(BeEmpty())
				return
			}
			g.Expect(exts).To(HaveLen(1))
			g.Expect(lo.FromPtr(exts[0].Name)).To(Equal(cseNameLinux))
			g.Expect(lo.FromPtr(exts[0].Properties.Type)).To(Equal("CustomScript"))
			g.Expect(exts[0].Properties.ProtectedSettings).To(Equal(&map[string]interface{}{"commandToExecute": script}))
		})
	}
}

func TestFleetIPConfigurationCount(t *testing.T) {
	for _, tc := range []struct {
		name, plugin, mode string
		maxPods            *int32
		expected           int32
	}{
		{"node subnet default", consts.NetworkPluginAzure, consts.NetworkPluginModeNone, nil, consts.DefaultNodeSubnetMaxPods},
		{"node subnet explicit", consts.NetworkPluginAzure, consts.NetworkPluginModeNone, lo.ToPtr(int32(42)), 42},
		{"overlay default", consts.NetworkPluginAzure, consts.NetworkPluginModeOverlay, nil, 1},
		{"overlay explicit", consts.NetworkPluginAzure, consts.NetworkPluginModeOverlay, lo.ToPtr(int32(42)), 1},
		{"kubenet", "kubenet", "", lo.ToPtr(int32(42)), 1},
		{"none", consts.NetworkPluginNone, "", nil, 1},
		{"invalid count reaches validation", consts.NetworkPluginAzure, "", lo.ToPtr(int32(0)), 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			nodeClass := &v1beta1.AKSNodeClass{Spec: v1beta1.AKSNodeClassSpec{MaxPods: tc.maxPods}}
			g.Expect(fleetIPConfigurationCount(nodeClass, tc.plugin, tc.mode)).To(Equal(tc.expected))
		})
	}
}

// --- Helper builders ---

// mkInstanceType builds a minimal InstanceType with given name and zone offerings.
func mkInstanceType(name string, zones ...string) *cloudprovider.InstanceType {
	offerings := cloudprovider.Offerings{}
	for _, z := range zones {
		offerings = append(offerings, &cloudprovider.Offering{
			Requirements: scheduling.NewRequirements(
				scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, z),
				scheduling.NewRequirement(karpv1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, karpv1.CapacityTypeOnDemand),
			),
			Price:     1.0,
			Available: true,
		})
	}
	return &cloudprovider.InstanceType{
		Name:      name,
		Offerings: offerings,
		Capacity: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("2"),
			corev1.ResourceMemory: resource.MustParse("8Gi"),
		},
		Requirements: scheduling.NewRequirements(
			scheduling.NewRequirement(corev1.LabelInstanceTypeStable, corev1.NodeSelectorOpIn, name),
		),
	}
}

// mkOffering builds an InstanceOffering (the allocation strategy output format) from an InstanceType.
func mkOffering(it *cloudprovider.InstanceType) allocationstrategy.InstanceOffering {
	return allocationstrategy.InstanceOffering{
		InstanceType: it,
		Offerings:    append(cloudprovider.Offerings{}, it.Offerings...),
	}
}

// --- Tests for extractCandidateInfo ---

// TestExtractCandidateInfo_SKUNames verifies that SKU names are extracted in order from candidates.
func TestExtractCandidateInfo_SKUNames(t *testing.T) {
	g := NewWithT(t)

	candidates := []allocationstrategy.InstanceOffering{
		mkOffering(mkInstanceType("Standard_D4s_v3", "westus-1")),
		mkOffering(mkInstanceType("Standard_D8s_v3", "westus-2")),
		mkOffering(mkInstanceType("Standard_D16s_v3", "westus-1", "westus-3")),
	}

	skuNames, _, _ := extractCandidateInfo(candidates)
	g.Expect(skuNames).To(Equal([]string{"Standard_D4s_v3", "Standard_D8s_v3", "Standard_D16s_v3"}))
}

// TestExtractCandidateInfo_ZonesAreSortedUnion verifies that zones are the sorted union across all
// offerings of all selected candidates — not just one candidate's zones.
func TestExtractCandidateInfo_ZonesAreSortedUnion(t *testing.T) {
	g := NewWithT(t)

	candidates := []allocationstrategy.InstanceOffering{
		mkOffering(mkInstanceType("A", "westus-3", "westus-1")),
		mkOffering(mkInstanceType("B", "westus-2")),
	}

	_, zones, _ := extractCandidateInfo(candidates)
	g.Expect(zones).To(Equal([]string{"westus-1", "westus-2", "westus-3"}))
}

// TestExtractCandidateInfo_InstanceTypeMap verifies each candidate gets an entry in the map keyed by name.
func TestExtractCandidateInfo_InstanceTypeMap(t *testing.T) {
	g := NewWithT(t)

	it1 := mkInstanceType("Standard_D4s_v3", "westus-1")
	it2 := mkInstanceType("Standard_D8s_v3", "westus-2")
	candidates := []allocationstrategy.InstanceOffering{
		mkOffering(it1),
		mkOffering(it2),
	}

	_, _, itMap := extractCandidateInfo(candidates)
	g.Expect(itMap).To(HaveLen(2))
	g.Expect(itMap["Standard_D4s_v3"]).To(Equal(it1))
	g.Expect(itMap["Standard_D8s_v3"]).To(Equal(it2))
}

// TestExtractCandidateInfo_EmptyZoneSkipped verifies that offerings with empty zone values
// don't produce empty-string entries in the zones slice.
func TestExtractCandidateInfo_EmptyZoneSkipped(t *testing.T) {
	g := NewWithT(t)

	// Create an instance type with one offering that has no zone label
	it := &cloudprovider.InstanceType{
		Name: "NoZone",
		Offerings: cloudprovider.Offerings{
			&cloudprovider.Offering{
				Requirements: scheduling.NewRequirements(
					scheduling.NewRequirement(karpv1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, karpv1.CapacityTypeOnDemand),
				),
				Price:     1.0,
				Available: true,
			},
		},
		Capacity: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("2"),
			corev1.ResourceMemory: resource.MustParse("8Gi"),
		},
		Requirements: scheduling.NewRequirements(
			scheduling.NewRequirement(corev1.LabelInstanceTypeStable, corev1.NodeSelectorOpIn, "NoZone"),
		),
	}
	candidates := []allocationstrategy.InstanceOffering{
		{InstanceType: it, Offerings: it.Offerings},
	}

	_, zones, _ := extractCandidateInfo(candidates)
	g.Expect(zones).To(BeEmpty())
}

// TestExtractCandidateInfo_DuplicateZonesDeduped verifies that if multiple candidates
// share the same zone, it appears only once in the output.
func TestExtractCandidateInfo_DuplicateZonesDeduped(t *testing.T) {
	g := NewWithT(t)

	candidates := []allocationstrategy.InstanceOffering{
		mkOffering(mkInstanceType("A", "westus-1", "westus-2")),
		mkOffering(mkInstanceType("B", "westus-1", "westus-2")),
		mkOffering(mkInstanceType("C", "westus-2")),
	}

	_, zones, _ := extractCandidateInfo(candidates)
	g.Expect(zones).To(Equal([]string{"westus-1", "westus-2"}))
}

// --- Tests for DefaultFleetProvider construction ---

// TestNewFleetProvider_MaxCandidateSKUsDefault verifies the constructor defaults to 10 when 0 is passed.
func TestNewFleetProvider_MaxCandidateSKUsDefault(t *testing.T) {
	g := NewWithT(t)

	p := NewFleetProvider(nil, nil, nil, nil, nil, nil, "", "", "", "", 0, nil, nil, nil)
	g.Expect(p.maxCandidateSKUs).To(Equal(defaultMaxCandidateSKUs))
}

// TestNewFleetProvider_MaxCandidateSKUsNegative verifies the constructor defaults when negative is passed.
func TestNewFleetProvider_MaxCandidateSKUsNegative(t *testing.T) {
	g := NewWithT(t)

	p := NewFleetProvider(nil, nil, nil, nil, nil, nil, "", "", "", "", -5, nil, nil, nil)
	g.Expect(p.maxCandidateSKUs).To(Equal(defaultMaxCandidateSKUs))
}

// TestNewFleetProvider_MaxCandidateSKUsCustom verifies a custom positive value is preserved.
func TestNewFleetProvider_MaxCandidateSKUsCustom(t *testing.T) {
	g := NewWithT(t)

	p := NewFleetProvider(nil, nil, nil, nil, nil, nil, "", "", "", "", 3, nil, nil, nil)
	g.Expect(p.maxCandidateSKUs).To(Equal(3))
}

// --- Tests for top-N SKU selection logic (tested via extractCandidateInfo + manual slice) ---

// TestTopNSKUs_LimitApplied verifies that when more candidates are available than maxCandidateSKUs,
// only the top N are used.
func TestTopNSKUs_LimitApplied(t *testing.T) {
	g := NewWithT(t)

	// Simulate 15 candidates, provider configured for max 10
	var candidates []allocationstrategy.InstanceOffering
	for i := 0; i < 15; i++ {
		candidates = append(candidates, mkOffering(mkInstanceType(
			fmt.Sprintf("Standard_D%ds_v3", i+1), "westus-1",
		)))
	}

	maxSKUs := 10
	topN := min(maxSKUs, len(candidates))
	selected := candidates[:topN]

	skuNames, _, _ := extractCandidateInfo(selected)
	g.Expect(skuNames).To(HaveLen(10))
	g.Expect(skuNames[0]).To(Equal("Standard_D1s_v3"))
	g.Expect(skuNames[9]).To(Equal("Standard_D10s_v3"))
}

// TestTopNSKUs_FewerThanMax verifies that when fewer candidates than max exist, all are included.
func TestTopNSKUs_FewerThanMax(t *testing.T) {
	g := NewWithT(t)

	candidates := []allocationstrategy.InstanceOffering{
		mkOffering(mkInstanceType("A", "westus-1")),
		mkOffering(mkInstanceType("B", "westus-2")),
		mkOffering(mkInstanceType("C", "westus-3")),
	}

	maxSKUs := 10
	topN := min(maxSKUs, len(candidates))
	selected := candidates[:topN]

	skuNames, _, _ := extractCandidateInfo(selected)
	g.Expect(skuNames).To(HaveLen(3))
}
