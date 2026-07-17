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
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/onsi/gomega"
)

// mockTransporter captures the final outgoing request body/URL/method and returns a
// fixed empty-JSON 200 response, standing in for the real HTTP transport at the
// bottom of the pipeline.
type mockTransporter struct {
	lastMethod string
	lastURL    string
	lastBody   []byte
}

func (m *mockTransporter) Do(req *http.Request) (*http.Response, error) {
	m.lastMethod = req.Method
	m.lastURL = req.URL.String()
	if req.Body != nil {
		b, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		m.lastBody = b
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(bytes.NewReader([]byte(`{}`))),
		Header:     http.Header{"Content-Type": []string{"application/json"}},
	}, nil
}

// newTestFleetPipeline builds a minimal azcore pipeline with only rawPropertiesPolicy
// and a capturing mock transport — mirroring how the policy is registered as a
// PerCallPolicy on the real Fleet client in azclient.go.
func newTestFleetPipeline(transporter *mockTransporter) runtime.Pipeline {
	return runtime.NewPipeline("fleettest", "v1", runtime.PipelineOptions{}, &policy.ClientOptions{
		PerCallPolicies: []policy.Policy{&rawPropertiesPolicy{}},
		Transport:       transporter,
	})
}

const fleetPutURL = "https://management.azure.com/subscriptions/sub/resourceGroups/rg/providers/Microsoft.AzureFleet/fleets/test-fleet?api-version=2024-11-01"

func doPatchedPut(t *testing.T, ctx context.Context, body string) *mockTransporter {
	t.Helper()
	transporter := &mockTransporter{}
	pipeline := newTestFleetPipeline(transporter)

	req, err := runtime.NewRequest(ctx, http.MethodPut, fleetPutURL)
	if err != nil {
		t.Fatalf("NewRequest failed: %v", err)
	}
	if err := req.SetBody(streaming.NopCloser(bytes.NewReader([]byte(body))), "application/json"); err != nil {
		t.Fatalf("SetBody failed: %v", err)
	}

	if _, err := pipeline.Do(req); err != nil {
		t.Fatalf("pipeline.Do failed: %v", err)
	}
	return transporter
}

// TestRawPropertiesPolicy_AllFieldsSet verifies that when all 3 interconnect fields
// are set via WithInterconnectPatch, the outgoing PUT body contains
// computeProfile.baseVirtualMachineProfile.interconnectBlockProfile.interconnectBlock.id,
// .networkProfile.interconnectGroupProfile.interconnectGroup.id, and .subgroups[0].id
// (spec E2E-003).
func TestRawPropertiesPolicy_AllFieldsSet(t *testing.T) {
	t.Parallel()
	g := gomega.NewWithT(t)

	ctx := WithInterconnectPatch(context.Background(), InterconnectPatch{
		InterconnectBlockID:    "/subscriptions/sub/.../interconnectBlocks/icb-1",
		InterconnectGroupID:    "/subscriptions/sub/.../interconnectGroups/icg-1",
		InterconnectSubgroupID: "/subscriptions/sub/.../interconnectGroups/icg-1/subgroups/sub-1",
	})

	transporter := doPatchedPut(t, ctx, `{"location":"eastus2","properties":{"vmSizesProfile":[{"name":"Standard_D2s_v3"}],"computeProfile":{"baseVirtualMachineProfile":{"networkProfile":{"networkApiVersion":"2020-11-01"}}}}}`)

	var doc map[string]any
	g.Expect(json.Unmarshal(transporter.lastBody, &doc)).To(gomega.Succeed())

	properties := doc["properties"].(map[string]any)
	baseVMProfile := properties["computeProfile"].(map[string]any)["baseVirtualMachineProfile"].(map[string]any)
	g.Expect(baseVMProfile["interconnectBlockProfile"].(map[string]any)["interconnectBlock"].(map[string]any)["id"]).
		To(gomega.Equal("/subscriptions/sub/.../interconnectBlocks/icb-1"))

	networkProfile := baseVMProfile["networkProfile"].(map[string]any)
	groupProfile := networkProfile["interconnectGroupProfile"].(map[string]any)
	g.Expect(groupProfile["interconnectGroup"].(map[string]any)["id"]).
		To(gomega.Equal("/subscriptions/sub/.../interconnectGroups/icg-1"))
	subgroups := groupProfile["subgroups"].([]any)
	g.Expect(subgroups).To(gomega.HaveLen(1))
	g.Expect(subgroups[0].(map[string]any)["id"]).
		To(gomega.Equal("/subscriptions/sub/.../interconnectGroups/icg-1/subgroups/sub-1"))

	// Pre-existing sibling fields must be preserved untouched (including the existing
	// networkProfile.networkApiVersion, since the patch merges into that same object).
	g.Expect(doc["location"]).To(gomega.Equal("eastus2"))
	g.Expect(properties["vmSizesProfile"]).ToNot(gomega.BeNil())
	g.Expect(networkProfile["networkApiVersion"]).To(gomega.Equal("2020-11-01"))
}

// TestRawPropertiesPolicy_NoPatchLeavesBodyUnchanged verifies that when no
// InterconnectPatch is set on the context, the request body reaching the transport
// is byte-identical to the original — confirming zero regression for existing Fleet
// NodePools that don't use interconnect placement (spec EC-001/AC-004).
func TestRawPropertiesPolicy_NoPatchLeavesBodyUnchanged(t *testing.T) {
	t.Parallel()
	g := gomega.NewWithT(t)

	original := `{"location":"eastus2","properties":{"vmSizesProfile":[{"name":"Standard_D2s_v3"}]}}`
	transporter := doPatchedPut(t, context.Background(), original)

	var got, want map[string]any
	g.Expect(json.Unmarshal(transporter.lastBody, &got)).To(gomega.Succeed())
	g.Expect(json.Unmarshal([]byte(original), &want)).To(gomega.Succeed())
	g.Expect(got).To(gomega.Equal(want))
}

// TestRawPropertiesPolicy_EmptyPatchLeavesBodyUnchanged verifies that a patch set
// on the context but with all fields empty (the zero value) is treated the same as
// no patch at all — the body is left unmodified.
func TestRawPropertiesPolicy_EmptyPatchLeavesBodyUnchanged(t *testing.T) {
	t.Parallel()
	g := gomega.NewWithT(t)

	ctx := WithInterconnectPatch(context.Background(), InterconnectPatch{})
	original := `{"location":"eastus2","properties":{}}`
	transporter := doPatchedPut(t, ctx, original)

	var got, want map[string]any
	g.Expect(json.Unmarshal(transporter.lastBody, &got)).To(gomega.Succeed())
	g.Expect(json.Unmarshal([]byte(original), &want)).To(gomega.Succeed())
	g.Expect(got).To(gomega.Equal(want))
}

// TestRawPropertiesPolicy_SubgroupWithoutGroup verifies that InterconnectSubgroupID
// can be set independently of InterconnectGroupID: the patch emits subgroups[0].id
// with no interconnectGroup key present (spec EC-002/AC-003).
func TestRawPropertiesPolicy_SubgroupWithoutGroup(t *testing.T) {
	t.Parallel()
	g := gomega.NewWithT(t)

	ctx := WithInterconnectPatch(context.Background(), InterconnectPatch{
		InterconnectSubgroupID: "/subscriptions/sub/.../interconnectGroups/icg-1/subgroups/sub-1",
	})

	transporter := doPatchedPut(t, ctx, `{"properties":{"computeProfile":{"baseVirtualMachineProfile":{}}}}`)

	var doc map[string]any
	g.Expect(json.Unmarshal(transporter.lastBody, &doc)).To(gomega.Succeed())

	properties := doc["properties"].(map[string]any)
	baseVMProfile := properties["computeProfile"].(map[string]any)["baseVirtualMachineProfile"].(map[string]any)
	groupProfile := baseVMProfile["networkProfile"].(map[string]any)["interconnectGroupProfile"].(map[string]any)
	g.Expect(groupProfile).ToNot(gomega.HaveKey("interconnectGroup"))
	subgroups := groupProfile["subgroups"].([]any)
	g.Expect(subgroups).To(gomega.HaveLen(1))
	g.Expect(subgroups[0].(map[string]any)["id"]).
		To(gomega.Equal("/subscriptions/sub/.../interconnectGroups/icg-1/subgroups/sub-1"))
}

// TestRawPropertiesPolicy_NonFleetURLIgnored verifies that a PUT to a URL that is
// not a Microsoft.AzureFleet/fleets resource is never patched, even if a patch is
// present on the context — the policy must only touch Fleet PUT requests.
func TestRawPropertiesPolicy_NonFleetURLIgnored(t *testing.T) {
	t.Parallel()
	g := gomega.NewWithT(t)

	ctx := WithInterconnectPatch(context.Background(), InterconnectPatch{
		InterconnectBlockID: "/subscriptions/sub/.../interconnectBlocks/icb-1",
	})

	transporter := &mockTransporter{}
	pipeline := newTestFleetPipeline(transporter)
	req, err := runtime.NewRequest(ctx, http.MethodPut,
		"https://management.azure.com/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/vm1?api-version=2024-11-01")
	if err != nil {
		t.Fatalf("NewRequest failed: %v", err)
	}
	original := `{"location":"eastus2"}`
	if err := req.SetBody(streaming.NopCloser(bytes.NewReader([]byte(original))), "application/json"); err != nil {
		t.Fatalf("SetBody failed: %v", err)
	}
	if _, err := pipeline.Do(req); err != nil {
		t.Fatalf("pipeline.Do failed: %v", err)
	}

	var got, want map[string]any
	g.Expect(json.Unmarshal(transporter.lastBody, &got)).To(gomega.Succeed())
	g.Expect(json.Unmarshal([]byte(original), &want)).To(gomega.Succeed())
	g.Expect(got).To(gomega.Equal(want))
}

// TestIsFleetPutURL locks in the URL-matching contract used to scope the policy to
// Fleet PUT requests only, including ARM's case-insensitivity for provider segments.
func TestIsFleetPutURL(t *testing.T) {
	t.Parallel()
	g := gomega.NewWithT(t)

	g.Expect(isFleetPutURL("/subscriptions/sub/resourceGroups/rg/providers/Microsoft.AzureFleet/fleets/test-fleet")).To(gomega.BeTrue())
	g.Expect(isFleetPutURL("/subscriptions/sub/resourceGroups/rg/providers/microsoft.azurefleet/fleets/test-fleet")).To(gomega.BeTrue())
	g.Expect(isFleetPutURL("/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/vm1")).To(gomega.BeFalse())
}
