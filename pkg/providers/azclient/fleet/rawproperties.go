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
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
)

// InterconnectPatch carries the raw ARM properties that the vendored
// armcomputefleet/v2 SDK (pinned at v2.0.0-beta.2, the newest version available
// upstream) does not expose as typed fields:
// properties.computeProfile.baseVirtualMachineProfile.interconnectBlockProfile and
// properties.computeProfile.baseVirtualMachineProfile.networkProfile.interconnectGroupProfile.
// It is threaded through a context.Context set by the executor immediately before
// calling FleetAPI.BeginCreateOrUpdate, and merged into the outgoing PUT request
// body by rawPropertiesPolicy.
type InterconnectPatch struct {
	InterconnectBlockID    string
	InterconnectGroupID    string
	InterconnectSubgroupID string
}

// IsEmpty reports whether the patch has no fields set. When true, no raw-body
// patch is applied and the typed BuildFleetBody output is sent unmodified —
// this is what keeps existing Fleet NodePools (that don't use interconnect
// placement) byte-identical to pre-change behavior.
func (p InterconnectPatch) IsEmpty() bool {
	return p.InterconnectBlockID == "" && p.InterconnectGroupID == "" && p.InterconnectSubgroupID == ""
}

type interconnectPatchContextKey struct{}

// WithInterconnectPatch returns a context carrying the interconnect patch to apply
// to the next Fleet PUT request sent through the pipeline that owns
// rawPropertiesPolicy. Callers MUST set this immediately before invoking
// FleetAPI.BeginCreateOrUpdate for the same request.
func WithInterconnectPatch(ctx context.Context, patch InterconnectPatch) context.Context {
	return context.WithValue(ctx, interconnectPatchContextKey{}, patch)
}

// interconnectPatchFromContext retrieves the patch set by WithInterconnectPatch, if any.
func interconnectPatchFromContext(ctx context.Context) (InterconnectPatch, bool) {
	patch, ok := ctx.Value(interconnectPatchContextKey{}).(InterconnectPatch)
	return patch, ok
}

var _ policy.Policy = &rawPropertiesPolicy{}

// NewRawPropertiesPolicy returns a policy.Policy that injects the interconnect
// ARM properties (see InterconnectPatch) into outgoing Fleet PUT requests. It
// MUST be registered as a PerCallPolicy on the armcomputefleet FleetsClient's
// pipeline (see azclient.go) for WithInterconnectPatch to have any effect.
func NewRawPropertiesPolicy() policy.Policy {
	return &rawPropertiesPolicy{}
}

// rawPropertiesPolicy is an Azure SDK per-call policy that injects ARM properties
// not exposed by the vendored armcomputefleet/v2 SDK (baseVirtualMachineProfile.interconnectBlockProfile,
// baseVirtualMachineProfile.networkProfile.interconnectGroupProfile) into the outgoing Fleet
// PUT request body. It is a no-op for any request that isn't a Fleet PUT, and a no-op when no
// InterconnectPatch was set on the request's context (or the patch is empty).
//
// This mechanism exists because armcomputefleet/v2 v2.0.0-beta.2 has zero
// "Interconnect" types (verified via source grep of the vendored module) and no
// newer version exists upstream, so the typed BuildFleetBody path cannot express
// these fields.
type rawPropertiesPolicy struct{}

func (p *rawPropertiesPolicy) Do(req *policy.Request) (*http.Response, error) {
	if req.Raw().Method != http.MethodPut || !isFleetPutURL(req.Raw().URL.Path) {
		return req.Next()
	}

	patch, ok := interconnectPatchFromContext(req.Raw().Context())
	if !ok || patch.IsEmpty() {
		return req.Next()
	}

	if err := applyInterconnectPatch(req, patch); err != nil {
		return nil, err
	}

	return req.Next()
}

// isFleetPutURL reports whether the given request path targets a
// Microsoft.AzureFleet/fleets/{name} resource. ARM resource-provider path
// segments are case-insensitive, so the comparison is case-insensitive too.
func isFleetPutURL(urlPath string) bool {
	return strings.Contains(strings.ToLower(urlPath), "/providers/microsoft.azurefleet/fleets/")
}

// applyInterconnectPatch merges the interconnect fields into the request body's
// properties object and replaces the request body in place.
func applyInterconnectPatch(req *policy.Request, patch InterconnectPatch) error {
	body := req.Body()
	if body == nil {
		return nil
	}

	raw, err := io.ReadAll(body)
	if err != nil {
		return fmt.Errorf("rawPropertiesPolicy: read fleet PUT body: %w", err)
	}

	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return fmt.Errorf("rawPropertiesPolicy: unmarshal fleet PUT body: %w", err)
	}

	properties, _ := doc["properties"].(map[string]any)
	if properties == nil {
		properties = map[string]any{}
	}

	// Both interconnect fields live under properties.computeProfile.baseVirtualMachineProfile
	// in the ARM schema — NOT at the top level of properties. interconnectGroupProfile in
	// particular must merge into the existing baseVirtualMachineProfile.networkProfile object
	// (the one already carrying networkInterfaceConfigurations), not a separate top-level one.
	computeProfile, _ := properties["computeProfile"].(map[string]any)
	if computeProfile == nil {
		computeProfile = map[string]any{}
	}
	baseVMProfile, _ := computeProfile["baseVirtualMachineProfile"].(map[string]any)
	if baseVMProfile == nil {
		baseVMProfile = map[string]any{}
	}

	if patch.InterconnectBlockID != "" {
		baseVMProfile["interconnectBlockProfile"] = map[string]any{
			"interconnectBlock": map[string]any{"id": patch.InterconnectBlockID},
		}
	}

	if patch.InterconnectGroupID != "" || patch.InterconnectSubgroupID != "" {
		groupProfile := map[string]any{}
		if patch.InterconnectGroupID != "" {
			groupProfile["interconnectGroup"] = map[string]any{"id": patch.InterconnectGroupID}
		}
		if patch.InterconnectSubgroupID != "" {
			groupProfile["subgroups"] = []map[string]any{
				{"id": patch.InterconnectSubgroupID},
			}
		}

		networkProfile, _ := baseVMProfile["networkProfile"].(map[string]any)
		if networkProfile == nil {
			networkProfile = map[string]any{}
		}
		networkProfile["interconnectGroupProfile"] = groupProfile
		baseVMProfile["networkProfile"] = networkProfile
	}

	computeProfile["baseVirtualMachineProfile"] = baseVMProfile
	properties["computeProfile"] = computeProfile
	doc["properties"] = properties

	patched, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("rawPropertiesPolicy: marshal patched fleet PUT body: %w", err)
	}

	return req.SetBody(streaming.NopCloser(bytes.NewReader(patched)), "application/json")
}
