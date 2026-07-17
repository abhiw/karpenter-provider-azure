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
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

// BatchKeyFields contains all fields that determine batch grouping.
// Requests with identical BatchKeyFields land in the same Fleet.
// Used by BuildFleetBody to construct the Fleet PUT body.
type BatchKeyFields struct {
	NodePoolName        string
	CapacityType        string
	ImageID             string
	SubnetID            string
	SSHPublicKey        string
	AdminUsername       string
	CustomData          string
	OSDiskSizeGB        int
	OSDiskType          string
	EncryptionAtHost    bool
	DiskEncryptionSetID string
	NodeIdentities      string   // sorted, comma-joined
	NSG                 string
	CandidateSKUs       []string // sorted alphabetically before hashing
	Zones               []string // sorted alphabetically before hashing

	// Interconnect topology fields (see FleetVMProvisionRequest). Requests targeting
	// different interconnect placement MUST NOT share a Fleet, since a Fleet's
	// interconnectBlockProfile/networkProfile.interconnectGroupProfile can only carry
	// one configuration.
	InterconnectBlockID    string
	InterconnectGroupID    string
	InterconnectSubgroupID string
}

// DetermineBatchKey computes a deterministic grouping key for a FleetVMProvisionRequest.
// It builds the actual Fleet body with capacity=1 and hashes it, so the batch key
// is a direct reflection of the Fleet PUT request. Two requests that produce identical
// Fleet bodies (at capacity=1) will always batch together - no separate field list
// to maintain or risk drifting from the Fleet body.
//
// Per-VM fields (NodeClaimName) are not part of the Fleet body and do not affect the key.
//
// This is the batcher.DetermineBatchKey[FleetVMProvisionRequest] implementation.
func DetermineBatchKey(req *FleetVMProvisionRequest) (string, error) {
	if req == nil || req.NodeClaim == nil || req.NodeClass == nil || req.LaunchTemplate == nil {
		return "", fmt.Errorf("nil request, nodeclaim, nodeclass, or launch template")
	}

	fields := extractBatchKeyFieldsFromRequest(req)

	// Build the Fleet body with capacity=1. This is the canonical representation
	// of a single-VM Fleet request. Requests that produce the same body at capacity=1
	// are compatible and can share a Fleet at capacity=N.
	fleetBody := BuildFleetBody(
		fields,
		1, // capacity=1: constant across all requests
		req.Tags,
		nil, // spotMaxPrice: nil = default (-1)
		req.Location,
		req.LBBackendPools,
		req.InstanceTypes,
		false, // useSIG
		req.Extensions,
	)

	// The Fleet body does not include interconnect fields (they are injected via a
	// raw JSON patch on the request context, not expressed in the typed SDK struct).
	// Wrap the body with interconnect fields so they are included in the hash.
	hashInput := struct {
		Fleet                  interface{} `json:"fleet"`
		InterconnectBlockID    string      `json:"interconnectBlockID,omitempty"`
		InterconnectGroupID    string      `json:"interconnectGroupID,omitempty"`
		InterconnectSubgroupID string      `json:"interconnectSubgroupID,omitempty"`
	}{
		Fleet:                  fleetBody,
		InterconnectBlockID:    req.InterconnectBlockID,
		InterconnectGroupID:    req.InterconnectGroupID,
		InterconnectSubgroupID: req.InterconnectSubgroupID,
	}

	blob, err := json.Marshal(hashInput)
	if err != nil {
		return "", fmt.Errorf("marshal fleet body for batch key: %w", err)
	}

	sum := sha256.Sum256(blob)

	nodePoolName := req.NodeClaim.Labels[karpv1.NodePoolLabelKey]
	capacityType := req.CapacityType

	// Prefix with nodepool + capacityType so logs/metrics can tell batches apart at a glance,
	// mirroring the aksmachinesheaderbatch convention.
	return fmt.Sprintf("%s/%s/%x", nodePoolName, capacityType, sum[:8]), nil
}

// extractBatchKeyFieldsFromRequest builds BatchKeyFields from a FleetVMProvisionRequest.
// Used by DetermineBatchKey and by the executor (via extractBatchKeyFields).
func extractBatchKeyFieldsFromRequest(req *FleetVMProvisionRequest) BatchKeyFields {
	return BatchKeyFields{
		NodePoolName:  req.NodeClaim.Labels[karpv1.NodePoolLabelKey],
		CapacityType:  req.CapacityType,
		ImageID:       req.LaunchTemplate.ImageID,
		SubnetID:      req.LaunchTemplate.SubnetID,
		SSHPublicKey:  req.SSHPublicKey,
		AdminUsername: req.AdminUsername,
		CustomData:    req.LaunchTemplate.ScriptlessCustomData,
		OSDiskSizeGB:  int(req.LaunchTemplate.StorageProfileSizeGB),
		OSDiskType:    string(req.LaunchTemplate.StorageProfilePlacement),

		EncryptionAtHost:    req.NodeClass.GetEncryptionAtHost(),
		DiskEncryptionSetID: req.DiskEncryptionSetID,
		NodeIdentities:      joinSorted(req.NodeIdentities),
		NSG:                 req.NSG,
		CandidateSKUs:       sortedCopy(req.AcceptableSKUs),
		Zones:               sortedCopy(req.AcceptableZones),

		InterconnectBlockID:    req.InterconnectBlockID,
		InterconnectGroupID:    req.InterconnectGroupID,
		InterconnectSubgroupID: req.InterconnectSubgroupID,
	}
}

func sortedCopy(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

func joinSorted(in []string) string {
	return strings.Join(sortedCopy(in), ",")
}
