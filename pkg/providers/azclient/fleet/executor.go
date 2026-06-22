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
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"github.com/go-logr/logr"
	"github.com/samber/lo"
	"sigs.k8s.io/controller-runtime/pkg/log"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"

	"github.com/Azure/karpenter-provider-azure/pkg/providers/instance/offerings"
	"github.com/Azure/karpenter-provider-azure/pkg/utils/batcher"
)

const (
	// FleetNameTagKey is applied to all Fleet VMs so the executor can discover them after LRO.
	FleetNameTagKey = "karpenter.azure.com_fleet-name"

	// MaxFleetCapacity is the maximum number of VMs per single Fleet resource.
	// Azure Fleet API supports up to 10,000 VMs per fleet, but we use a conservative
	// limit to avoid excessively large ARM operations. This is hardcoded because it
	// is an Azure platform constraint, not a user-tunable setting.
	MaxFleetCapacity = 1000

	// inflightCooldown is how long the inflight entry persists after a successful
	// Fleet LRO. During this window, new batches for the same batch key receive
	// ErrFleetCoalesced. This prevents duplicate VM provisioning from phantom
	// NodeClaims the provisioner creates before nodes register. Kept short (15s)
	// because kubelet registers within seconds of Fleet LRO completing.
	inflightCooldown = 10 * time.Second
)

// inflightGroup tracks all in-progress Fleet LROs for a single batch key.
// Multiple concurrent Fleet creations can coexist for the same key when new
// NodeClaims arrive while a prior Fleet is inflight — only re-triggered
// duplicates (same NodeClaimName) coalesce; genuinely new claims proceed.
type inflightGroup struct {
	mu      sync.Mutex
	entries []*inflightEntry
}

// inflightEntry tracks a single in-progress Fleet LRO within an inflightGroup.
type inflightEntry struct {
	done  chan struct{}       // closed when the LRO + assignment completes
	err   error              // non-nil if the Fleet LRO failed
	names map[string]struct{} // NodeClaim names served by this Fleet
}

// executor sends batches to the Azure Fleet API.
// It transforms a pending batch into a Fleet CreateOrUpdate call, waits for
// the LRO, runs VM assignment, and distributes results back to each request.
//
// Inflight coalescing: if a second batch fires for the same batch key while
// the first is still running (due to provisioner re-triggers during the LRO),
// the second batch waits for the first to complete and receives a retryable
// error instead of creating a duplicate Fleet.
//
// Batch splitting: when a single batch exceeds MaxFleetCapacity, it is split
// into parallel sub-batches, each creating its own Fleet resource.
type executor struct {
	fleetClient   FleetAPI
	vmClient      VMAPI
	errorHandler  *offerings.FleetErrorHandler
	clusterName   string
	resourceGroup string
	location      string

	// maxFleetCapacity is the max VMs per Fleet resource; batches larger than
	// this are split. Defaults to MaxFleetCapacity. Exposed as a field only for
	// unit test overrides.
	maxFleetCapacity int

	inflight sync.Map // batchKey → *inflightGroup
}

func newExecutor(
	fleetClient FleetAPI,
	vmClient VMAPI,
	errorHandler *offerings.FleetErrorHandler,
	clusterName,
	resourceGroup,
	location string,
) *executor {
	return &executor{
		fleetClient:      fleetClient,
		vmClient:         vmClient,
		errorHandler:     errorHandler,
		clusterName:      clusterName,
		resourceGroup:    resourceGroup,
		location:         location,
		maxFleetCapacity: MaxFleetCapacity,
	}
}

// executeBatch is the batcher.ExecuteBatch[FleetVMProvisionRequest, FleetBatchResponse] implementation.
// It orchestrates: fleet name → body → PUT → LRO poll → VM list → shared state → distribute responses.
//
// Per-name inflight coalescing: the provisioner re-triggers every ~10s while
// pods remain Pending, creating duplicate NodeClaims for already-inflight pods.
// Instead of blocking the entire batch when ANY inflight exists for the same key,
// we split the incoming batch into:
//   - duplicates: NodeClaim names already tracked by an inflight entry → wait + ErrFleetCoalesced
//   - newRequests: genuinely new NodeClaim names → proceed with a fresh Fleet
//
// This ensures new pods arriving during an LRO get VMs without waiting for the
// prior batch to complete.
//
// Batch splitting: when a single batch exceeds maxFleetCapacity, it is split
// into parallel sub-batches, each creating its own Fleet resource.
//
// Examples:
//
//	10 pods at t=0, 15 pods at t=10 (10 re-triggers + 5 new):
//	  t=0s:  batch (10 reqs) → registers names {nc1..nc10} → 1 Fleet PUT
//	  t=10s: batch (15 reqs) → split: 10 duplicates coalesce, 5 new proceed → new Fleet PUT
//	  t=90s: both LROs complete → all 15 VMs register
//
//	50,000 pods, MaxFleetCapacity=1000:
//	  t=0s:  batch (50,000 reqs) → doFleetCreate splits into 50 sub-batches
//	         → 50 parallel Fleet PUTs, each with capacity=1000
//	  t=10s: re-triggers coalesce per-name; any genuinely new claims get a fresh Fleet
func (e *executor) executeBatch(ctx context.Context, batch *batcher.Batch[FleetVMProvisionRequest, FleetBatchResponse]) {
	logger := log.FromContext(ctx).WithValues("batchKey", batch.Key, "batchSize", len(batch.Requests))

	// --- Per-name inflight coalescing ---
	group := e.getOrCreateGroup(batch.Key)
	duplicates, newRequests := e.splitByInflightNames(group, batch.Requests)

	// Handle duplicates: wait for their respective inflight entries, then return retryable error.
	if len(duplicates) > 0 {
		logger.Info("coalescing duplicate NodeClaims with in-flight Fleet LROs",
			"duplicateCount", len(duplicates), "newCount", len(newRequests))
		go e.waitAndCoalesceDuplicates(ctx, duplicates, batch.Key)
	}

	// If no genuinely new requests, we're done.
	if len(newRequests) == 0 {
		return
	}

	// Register a new inflight entry for the new requests.
	names := make(map[string]struct{}, len(newRequests))
	for _, req := range newRequests {
		names[req.Payload.NodeClaimName] = struct{}{}
	}
	entry := &inflightEntry{
		done:  make(chan struct{}),
		names: names,
	}
	group.mu.Lock()
	group.entries = append(group.entries, entry)
	group.mu.Unlock()

	// Signal completion when done. Apply cooldown to prevent phantom re-triggers.
	defer func() {
		close(entry.done)
		if entry.err != nil {
			e.removeEntry(group, entry)
		} else {
			go func() {
				time.Sleep(inflightCooldown)
				e.removeEntry(group, entry)
			}()
		}
	}()

	// --- Normal Fleet creation path (only for genuinely new requests) ---
	newBatch := &batcher.Batch[FleetVMProvisionRequest, FleetBatchResponse]{
		ID:       batch.ID,
		Key:      batch.Key,
		Requests: newRequests,
	}
	logger.Info("proceeding with new Fleet creation", "newRequestCount", len(newRequests))
	_, _, err := e.doFleetCreate(ctx, newBatch, logger)
	if err != nil {
		entry.err = err
	}
}

// getOrCreateGroup returns the inflightGroup for a batch key, creating one if needed.
func (e *executor) getOrCreateGroup(batchKey string) *inflightGroup {
	val, _ := e.inflight.LoadOrStore(batchKey, &inflightGroup{})
	return val.(*inflightGroup)
}

// splitByInflightNames partitions requests into duplicates (name already inflight)
// and new requests (name not seen in any active entry).
func (e *executor) splitByInflightNames(
	group *inflightGroup,
	requests []*batcher.BatchedRequest[FleetVMProvisionRequest, FleetBatchResponse],
) (
	duplicates []*duplicateRequest,
	newRequests []*batcher.BatchedRequest[FleetVMProvisionRequest, FleetBatchResponse],
) {
	group.mu.Lock()
	defer group.mu.Unlock()

	for _, req := range requests {
		name := req.Payload.NodeClaimName
		matched := false
		for _, entry := range group.entries {
			if _, exists := entry.names[name]; exists {
				duplicates = append(duplicates, &duplicateRequest{
					request: req,
					entry:   entry,
				})
				matched = true
				break
			}
		}
		if !matched {
			newRequests = append(newRequests, req)
		}
	}
	return duplicates, newRequests
}

// duplicateRequest pairs a batched request with the inflight entry it matched.
type duplicateRequest struct {
	request *batcher.BatchedRequest[FleetVMProvisionRequest, FleetBatchResponse]
	entry   *inflightEntry
}

// waitAndCoalesceDuplicates waits for each duplicate's inflight entry to complete,
// then sends ErrFleetCoalesced (or the inflight error) to the caller.
func (e *executor) waitAndCoalesceDuplicates(ctx context.Context, duplicates []*duplicateRequest, batchKey string) {
	for _, dup := range duplicates {
		select {
		case <-dup.entry.done:
		case <-ctx.Done():
			dup.request.ResponseChan <- &batcher.Response[FleetBatchResponse]{
				Payload: FleetBatchResponse{Error: fmt.Errorf("context canceled while waiting for in-flight fleet: %w", ctx.Err())},
			}
			continue
		}
		if dup.entry.err != nil {
			dup.request.ResponseChan <- &batcher.Response[FleetBatchResponse]{
				Payload: FleetBatchResponse{Error: fmt.Errorf("coalesced fleet LRO failed: %w", dup.entry.err)},
			}
		} else {
			dup.request.ResponseChan <- &batcher.Response[FleetBatchResponse]{
				Payload: FleetBatchResponse{Error: fmt.Errorf("batch key %s: %w", batchKey, ErrFleetCoalesced)},
			}
		}
	}
}

// removeEntry removes a completed entry from the group. If the group is empty,
// cleans up the sync.Map entry.
func (e *executor) removeEntry(group *inflightGroup, entry *inflightEntry) {
	group.mu.Lock()
	for i, ent := range group.entries {
		if ent == entry {
			group.entries = append(group.entries[:i], group.entries[i+1:]...)
			break
		}
	}
	empty := len(group.entries) == 0
	group.mu.Unlock()

	// If no more entries for this key, remove the group from the map.
	// Note: there's a tiny race where a new entry could be added between
	// the check and the delete, but LoadOrStore in getOrCreateGroup handles
	// this safely — worst case is a redundant empty group object.
	if empty {
		e.inflight.Range(func(key, value any) bool {
			if value == group {
				e.inflight.Delete(key)
				return false
			}
			return true
		})
	}
}


// doFleetCreate performs the actual Fleet PUT → LRO → VM list → assignment → distribute flow.
// Returns the number of VMs created and requested, plus an error if the Fleet LRO
// or VM listing failed. The caller uses vmsCreated vs vmsRequested to decide
// whether to keep the inflight cooldown (fully fulfilled) or clear immediately
// (under-provisioned, so new batches for unfulfilled NodeClaims can proceed).
//
// If the batch exceeds maxFleetCapacity, it is split into parallel sub-batches,
// each creating its own Fleet resource with up to maxFleetCapacity VMs.
func (e *executor) doFleetCreate(ctx context.Context, batch *batcher.Batch[FleetVMProvisionRequest, FleetBatchResponse], logger logr.Logger) (vmsCreated, vmsRequested int, err error) {
	if len(batch.Requests) > e.maxFleetCapacity {
		return e.doSplitFleetCreate(ctx, batch, logger)
	}
	return e.doSingleFleetCreate(ctx, batch, logger)
}

// doSplitFleetCreate splits a large batch into sub-batches of maxFleetCapacity
// and creates each in parallel with its own Fleet resource.
func (e *executor) doSplitFleetCreate(ctx context.Context, batch *batcher.Batch[FleetVMProvisionRequest, FleetBatchResponse], logger logr.Logger) (totalCreated, totalRequested int, err error) {
	chunks := splitBatchRequests(batch.Requests, e.maxFleetCapacity)
	logger.Info("splitting large batch into sub-batches", "totalRequests", len(batch.Requests), "subBatches", len(chunks), "maxFleetCapacity", e.maxFleetCapacity)

	totalRequested = len(batch.Requests)

	type subResult struct {
		created   int
		requested int
		err       error
	}

	var wg sync.WaitGroup
	results := make([]subResult, len(chunks))

	for i, chunk := range chunks {
		wg.Add(1)
		go func(idx int, requests []*batcher.BatchedRequest[FleetVMProvisionRequest, FleetBatchResponse]) {
			defer wg.Done()
			subBatch := &batcher.Batch[FleetVMProvisionRequest, FleetBatchResponse]{
				ID:       fmt.Sprintf("%s-sub%d", batch.ID, idx),
				Key:      batch.Key,
				Requests: requests,
			}
			subLogger := logger.WithValues("subBatch", idx, "subBatchSize", len(requests))
			c, r, e := e.doSingleFleetCreate(ctx, subBatch, subLogger)
			results[idx] = subResult{created: c, requested: r, err: e}
		}(i, chunk)
	}

	wg.Wait()

	// Aggregate results across sub-batches.
	var combined []error
	for _, r := range results {
		totalCreated += r.created
		if r.err != nil {
			combined = append(combined, r.err)
		}
	}
	if len(combined) > 0 {
		return totalCreated, totalRequested, fmt.Errorf("fleet sub-batch errors: %v", combined)
	}
	return totalCreated, totalRequested, nil
}

// splitBatchRequests splits a slice of requests into chunks of at most chunkSize.
func splitBatchRequests[Req, Resp any](requests []*batcher.BatchedRequest[Req, Resp], chunkSize int) [][]*batcher.BatchedRequest[Req, Resp] {
	var chunks [][]*batcher.BatchedRequest[Req, Resp]
	for i := 0; i < len(requests); i += chunkSize {
		end := i + chunkSize
		if end > len(requests) {
			end = len(requests)
		}
		chunks = append(chunks, requests[i:end])
	}
	return chunks
}

// doSingleFleetCreate creates a single Fleet resource for the batch requests.
// Returns (vmsCreated, vmsRequested, error) for fulfillment tracking.
func (e *executor) doSingleFleetCreate(ctx context.Context, batch *batcher.Batch[FleetVMProvisionRequest, FleetBatchResponse], logger logr.Logger) (int, int, error) {
	requested := len(batch.Requests)

	// 1. Compute deterministic fleet name from batch key.
	name := fleetName(e.clusterName, batch.Key)
	logger = logger.WithValues("fleetName", name)

	// 2. Collect merged instance types and build assignment requests.
	mergedInstanceTypes := make(map[string]*cloudprovider.InstanceType)
	requests := make([]*VMAssignmentRequest, 0, len(batch.Requests))
	var representative *FleetVMProvisionRequest

	for _, br := range batch.Requests {
		req := &br.Payload
		if representative == nil {
			representative = req
		}
		for k, v := range req.InstanceTypes {
			mergedInstanceTypes[k] = v
		}
		requests = append(requests, &VMAssignmentRequest{
			NodeClaimName:   req.NodeClaimName,
			AcceptableSKUs:  req.AcceptableSKUs,
			AcceptableZones: req.AcceptableZones,
			InstanceTypes:   req.InstanceTypes,
		})
	}

	// 3. Build fleet body from the representative request (all requests in same batch
	//    share the same template/image/subnet per batch key guarantee).
	//    Inject fleet-name tag so we can discover the VMs after LRO.
	fields := extractBatchKeyFields(representative)
	fleetTags := make(map[string]*string, len(representative.Tags)+1)
	for k, v := range representative.Tags {
		fleetTags[k] = v
	}
	fleetTags[FleetNameTagKey] = lo.ToPtr(name)

	fleetBody := BuildFleetBody(
		fields,
		int32(len(batch.Requests)),
		fleetTags,
		nil, // spotMaxPrice: nil → default -1 (up to on-demand price)
		e.location,
		representative.LBBackendPools,
		mergedInstanceTypes,
		false, // useSIG: not used in POC
		representative.Extensions,
	)

	// 4. Call Fleet API BeginCreateOrUpdate.
	logger.Info("submitting fleet create-or-update")
	if v := logger.V(1); v.Enabled() {
		if data, mErr := json.Marshal(fleetBody); mErr == nil {
			v.Info("fleet request body", "fleetName", name, "json", string(data))
		} else {
			v.Info("fleet request body marshal failed", "error", mErr.Error())
		}
	}
	poller, err := e.fleetClient.BeginCreateOrUpdate(ctx, e.resourceGroup, name, *fleetBody, nil)
	if err != nil {
		logger.Error(err, "fleet BeginCreateOrUpdate failed")
		fleetErr := fmt.Errorf("fleet create: %w", err)
		e.distributeError(batch, fleetErr)
		return 0, requested, fleetErr
	}

	// 5. Poll LRO to completion.
	_, err = poller.PollUntilDone(ctx, nil)
	if err != nil {
		logger.Error(err, "fleet LRO poll failed")
		lroErr := fmt.Errorf("fleet LRO: %w", err)
		e.distributeError(batch, lroErr)
		return 0, requested, lroErr
	}
	logger.Info("fleet LRO completed")

	// 6. List VMs created by this Fleet (identified by fleet-name tag).
	vms, err := e.listFleetVMs(ctx, name)
	if err != nil {
		logger.Error(err, "failed to list fleet VMs")
		listErr := fmt.Errorf("list fleet VMs: %w", err)
		e.distributeError(batch, listErr)
		return 0, requested, listErr
	}
	logger.Info("listed fleet VMs", "count", len(vms))

	sharedState := NewFleetSharedState(
		requests,
		mergedInstanceTypes,
		e.vmClient,
		name,
		e.resourceGroup,
	)
	sharedState.SetVMs(vms)

	// Run assignment only (fast, in-memory) — determines which VM goes to which NodeClaim.
	sharedState.runAssignment(ctx)

	// 7. Distribute shared state to all requests immediately, so promises get
	// providerIDs without waiting for VM tagging (which is slow, ~30s per VM).
	e.distributeSharedState(batch, sharedState)

	// 8. Tag VMs and delete surplus in the background. Tagging is best-effort
	// housekeeping for Fleet VM GC — not on the critical path for node registration.
	go sharedState.runTaggingAndCleanup(ctx)

	return len(vms), requested, nil
}

// distributeError sends an error to all requests in the batch.
func (e *executor) distributeError(batch *batcher.Batch[FleetVMProvisionRequest, FleetBatchResponse], err error) {
	for _, req := range batch.Requests {
		req.ResponseChan <- &batcher.Response[FleetBatchResponse]{
			Payload: FleetBatchResponse{Error: err},
		}
	}
}

// distributeSharedState sends the shared state to all requests in the batch.
func (e *executor) distributeSharedState(batch *batcher.Batch[FleetVMProvisionRequest, FleetBatchResponse], state *FleetSharedState) {
	for _, req := range batch.Requests {
		req.ResponseChan <- &batcher.Response[FleetBatchResponse]{
			Payload: FleetBatchResponse{SharedState: state},
		}
	}
}

// fleetName returns a unique fleet name: "fleet-{clusterName}-{hash8}-{rand4}"
// Each invocation produces a distinct name because Launch-mode Fleets are immutable
// (cannot be updated after creation). The random suffix ensures no 409 conflicts
// when the same batch key fires multiple times.
// batchKey format: "<nodepool>/<capacityType>/<hash16>"
func fleetName(clusterName, batchKey string) string {
	// Extract last segment (the 16-char hex hash), take first 8 chars.
	lastSlash := strings.LastIndex(batchKey, "/")
	hash := batchKey[lastSlash+1:]
	if len(hash) > 8 {
		hash = hash[:8]
	}
	// Append 4 random hex chars to make the name unique per invocation.
	var randBytes [2]byte
	_, _ = rand.Read(randBytes[:])
	suffix := hex.EncodeToString(randBytes[:])
	return fmt.Sprintf("fleet-%s-%s-%s", clusterName, hash, suffix)
}

// extractBatchKeyFields builds the BatchKeyFields from a FleetVMProvisionRequest.
// Used by the executor to pass to BuildFleetBody.
func extractBatchKeyFields(req *FleetVMProvisionRequest) BatchKeyFields {
	return BatchKeyFields{
		NodePoolName:        req.NodeClaim.Labels[karpv1.NodePoolLabelKey],
		CapacityType:        req.CapacityType,
		ImageID:             req.LaunchTemplate.ImageID,
		SubnetID:            req.LaunchTemplate.SubnetID,
		SSHPublicKey:        req.SSHPublicKey,
		AdminUsername:       req.AdminUsername,
		CustomData:          req.LaunchTemplate.ScriptlessCustomData,
		OSDiskSizeGB:        int(req.LaunchTemplate.StorageProfileSizeGB),
		OSDiskType:          string(req.LaunchTemplate.StorageProfilePlacement),
		EncryptionAtHost:    req.NodeClass.GetEncryptionAtHost(),
		DiskEncryptionSetID: req.DiskEncryptionSetID,
		NodeIdentities:      joinSorted(req.NodeIdentities),
		NSG:                 req.NSG,
		CandidateSKUs:       sortedCopy(req.AcceptableSKUs),
		Zones:               sortedCopy(req.AcceptableZones),
	}
}

// listFleetVMs lists all VMs in the resource group that carry the fleet-name tag
// matching the given name. This discovers VMs created by the Fleet VMSS Flex.
func (e *executor) listFleetVMs(ctx context.Context, name string) ([]*armcompute.VirtualMachine, error) {
	pager := e.vmClient.NewListPager(e.resourceGroup, nil)
	var vms []*armcompute.VirtualMachine
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("listing VMs page: %w", err)
		}
		for _, vm := range page.Value {
			if vm == nil || vm.Tags == nil {
				continue
			}
			if tagVal, ok := vm.Tags[FleetNameTagKey]; ok && tagVal != nil && *tagVal == name {
				vms = append(vms, vm)
			}
		}
	}
	return vms, nil
}
