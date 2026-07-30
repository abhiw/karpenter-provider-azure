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

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	armcomputefleet "github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/computefleet/armcomputefleet/v2"
	"github.com/go-logr/logr"
	"github.com/samber/lo"
	"sigs.k8s.io/controller-runtime/pkg/log"
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
)

// executor sends batches to the Azure Fleet API.
// It transforms a pending batch into a Fleet CreateOrUpdate call, waits for
// the LRO, runs VM assignment, and distributes results back to each request.
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
// It orchestrates: fleet name -> body -> PUT -> VM list -> shared state -> distribute responses.
//
// Batch splitting: when a single batch exceeds maxFleetCapacity, it is split
// into parallel sub-batches, each creating its own Fleet resource.
func (e *executor) executeBatch(ctx context.Context, batch *batcher.Batch[FleetVMProvisionRequest, FleetBatchResponse]) {
	logger := log.FromContext(ctx).WithValues("batchKey", batch.Key, "batchSize", len(batch.Requests))
	logger.Info("proceeding with Fleet creation", "requestCount", len(batch.Requests))
	e.doFleetCreate(ctx, batch, logger)
}

// doFleetCreate performs the actual Fleet PUT -> VM list -> assignment -> distribute flow.
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
	fields := extractBatchKeyFieldsFromRequest(representative)
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
	// We intentionally do NOT poll the LRO to completion. The Fleet PUT is treated
	// as synchronous: BeginCreateOrUpdate has already issued the request and received
	// the initial (sync-path) response by the time it returns, so we rely on that
	// response and proceed directly to listing the VMs the Fleet created.
	//
	// The interconnect fields are not expressible via the typed armcomputefleet/v2
	// SDK (see rawproperties.go), so they're passed via context to the raw-properties
	// pipeline policy registered on the Fleet client, set here immediately before the call.
	putCtx := WithInterconnectPatch(ctx, InterconnectPatch{
		InterconnectBlockID:    fields.InterconnectBlockID,
		InterconnectGroupID:    fields.InterconnectGroupID,
		InterconnectSubgroupID: fields.InterconnectSubgroupID,
	})
	poller, err := e.fleetClient.BeginCreateOrUpdate(putCtx, e.resourceGroup, name, *fleetBody, nil)
	if err != nil {
		logger.Error(err, "fleet BeginCreateOrUpdate failed")
		fleetErr := fmt.Errorf("fleet create: %w", err)
		e.distributeError(batch, fleetErr)
		return 0, requested, fleetErr
	}
	// Log poller/response state to diagnose whether Fleet completed synchronously
	pollerDone := poller != nil && poller.Done()
	logger.Info("fleet create-or-update returned",
		"pollerNil", poller == nil,
		"pollerDone", pollerDone,
	)
	if pollerDone {
		resp, respErr := poller.Result(ctx)
		if respErr != nil {
			logger.Error(respErr, "fleet poller.Result() failed")
		} else {
			fleetState := ""
			if resp.Properties != nil && resp.Properties.ProvisioningState != nil {
				fleetState = string(*resp.Properties.ProvisioningState)
			}
			logger.Info("fleet poller.Result()",
				"provisioningState", fleetState,
				"fleetID", lo.FromPtr(resp.ID),
			)
		}
	}

	// 5. List VMs created by this Fleet (identified by fleet-name tag).
	vms, err := e.listFleetVMs(ctx, name)
	if err != nil {
		logger.Error(err, "failed to list fleet VMs")
		listErr := fmt.Errorf("list fleet VMs: %w", err)
		e.distributeError(batch, listErr)
		return 0, requested, listErr
	}
	logger.Info("listed fleet VMs", "count", len(vms))

	if len(vms) == 0 {
		logger.Info("WARNING: fleet returned 0 VMs after successful PUT",
			"fleetName", name,
			"requestedCapacity", len(batch.Requests),
			"hint", "Fleet API may have succeeded without provisioning VMs (capacity/quota issue)",
		)
	}

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

	// 6. Distribute shared state to all requests. Promises read providerIDs from it.
	e.distributeSharedState(batch, sharedState)

	// Tagging of assigned VMs is handled out-of-band by the fleettag controller.
	// Surplus VMs (created but never assigned to a NodeClaim) are reclaimed by the
	// generic instance garbage collector, which matches cloud VMs to NodeClaims by
	// ProviderID and deletes any that have no owning NodeClaim.

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

// listFleetVMs lists VMs belonging to a specific Fleet using the Fleet's ListVirtualMachines API.
// The Fleet SDK (v2 beta.3+) returns VMSize and Zone directly, so no compute GET is needed.
// The returned armcompute.VirtualMachine stubs carry ID, Name, Type, VMSize, Zone, and Location
// (from the executor's region). ProvisioningState is NOT available from the Fleet SDK — it is
// resolved later in FleetMemberPromise.Wait() via the fleetvmpoller.
func (e *executor) listFleetVMs(ctx context.Context, name string) ([]*armcompute.VirtualMachine, error) {
	logger := log.FromContext(ctx).WithValues("fleetName", name)

	// Use the Fleet SDK's dedicated ListVirtualMachines API
	pager := e.fleetClient.NewListVirtualMachinesPager(e.resourceGroup, name, nil)
	var fleetVMs []*armcomputefleet.VirtualMachine
	pageNum := 0
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("listing fleet VMs via fleet API: %w", err)
		}
		pageNum++
		fleetVMs = append(fleetVMs, page.Value...)

		// Log raw response
		if rawJSON, mErr := json.Marshal(page); mErr == nil {
			logger.Info("listFleetVMs raw page response", "page", pageNum, "json", string(rawJSON))
		}
	}

	logger.Info("listFleetVMs complete (fleet API)", "fleetVMCount", len(fleetVMs), "pages", pageNum)

	if len(fleetVMs) == 0 {
		return nil, nil
	}

	// Convert Fleet SDK VirtualMachine → armcompute.VirtualMachine using fields
	// available directly from the Fleet ListVirtualMachines API (no compute GET needed).
	var vms []*armcompute.VirtualMachine
	for _, fvm := range fleetVMs {
		if fvm == nil || fvm.Name == nil {
			continue
		}
		vmName := *fvm.Name
		opStatus := ""
		if fvm.OperationStatus != nil {
			opStatus = string(*fvm.OperationStatus)
		}
		// Log all fields returned by Fleet ListVirtualMachines API
		var errMsg string
		if fvm.Error != nil {
			if fvm.Error.Code != nil {
				errMsg = *fvm.Error.Code
			}
			if fvm.Error.Message != nil {
				errMsg += ": " + *fvm.Error.Message
			}
		}
		logger.Info("listFleetVMs: VM entry",
			"vm", vmName,
			"operationStatus", opStatus,
			"id", lo.FromPtr(fvm.ID),
			"type", lo.FromPtr(fvm.Type),
			"vmSize", lo.FromPtr(fvm.VMSize),
			"zone", lo.FromPtr(fvm.Zone),
			"priority", lo.FromPtr(fvm.Priority),
			"error", errMsg,
		)

		// Skip VMs that failed at Fleet level — they were never created in ARM.
		if fvm.OperationStatus != nil && *fvm.OperationStatus == armcomputefleet.VMOperationStatusFailed {
			logger.Info("skipping failed fleet VM", "vm", vmName)
			continue
		}

		// Build armcompute.VirtualMachine from Fleet SDK fields.
		// Location is set from e.location (the executor's own Fleet region):
		// every VM a Fleet creates lives in that Fleet's region. Required by
		// skuAndZone/MakeAKSLabelZoneFromVM for zone label resolution.
		vm := &armcompute.VirtualMachine{
			ID:       fvm.ID,
			Name:     fvm.Name,
			Type:     fvm.Type,
			Location: lo.ToPtr(e.location),
		}
		if fvm.VMSize != nil {
			vm.Properties = &armcompute.VirtualMachineProperties{
				HardwareProfile: &armcompute.HardwareProfile{
					VMSize: lo.ToPtr(armcompute.VirtualMachineSizeTypes(*fvm.VMSize)),
				},
			}
		}
		// Fleet SDK returns Zone as a single *string (e.g. "1");
		// armcompute.VirtualMachine.Zones is []*string.
		if fvm.Zone != nil {
			vm.Zones = []*string{fvm.Zone}
		}
		vms = append(vms, vm)
	}

	logger.Info("listFleetVMs converted fleet VMs", "convertedCount", len(vms), "fleetVMCount", len(fleetVMs))
	return vms, nil
}
