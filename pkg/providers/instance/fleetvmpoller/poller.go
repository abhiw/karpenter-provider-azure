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

// Package fleetvmpoller provides a GET-based poller for tracking Fleet VM provisioning
// status by polling GET /virtualMachines/{name} until terminal state. This follows the
// same pattern as aksmachinepoller but targets ARM compute VMs instead of AKS machines.
//
// Fleet VMs don't have an LRO to poll (Fleet intentionally skips LRO polling), so this
// poller fills that gap by repeatedly GET-ing the VM resource until provisioningState
// reaches Succeeded or Failed.
package fleetvmpoller

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"github.com/samber/lo"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// VMGetter abstracts the compute VM GET call for testability.
type VMGetter interface {
	Get(ctx context.Context, resourceGroupName string, vmName string, options *armcompute.VirtualMachinesClientGetOptions) (armcompute.VirtualMachinesClientGetResponse, error)
}

// Options contains configuration for polling. Aligned with aksmachinepoller defaults.
type Options struct {
	// PollInterval is the interval between GET requests (default 5s).
	PollInterval time.Duration
	// RetryDelay is the initial delay before retrying after a transient error (default 1s).
	RetryDelay time.Duration
	// MaxRetryDelay is the maximum backoff delay (default 30s).
	MaxRetryDelay time.Duration
	// MaxRetries is the max consecutive retry attempts before giving up (default 10).
	// Resets when a healthy non-terminal state (Creating/Updating) is observed.
	MaxRetries int
}

// DefaultOptions returns production poller configuration (same as aksmachinepoller).
func DefaultOptions() Options {
	return Options{
		PollInterval:  5 * time.Second,
		RetryDelay:    1 * time.Second,
		MaxRetryDelay: 30 * time.Second,
		MaxRetries:    10,
	}
}

// InstantOptions returns poller configuration for tests.
func InstantOptions() Options {
	return Options{
		PollInterval:  1 * time.Millisecond,
		RetryDelay:    1 * time.Millisecond,
		MaxRetryDelay: 1 * time.Millisecond,
		MaxRetries:    3,
	}
}

// Poller polls a Fleet VM via compute GET until provisioningState reaches a terminal state.
type Poller struct {
	config        Options
	client        VMGetter
	resourceGroup string
	vmName        string
}

// NewPoller creates a poller for a specific Fleet VM.
func NewPoller(config Options, client VMGetter, resourceGroup, vmName string) *Poller {
	return &Poller{
		config:        config,
		client:        client,
		resourceGroup: resourceGroup,
		vmName:        vmName,
	}
}

// PollUntilDone polls GET /virtualMachines/{name} until provisioningState is terminal.
// Returns the full VM on success (Succeeded), or an error on failure (Failed/timeout/ctx cancel).
// The returned error wraps the provisioning failure details when available.
func (p *Poller) PollUntilDone(ctx context.Context) (*armcompute.VirtualMachine, error) {
	var retryAttemptsLeft int
	var currentRetryDelay time.Duration
	p.resetRetryState(&retryAttemptsLeft, &currentRetryDelay)

	ticker := time.NewTicker(p.config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("context canceled while polling fleet VM %q: %w", p.vmName, ctx.Err())
		case <-ticker.C:
			vm, err, done := p.pollOnce(ctx, &retryAttemptsLeft, &currentRetryDelay)
			if done {
				return vm, err
			}
		}
	}
}

func (p *Poller) pollOnce(ctx context.Context, retryAttemptsLeft *int, currentRetryDelay *time.Duration) (*armcompute.VirtualMachine, error, bool) {
	resp, err := p.client.Get(ctx, p.resourceGroup, p.vmName, nil)
	if err != nil {
		return p.handleGetError(ctx, err, retryAttemptsLeft, currentRetryDelay)
	}

	vm := &resp.VirtualMachine
	if vm.Properties == nil {
		return p.handleNilProperties(ctx, retryAttemptsLeft, currentRetryDelay)
	}

	state := lo.FromPtr(vm.Properties.ProvisioningState)
	switch state {
	case "Succeeded":
		return vm, nil, true
	case "Failed":
		errMsg := extractProvisioningError(vm)
		return vm, fmt.Errorf("fleet VM %q provisioning failed: %s", p.vmName, errMsg), true
	case "Creating", "Updating":
		// Healthy non-terminal state: reset retry budget, continue polling
		p.resetRetryState(retryAttemptsLeft, currentRetryDelay)
		return nil, nil, false
	default:
		// Nil or unrecognized state: retry with backoff
		log.FromContext(ctx).V(2).Info("fleet VM poller: unexpected provisioning state",
			"vmName", p.vmName, "state", state, "retriesLeft", *retryAttemptsLeft)
		if *retryAttemptsLeft > 0 {
			p.consumeRetry(ctx, retryAttemptsLeft, currentRetryDelay)
			return nil, nil, false
		}
		return nil, fmt.Errorf("fleet VM %q stuck in state %q after exhausting %d retries", p.vmName, state, p.config.MaxRetries), true
	}
}

func (p *Poller) handleGetError(ctx context.Context, err error, retryAttemptsLeft *int, currentRetryDelay *time.Duration) (*armcompute.VirtualMachine, error, bool) {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil, fmt.Errorf("failed to get fleet VM %q: %w", p.vmName, err), true
	}

	if !isTransientError(err) {
		return nil, fmt.Errorf("non-retryable error getting fleet VM %q: %w", p.vmName, err), true
	}

	log.FromContext(ctx).V(2).Info("fleet VM poller: transient GET error, may retry",
		"vmName", p.vmName, "error", err, "retriesLeft", *retryAttemptsLeft)

	if *retryAttemptsLeft > 0 {
		p.consumeRetry(ctx, retryAttemptsLeft, currentRetryDelay)
		return nil, nil, false
	}
	return nil, fmt.Errorf("failed to get fleet VM %q after exhausting %d retries: %w", p.vmName, p.config.MaxRetries, err), true
}

func (p *Poller) handleNilProperties(ctx context.Context, retryAttemptsLeft *int, currentRetryDelay *time.Duration) (*armcompute.VirtualMachine, error, bool) {
	log.FromContext(ctx).V(1).Info("fleet VM poller: nil properties on GET response",
		"vmName", p.vmName, "retriesLeft", *retryAttemptsLeft)

	if *retryAttemptsLeft > 0 {
		p.consumeRetry(ctx, retryAttemptsLeft, currentRetryDelay)
		return nil, nil, false
	}
	return nil, fmt.Errorf("fleet VM %q has nil properties after exhausting %d retries", p.vmName, p.config.MaxRetries), true
}

func (p *Poller) consumeRetry(_ context.Context, retryAttemptsLeft *int, currentRetryDelay *time.Duration) {
	*retryAttemptsLeft--
	time.Sleep(*currentRetryDelay)
	*currentRetryDelay = min(*currentRetryDelay*2, p.config.MaxRetryDelay)
}

func (p *Poller) resetRetryState(retryAttemptsLeft *int, currentRetryDelay *time.Duration) {
	*retryAttemptsLeft = p.config.MaxRetries
	*currentRetryDelay = p.config.RetryDelay
}

// isTransientError checks if an error is retryable (same logic as aksmachinepoller).
func isTransientError(err error) bool {
	if err == nil {
		return false
	}
	var respErr *azcore.ResponseError
	if errors.As(err, &respErr) {
		switch respErr.StatusCode {
		case http.StatusRequestTimeout,
			http.StatusTooManyRequests,
			http.StatusInternalServerError,
			http.StatusBadGateway,
			http.StatusServiceUnavailable,
			http.StatusGatewayTimeout:
			return true
		default:
			return false
		}
	}
	// Network errors, timeouts, etc. are transient
	return true
}

// extractProvisioningError extracts a human-readable error from a failed VM's instanceView.
func extractProvisioningError(vm *armcompute.VirtualMachine) string {
	if vm == nil || vm.Properties == nil || vm.Properties.InstanceView == nil {
		return "provisioningState=Failed (no details available)"
	}
	for _, status := range vm.Properties.InstanceView.Statuses {
		if status == nil {
			continue
		}
		code := lo.FromPtr(status.Code)
		// ProvisioningState statuses have code like "ProvisioningState/failed"
		if len(code) > 0 && lo.FromPtr(status.Level) == armcompute.StatusLevelTypesError {
			msg := lo.FromPtr(status.Message)
			if msg == "" {
				msg = lo.FromPtr(status.DisplayStatus)
			}
			return fmt.Sprintf("code=%s message=%s", code, msg)
		}
	}
	return "provisioningState=Failed (no error details in instanceView)"
}
