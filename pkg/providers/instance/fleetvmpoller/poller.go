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
	// NotFoundTimeout bounds initial-404 retries from the first not-found response.
	NotFoundTimeout time.Duration
	// ErrorTimeout bounds a consecutive error streak, including GETs and backoff.
	ErrorTimeout time.Duration
	// PollInterval is the interval between GET requests (default 5s).
	PollInterval time.Duration
	// MaxDuration is the maximum duration for the polling operation, including retry
	// backoff (default 15m).
	MaxDuration time.Duration
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
		NotFoundTimeout: 20 * time.Second,
		ErrorTimeout:    2 * time.Minute,
		PollInterval:    5 * time.Second,
		MaxDuration:     15 * time.Minute,
		RetryDelay:      1 * time.Second,
		MaxRetryDelay:   30 * time.Second,
		MaxRetries:      10,
	}
}

// InstantOptions returns poller configuration for tests.
func InstantOptions() Options {
	return Options{
		PollInterval:  1 * time.Millisecond,
		MaxDuration:   time.Second,
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

	// everObserved is set once a GET call returns the VM with non-nil properties,
	// i.e. the VM has become visible via the API at least once. Used to distinguish
	// "not yet visible" 404s (retryable, since Fleet creation may not have propagated
	// to this GET path yet) from "was visible, now gone" 404s (terminal, likely
	// deleted mid-flight).
	everObserved bool
	retryContext context.Context
	retryCancel  context.CancelFunc
}

// NewPoller creates a poller for a specific Fleet VM.
func NewPoller(config Options, client VMGetter, resourceGroup, vmName string) *Poller {
	if config.NotFoundTimeout <= 0 {
		config.NotFoundTimeout = 20 * time.Second
	}
	if config.ErrorTimeout <= 0 {
		config.ErrorTimeout = 2 * time.Minute
	}
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
	pollCtx := ctx
	cancel := func() {}
	if p.config.MaxDuration > 0 {
		timeoutCause := fmt.Errorf(
			"fleet VM %q polling exceeded maximum duration of %s: %w",
			p.vmName,
			p.config.MaxDuration,
			context.DeadlineExceeded,
		)
		pollCtx, cancel = context.WithTimeoutCause(ctx, p.config.MaxDuration, timeoutCause)
	}
	defer cancel()
	defer p.clearErrorWindow()

	var retryAttemptsLeft int
	var currentRetryDelay time.Duration
	p.resetRetryState(&retryAttemptsLeft, &currentRetryDelay)

	ticker := time.NewTicker(p.config.PollInterval)
	defer ticker.Stop()

	for {
		activeCtx := pollCtx
		if p.retryContext != nil {
			activeCtx = p.retryContext
		}
		select {
		case <-activeCtx.Done():
			return nil, p.contextError(activeCtx)
		case <-ticker.C:
			if activeCtx.Err() != nil {
				return nil, p.contextError(activeCtx)
			}
			vm, err, done := p.pollOnce(activeCtx, &retryAttemptsLeft, &currentRetryDelay)
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

	p.everObserved = true

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
		p.startErrorWindow(ctx, fmt.Errorf("unexpected provisioning state %q", state), p.config.ErrorTimeout)
		// Nil or unrecognized state: retry with backoff
		log.FromContext(ctx).V(2).Info("fleet VM poller: unexpected provisioning state",
			"vmName", p.vmName, "state", state, "retriesLeft", *retryAttemptsLeft, "retryDelay", *currentRetryDelay)
		shouldRetry, backoffErr := p.retryWithBackoff(ctx, retryAttemptsLeft, currentRetryDelay)
		if backoffErr != nil {
			return nil, backoffErr, true
		}
		if shouldRetry {
			return nil, nil, false
		}
		return nil, fmt.Errorf("fleet VM %q stuck in state %q after exhausting %d retries", p.vmName, state, p.config.MaxRetries), true
	}
}

func (p *Poller) handleGetError(ctx context.Context, err error, retryAttemptsLeft *int, currentRetryDelay *time.Duration) (*armcompute.VirtualMachine, error, bool) {
	if ctx.Err() != nil {
		return nil, p.contextError(ctx), true
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil, fmt.Errorf("failed to get fleet VM %q: %w", p.vmName, err), true
	}

	// A 404 before the VM has ever been observed may be a propagation delay.
	// Retry within the shorter visibility window without treating it as quota.
	// A 404 after the VM was previously observed likely means it was deleted
	// mid-flight, and remains terminal/non-retryable.
	notFoundBeforeVisible := isNotFoundError(err) && !p.everObserved
	if !isTransientError(err) && !notFoundBeforeVisible {
		return nil, fmt.Errorf("non-retryable error getting fleet VM %q: %w", p.vmName, err), true
	}

	if notFoundBeforeVisible {
		p.startErrorWindow(ctx, err, p.config.NotFoundTimeout)
		log.FromContext(ctx).V(2).Info("fleet VM poller: VM not found yet, likely not propagated to GET path, may retry",
			"vmName", p.vmName, "error", err, "retriesLeft", *retryAttemptsLeft, "retryDelay", *currentRetryDelay)
	} else {
		p.startErrorWindow(ctx, err, p.config.ErrorTimeout)
		log.FromContext(ctx).V(2).Info("fleet VM poller: transient GET error, may retry",
			"vmName", p.vmName, "error", err, "retriesLeft", *retryAttemptsLeft, "retryDelay", *currentRetryDelay)
	}

	shouldRetry, backoffErr := p.retryWithBackoff(ctx, retryAttemptsLeft, currentRetryDelay)
	if backoffErr != nil {
		return nil, backoffErr, true
	}
	if shouldRetry {
		return nil, nil, false
	}
	return nil, fmt.Errorf("failed to get fleet VM %q after exhausting %d retries: %w", p.vmName, p.config.MaxRetries, err), true
}

func (p *Poller) handleNilProperties(ctx context.Context, retryAttemptsLeft *int, currentRetryDelay *time.Duration) (*armcompute.VirtualMachine, error, bool) {
	p.startErrorWindow(ctx, fmt.Errorf("VM properties are nil"), p.config.ErrorTimeout)
	log.FromContext(ctx).V(1).Info("fleet VM poller: nil properties on GET response",
		"vmName", p.vmName, "retriesLeft", *retryAttemptsLeft, "retryDelay", *currentRetryDelay)

	shouldRetry, backoffErr := p.retryWithBackoff(ctx, retryAttemptsLeft, currentRetryDelay)
	if backoffErr != nil {
		return nil, backoffErr, true
	}
	if shouldRetry {
		return nil, nil, false
	}
	return nil, fmt.Errorf("fleet VM %q has nil properties after exhausting %d retries", p.vmName, p.config.MaxRetries), true
}

// retryWithBackoff applies exponential backoff and returns true if retry should continue, false if exhausted.
// It decrements retryAttemptsLeft, sleeps with exponential backoff, and updates currentRetryDelay.
// Mirrors aksmachinepoller's retryWithBackoff for consistent behavior across both pollers.
func (p *Poller) retryWithBackoff(ctx context.Context, retryAttemptsLeft *int, currentRetryDelay *time.Duration) (shouldRetry bool, err error) {
	if p.retryContext != nil {
		ctx = p.retryContext
	}
	if *retryAttemptsLeft <= 0 {
		return false, nil
	}

	*retryAttemptsLeft--

	timer := time.NewTimer(*currentRetryDelay)
	defer timer.Stop()

	select {
	case <-timer.C:
		*currentRetryDelay = min(*currentRetryDelay*2, p.config.MaxRetryDelay)
		return true, nil
	case <-ctx.Done():
		return false, p.contextError(ctx)
	}
}

func (p *Poller) resetRetryState(retryAttemptsLeft *int, currentRetryDelay *time.Duration) {
	p.clearErrorWindow()
	*retryAttemptsLeft = p.config.MaxRetries
	*currentRetryDelay = p.config.RetryDelay
}

func (p *Poller) startErrorWindow(ctx context.Context, err error, duration time.Duration) {
	deadline := time.Now().Add(duration)
	if p.retryContext != nil {
		if current, ok := p.retryContext.Deadline(); ok && !deadline.Before(current) {
			return
		}
	}
	cause := fmt.Errorf("fleet VM %q error retry window of %s expired: %w", p.vmName, duration, err)
	// Keep the previous context alive when tightening its deadline, since the new
	// context inherits cancellation from it.
	previousCancel := p.retryCancel
	p.retryContext, p.retryCancel = context.WithDeadlineCause(ctx, deadline, cause)
	if previousCancel != nil {
		cancel := p.retryCancel
		p.retryCancel = func() {
			cancel()
			previousCancel()
		}
	}
}

func (p *Poller) clearErrorWindow() {
	if p.retryCancel != nil {
		p.retryCancel()
	}
	p.retryContext = nil
	p.retryCancel = nil
}

func (p *Poller) contextError(ctx context.Context) error {
	return fmt.Errorf("context ended while polling fleet VM %q: %w", p.vmName, context.Cause(ctx))
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

// isNotFoundError determines if an error is an HTTP 404 (Not Found) response.
func isNotFoundError(err error) bool {
	var respErr *azcore.ResponseError
	if errors.As(err, &respErr) {
		return respErr.StatusCode == http.StatusNotFound
	}
	return false
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
