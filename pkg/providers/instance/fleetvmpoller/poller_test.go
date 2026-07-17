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

package fleetvmpoller

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockVMGetter implements VMGetter for testing.
type mockVMGetter struct {
	responses []mockResponse
	callCount atomic.Int32
}

type mockResponse struct {
	vm  *armcompute.VirtualMachine
	err error
}

func (m *mockVMGetter) Get(
	_ context.Context,
	_ string,
	_ string,
	_ *armcompute.VirtualMachinesClientGetOptions,
) (armcompute.VirtualMachinesClientGetResponse, error) {
	idx := int(m.callCount.Add(1)) - 1
	if idx >= len(m.responses) {
		idx = len(m.responses) - 1
	}
	resp := m.responses[idx]
	if resp.err != nil {
		return armcompute.VirtualMachinesClientGetResponse{}, resp.err
	}
	return armcompute.VirtualMachinesClientGetResponse{
		VirtualMachine: *resp.vm,
	}, nil
}

func (m *mockVMGetter) CallCount() int {
	return int(m.callCount.Load())
}

func testOptions() Options {
	return Options{
		PollInterval:  10 * time.Millisecond,
		RetryDelay:    5 * time.Millisecond,
		MaxRetryDelay: 20 * time.Millisecond,
		MaxRetries:    3,
	}
}

func vmWithState(state string) *armcompute.VirtualMachine {
	return &armcompute.VirtualMachine{
		Name: lo.ToPtr("fleet-vm-1"),
		ID: lo.ToPtr(
			"/subscriptions/12345678-1234-1234-1234-123456789012" +
				"/resourceGroups/MC_rg" +
				"/providers/Microsoft.Compute/virtualMachines/fleet-vm-1",
		),
		Properties: &armcompute.VirtualMachineProperties{
			ProvisioningState: lo.ToPtr(state),
		},
	}
}

func vmWithFailedStateAndInstanceView(errorCode, errorMsg string) *armcompute.VirtualMachine {
	vm := vmWithState("Failed")
	vm.Properties.InstanceView = &armcompute.VirtualMachineInstanceView{
		Statuses: []*armcompute.InstanceViewStatus{
			{
				Code:    lo.ToPtr(errorCode),
				Level:   lo.ToPtr(armcompute.StatusLevelTypesError),
				Message: lo.ToPtr(errorMsg),
			},
		},
	}
	return vm
}

// --- Tests for PollUntilDone ---

func TestPollUntilDone_ImmediateSuccess(t *testing.T) {
	mock := &mockVMGetter{
		responses: []mockResponse{
			{vm: vmWithState("Succeeded")},
		},
	}

	poller := NewPoller(testOptions(), mock, "rg", "fleet-vm-1")
	vm, err := poller.PollUntilDone(context.Background())

	assert.NoError(t, err)
	require.NotNil(t, vm)
	assert.Equal(t, "fleet-vm-1", lo.FromPtr(vm.Name))
	assert.Equal(t, 1, mock.CallCount())
}

func TestPollUntilDone_CreatingThenSucceeded(t *testing.T) {
	mock := &mockVMGetter{
		responses: []mockResponse{
			{vm: vmWithState("Creating")},
			{vm: vmWithState("Creating")},
			{vm: vmWithState("Succeeded")},
		},
	}

	poller := NewPoller(testOptions(), mock, "rg", "fleet-vm-1")
	vm, err := poller.PollUntilDone(context.Background())

	assert.NoError(t, err)
	require.NotNil(t, vm)
	assert.Equal(t, "Succeeded", lo.FromPtr(vm.Properties.ProvisioningState))
	assert.Equal(t, 3, mock.CallCount())
}

func TestPollUntilDone_UpdatingThenSucceeded(t *testing.T) {
	mock := &mockVMGetter{
		responses: []mockResponse{
			{vm: vmWithState("Updating")},
			{vm: vmWithState("Succeeded")},
		},
	}

	poller := NewPoller(testOptions(), mock, "rg", "fleet-vm-1")
	vm, err := poller.PollUntilDone(context.Background())

	assert.NoError(t, err)
	require.NotNil(t, vm)
	assert.Equal(t, 2, mock.CallCount())
}

func TestPollUntilDone_CreatingThenFailed(t *testing.T) {
	mock := &mockVMGetter{
		responses: []mockResponse{
			{vm: vmWithState("Creating")},
			{vm: vmWithFailedStateAndInstanceView("SkuNotAvailable", "SKU not available in region")},
		},
	}

	poller := NewPoller(testOptions(), mock, "rg", "fleet-vm-1")
	vm, err := poller.PollUntilDone(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "provisioning failed")
	assert.Contains(t, err.Error(), "SkuNotAvailable")
	// VM is still returned on failure for caller to inspect
	require.NotNil(t, vm)
	assert.Equal(t, 2, mock.CallCount())
}

func TestPollUntilDone_ImmediateFailed(t *testing.T) {
	mock := &mockVMGetter{
		responses: []mockResponse{
			{vm: vmWithFailedStateAndInstanceView("AllocationFailed", "Allocation failed")},
		},
	}

	poller := NewPoller(testOptions(), mock, "rg", "fleet-vm-1")
	vm, err := poller.PollUntilDone(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "provisioning failed")
	assert.Contains(t, err.Error(), "AllocationFailed")
	require.NotNil(t, vm)
	assert.Equal(t, 1, mock.CallCount())
}

func TestPollUntilDone_FailedWithNoInstanceView(t *testing.T) {
	mock := &mockVMGetter{
		responses: []mockResponse{
			{vm: vmWithState("Failed")}, // no instanceView
		},
	}

	poller := NewPoller(testOptions(), mock, "rg", "fleet-vm-1")
	_, err := poller.PollUntilDone(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "provisioning failed")
	assert.Contains(t, err.Error(), "no details available")
	assert.Equal(t, 1, mock.CallCount())
}

func TestPollUntilDone_ContextCancelled(t *testing.T) {
	mock := &mockVMGetter{
		responses: []mockResponse{
			{vm: vmWithState("Creating")},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	poller := NewPoller(testOptions(), mock, "rg", "fleet-vm-1")
	_, err := poller.PollUntilDone(ctx)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "context canceled")
}

func TestPollUntilDone_ContextDeadlineExceeded(t *testing.T) {
	mock := &mockVMGetter{
		responses: []mockResponse{
			{vm: vmWithState("Creating")},
			{vm: vmWithState("Creating")},
			{vm: vmWithState("Creating")},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
	defer cancel()

	poller := NewPoller(testOptions(), mock, "rg", "fleet-vm-1")
	_, err := poller.PollUntilDone(ctx)

	require.Error(t, err)
	assert.True(t, errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) ||
		assert.ObjectsAreEqual("context canceled", err.Error()))
}

func TestPollUntilDone_TransientErrorRetry(t *testing.T) {
	transientErr := &azcore.ResponseError{
		StatusCode: http.StatusTooManyRequests,
		ErrorCode:  "TooManyRequests",
	}

	mock := &mockVMGetter{
		responses: []mockResponse{
			{vm: vmWithState("Creating")},
			{err: transientErr},
			{vm: vmWithState("Succeeded")},
		},
	}

	poller := NewPoller(testOptions(), mock, "rg", "fleet-vm-1")
	vm, err := poller.PollUntilDone(context.Background())

	assert.NoError(t, err)
	require.NotNil(t, vm)
	assert.Equal(t, 3, mock.CallCount())
}

func TestPollUntilDone_MultipleTransientErrorsThenSuccess(t *testing.T) {
	transientErr := &azcore.ResponseError{
		StatusCode: http.StatusInternalServerError,
		ErrorCode:  "InternalServerError",
	}

	mock := &mockVMGetter{
		responses: []mockResponse{
			{err: transientErr},
			{err: transientErr},
			{vm: vmWithState("Succeeded")},
		},
	}

	poller := NewPoller(testOptions(), mock, "rg", "fleet-vm-1")
	vm, err := poller.PollUntilDone(context.Background())

	assert.NoError(t, err)
	require.NotNil(t, vm)
	assert.Equal(t, 3, mock.CallCount())
}

func TestPollUntilDone_NonTransientErrorFailsImmediately(t *testing.T) {
	notFoundErr := &azcore.ResponseError{
		StatusCode: http.StatusNotFound,
		ErrorCode:  "ResourceNotFound",
	}

	mock := &mockVMGetter{
		responses: []mockResponse{
			{vm: vmWithState("Creating")},
			{err: notFoundErr},
		},
	}

	poller := NewPoller(testOptions(), mock, "rg", "fleet-vm-1")
	_, err := poller.PollUntilDone(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "non-retryable error")
	assert.Equal(t, 2, mock.CallCount())
}

func TestPollUntilDone_UnauthorizedErrorFailsImmediately(t *testing.T) {
	authErr := &azcore.ResponseError{
		StatusCode: http.StatusUnauthorized,
		ErrorCode:  "AuthenticationFailed",
	}

	mock := &mockVMGetter{
		responses: []mockResponse{
			{err: authErr},
		},
	}

	poller := NewPoller(testOptions(), mock, "rg", "fleet-vm-1")
	_, err := poller.PollUntilDone(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "non-retryable error")
	assert.Equal(t, 1, mock.CallCount())
}

func TestPollUntilDone_ForbiddenErrorFailsImmediately(t *testing.T) {
	forbiddenErr := &azcore.ResponseError{
		StatusCode: http.StatusForbidden,
		ErrorCode:  "AuthorizationFailed",
	}

	mock := &mockVMGetter{
		responses: []mockResponse{
			{err: forbiddenErr},
		},
	}

	poller := NewPoller(testOptions(), mock, "rg", "fleet-vm-1")
	_, err := poller.PollUntilDone(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "non-retryable error")
	assert.Equal(t, 1, mock.CallCount())
}

func TestPollUntilDone_ExhaustedRetriesOnTransientErrors(t *testing.T) {
	transientErr := &azcore.ResponseError{
		StatusCode: http.StatusServiceUnavailable,
		ErrorCode:  "ServiceUnavailable",
	}

	mock := &mockVMGetter{
		responses: []mockResponse{
			{err: transientErr},
			{err: transientErr},
			{err: transientErr},
			{err: transientErr}, // exceeds MaxRetries=3
		},
	}

	opts := testOptions()
	opts.MaxRetries = 3

	poller := NewPoller(opts, mock, "rg", "fleet-vm-1")
	_, err := poller.PollUntilDone(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "exhausting")
}

func TestPollUntilDone_NilPropertiesRetries(t *testing.T) {
	vmWithNilProps := &armcompute.VirtualMachine{
		Name:       lo.ToPtr("fleet-vm-1"),
		Properties: nil,
	}

	mock := &mockVMGetter{
		responses: []mockResponse{
			{vm: vmWithNilProps},
			{vm: vmWithNilProps},
			{vm: vmWithState("Succeeded")},
		},
	}

	poller := NewPoller(testOptions(), mock, "rg", "fleet-vm-1")
	vm, err := poller.PollUntilDone(context.Background())

	assert.NoError(t, err)
	require.NotNil(t, vm)
	assert.Equal(t, 3, mock.CallCount())
}

func TestPollUntilDone_NilPropertiesExhaustsRetries(t *testing.T) {
	vmWithNilProps := &armcompute.VirtualMachine{
		Name:       lo.ToPtr("fleet-vm-1"),
		Properties: nil,
	}

	mock := &mockVMGetter{
		responses: []mockResponse{
			{vm: vmWithNilProps},
			{vm: vmWithNilProps},
			{vm: vmWithNilProps},
			{vm: vmWithNilProps},
		},
	}

	opts := testOptions()
	opts.MaxRetries = 3

	poller := NewPoller(opts, mock, "rg", "fleet-vm-1")
	_, err := poller.PollUntilDone(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "nil properties")
	assert.Contains(t, err.Error(), "exhausting")
}

func TestPollUntilDone_UnrecognizedStateExhaustsRetries(t *testing.T) {
	mock := &mockVMGetter{
		responses: []mockResponse{
			{vm: vmWithState("SomeWeirdState")},
			{vm: vmWithState("SomeWeirdState")},
			{vm: vmWithState("SomeWeirdState")},
			{vm: vmWithState("SomeWeirdState")},
		},
	}

	opts := testOptions()
	opts.MaxRetries = 3

	poller := NewPoller(opts, mock, "rg", "fleet-vm-1")
	_, err := poller.PollUntilDone(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "stuck in state")
	assert.Contains(t, err.Error(), "SomeWeirdState")
}

func TestPollUntilDone_RetryBudgetResetsOnHealthyState(t *testing.T) {
	transientErr := &azcore.ResponseError{
		StatusCode: http.StatusInternalServerError,
		ErrorCode:  "InternalServerError",
	}

	// MaxRetries=2:
	// 1. Creating (healthy - budget stays at 2)
	// 2. transient error (budget: 2->1)
	// 3. transient error (budget: 1->0)
	// 4. Creating (healthy - budget resets to 2)
	// 5. transient error (budget: 2->1)
	// 6. transient error (budget: 1->0)
	// 7. Succeeded (done)
	mock := &mockVMGetter{
		responses: []mockResponse{
			{vm: vmWithState("Creating")},
			{err: transientErr},
			{err: transientErr},
			{vm: vmWithState("Creating")},
			{err: transientErr},
			{err: transientErr},
			{vm: vmWithState("Succeeded")},
		},
	}

	opts := testOptions()
	opts.MaxRetries = 2

	poller := NewPoller(opts, mock, "rg", "fleet-vm-1")
	vm, err := poller.PollUntilDone(context.Background())

	assert.NoError(t, err)
	require.NotNil(t, vm)
	assert.Equal(t, 7, mock.CallCount())
}

func TestPollUntilDone_RetryBudgetResetsOnUpdatingState(t *testing.T) {
	transientErr := &azcore.ResponseError{
		StatusCode: http.StatusBadGateway,
		ErrorCode:  "BadGateway",
	}

	mock := &mockVMGetter{
		responses: []mockResponse{
			{err: transientErr},            // budget: 2->1
			{vm: vmWithState("Updating")},  // budget resets to 2
			{err: transientErr},            // budget: 2->1
			{err: transientErr},            // budget: 1->0
			{vm: vmWithState("Succeeded")}, // done
		},
	}

	opts := testOptions()
	opts.MaxRetries = 2

	poller := NewPoller(opts, mock, "rg", "fleet-vm-1")
	vm, err := poller.PollUntilDone(context.Background())

	assert.NoError(t, err)
	require.NotNil(t, vm)
	assert.Equal(t, 5, mock.CallCount())
}

func TestPollUntilDone_NetworkErrorIsTransient(t *testing.T) {
	networkErr := errors.New("connection reset by peer")

	mock := &mockVMGetter{
		responses: []mockResponse{
			{err: networkErr},
			{vm: vmWithState("Succeeded")},
		},
	}

	poller := NewPoller(testOptions(), mock, "rg", "fleet-vm-1")
	vm, err := poller.PollUntilDone(context.Background())

	assert.NoError(t, err)
	require.NotNil(t, vm)
	assert.Equal(t, 2, mock.CallCount())
}

func TestPollUntilDone_ContextErrorInGetFailsImmediately(t *testing.T) {
	mock := &mockVMGetter{
		responses: []mockResponse{
			{err: context.Canceled},
		},
	}

	poller := NewPoller(testOptions(), mock, "rg", "fleet-vm-1")
	_, err := poller.PollUntilDone(context.Background())

	require.Error(t, err)
	assert.True(t, errors.Is(err, context.Canceled))
	assert.Equal(t, 1, mock.CallCount())
}

// --- Tests for isTransientError ---

func TestIsTransientError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name: "408 RequestTimeout",
			err: &azcore.ResponseError{
				StatusCode: http.StatusRequestTimeout,
			},
			expected: true,
		},
		{
			name: "429 TooManyRequests",
			err: &azcore.ResponseError{
				StatusCode: http.StatusTooManyRequests,
			},
			expected: true,
		},
		{
			name: "500 InternalServerError",
			err: &azcore.ResponseError{
				StatusCode: http.StatusInternalServerError,
			},
			expected: true,
		},
		{
			name: "502 BadGateway",
			err: &azcore.ResponseError{
				StatusCode: http.StatusBadGateway,
			},
			expected: true,
		},
		{
			name: "503 ServiceUnavailable",
			err: &azcore.ResponseError{
				StatusCode: http.StatusServiceUnavailable,
			},
			expected: true,
		},
		{
			name: "504 GatewayTimeout",
			err: &azcore.ResponseError{
				StatusCode: http.StatusGatewayTimeout,
			},
			expected: true,
		},
		{
			name: "404 NotFound is NOT transient",
			err: &azcore.ResponseError{
				StatusCode: http.StatusNotFound,
			},
			expected: false,
		},
		{
			name: "401 Unauthorized is NOT transient",
			err: &azcore.ResponseError{
				StatusCode: http.StatusUnauthorized,
			},
			expected: false,
		},
		{
			name: "403 Forbidden is NOT transient",
			err: &azcore.ResponseError{
				StatusCode: http.StatusForbidden,
			},
			expected: false,
		},
		{
			name: "400 BadRequest is NOT transient",
			err: &azcore.ResponseError{
				StatusCode: http.StatusBadRequest,
			},
			expected: false,
		},
		{
			name: "409 Conflict is NOT transient",
			err: &azcore.ResponseError{
				StatusCode: http.StatusConflict,
			},
			expected: false,
		},
		{
			name:     "generic error (network) IS transient",
			err:      errors.New("connection reset by peer"),
			expected: true,
		},
		{
			name:     "wrapped network error IS transient",
			err:      errors.New("dial tcp: lookup compute.azure.com: no such host"),
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isTransientError(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// --- Tests for extractProvisioningError ---

func TestExtractProvisioningError_NilVM(t *testing.T) {
	msg := extractProvisioningError(nil)
	assert.Contains(t, msg, "no details available")
}

func TestExtractProvisioningError_NilProperties(t *testing.T) {
	vm := &armcompute.VirtualMachine{}
	msg := extractProvisioningError(vm)
	assert.Contains(t, msg, "no details available")
}

func TestExtractProvisioningError_NilInstanceView(t *testing.T) {
	vm := &armcompute.VirtualMachine{
		Properties: &armcompute.VirtualMachineProperties{},
	}
	msg := extractProvisioningError(vm)
	assert.Contains(t, msg, "no details available")
}

func TestExtractProvisioningError_EmptyStatuses(t *testing.T) {
	vm := &armcompute.VirtualMachine{
		Properties: &armcompute.VirtualMachineProperties{
			InstanceView: &armcompute.VirtualMachineInstanceView{
				Statuses: []*armcompute.InstanceViewStatus{},
			},
		},
	}
	msg := extractProvisioningError(vm)
	assert.Contains(t, msg, "no error details in instanceView")
}

func TestExtractProvisioningError_ErrorStatus(t *testing.T) {
	vm := &armcompute.VirtualMachine{
		Properties: &armcompute.VirtualMachineProperties{
			InstanceView: &armcompute.VirtualMachineInstanceView{
				Statuses: []*armcompute.InstanceViewStatus{
					{
						Code:    lo.ToPtr("ProvisioningState/failed/AllocationFailed"),
						Level:   lo.ToPtr(armcompute.StatusLevelTypesError),
						Message: lo.ToPtr("Allocation failed due to zone constraints"),
					},
				},
			},
		},
	}
	msg := extractProvisioningError(vm)
	assert.Contains(t, msg, "AllocationFailed")
	assert.Contains(t, msg, "zone constraints")
}

func TestExtractProvisioningError_ErrorStatusWithDisplayStatus(t *testing.T) {
	vm := &armcompute.VirtualMachine{
		Properties: &armcompute.VirtualMachineProperties{
			InstanceView: &armcompute.VirtualMachineInstanceView{
				Statuses: []*armcompute.InstanceViewStatus{
					{
						Code:          lo.ToPtr("ProvisioningState/failed"),
						Level:         lo.ToPtr(armcompute.StatusLevelTypesError),
						Message:       nil, // no message
						DisplayStatus: lo.ToPtr("Provisioning failed"),
					},
				},
			},
		},
	}
	msg := extractProvisioningError(vm)
	assert.Contains(t, msg, "Provisioning failed")
}

func TestExtractProvisioningError_SkipsNonErrorStatuses(t *testing.T) {
	vm := &armcompute.VirtualMachine{
		Properties: &armcompute.VirtualMachineProperties{
			InstanceView: &armcompute.VirtualMachineInstanceView{
				Statuses: []*armcompute.InstanceViewStatus{
					{
						Code:  lo.ToPtr("ProvisioningState/succeeded"),
						Level: lo.ToPtr(armcompute.StatusLevelTypesInfo),
					},
					nil, // nil status should be skipped
					{
						Code:    lo.ToPtr("ProvisioningState/failed/QuotaExceeded"),
						Level:   lo.ToPtr(armcompute.StatusLevelTypesError),
						Message: lo.ToPtr("Quota exceeded"),
					},
				},
			},
		},
	}
	msg := extractProvisioningError(vm)
	assert.Contains(t, msg, "QuotaExceeded")
}

// --- Tests for DefaultOptions / InstantOptions ---

func TestDefaultOptions(t *testing.T) {
	opts := DefaultOptions()
	assert.Equal(t, 5*time.Second, opts.PollInterval)
	assert.Equal(t, 1*time.Second, opts.RetryDelay)
	assert.Equal(t, 30*time.Second, opts.MaxRetryDelay)
	assert.Equal(t, 10, opts.MaxRetries)
}

func TestInstantOptions(t *testing.T) {
	opts := InstantOptions()
	assert.Equal(t, 1*time.Millisecond, opts.PollInterval)
	assert.Equal(t, 1*time.Millisecond, opts.RetryDelay)
	assert.Equal(t, 1*time.Millisecond, opts.MaxRetryDelay)
	assert.Equal(t, 3, opts.MaxRetries)
}
