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
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"github.com/samber/lo"
	corecloudprovider "sigs.k8s.io/karpenter/pkg/cloudprovider"

	"github.com/Azure/karpenter-provider-azure/pkg/providers/azclient/fleet"
)

const (
	testTagRG = "rg-test"
)

// makeVM constructs a minimal VM carrying only the fields findVMByTag inspects
// (Name for assertions, Tags for the match decision). We deliberately avoid
// pkg/fake here because it imports pkg/providers/instance and would create an
// import cycle in this _test file.
func makeVM(name string, tags map[string]string) armcompute.VirtualMachine {
	t := map[string]*string{}
	for k, v := range tags {
		t[k] = lo.ToPtr(v)
	}
	return armcompute.VirtualMachine{
		Name: lo.ToPtr(name),
		Tags: t,
	}
}

// pagerFromPages returns a pager that yields the given pages in order. Empty
// pages slice means an immediately-empty pager.
func pagerFromPages(pages [][]*armcompute.VirtualMachine) *runtime.Pager[armcompute.VirtualMachinesClientListResponse] {
	calls := 0
	return runtime.NewPager(runtime.PagingHandler[armcompute.VirtualMachinesClientListResponse]{
		More: func(_ armcompute.VirtualMachinesClientListResponse) bool {
			return calls < len(pages)
		},
		Fetcher: func(_ context.Context, _ *armcompute.VirtualMachinesClientListResponse) (armcompute.VirtualMachinesClientListResponse, error) {
			page := pages[calls]
			calls++
			return armcompute.VirtualMachinesClientListResponse{
				VirtualMachineListResult: armcompute.VirtualMachineListResult{Value: page},
			}, nil
		},
	})
}

// errorPager returns a pager whose first Fetcher call returns sentinel.
func errorPager(sentinel error) *runtime.Pager[armcompute.VirtualMachinesClientListResponse] {
	return runtime.NewPager(runtime.PagingHandler[armcompute.VirtualMachinesClientListResponse]{
		More: func(_ armcompute.VirtualMachinesClientListResponse) bool { return true },
		Fetcher: func(_ context.Context, _ *armcompute.VirtualMachinesClientListResponse) (armcompute.VirtualMachinesClientListResponse, error) {
			return armcompute.VirtualMachinesClientListResponse{}, sentinel
		},
	})
}

func TestFindVMByTag_SinglePageHit(t *testing.T) {
	target := makeVM("vm-target", map[string]string{fleet.NodeClaimNameTagKey: "nc-1"})
	other := makeVM("vm-other", map[string]string{fleet.NodeClaimNameTagKey: "nc-2"})
	pager := pagerFromPages([][]*armcompute.VirtualMachine{{&other, &target}})

	got, err := findVMByTag(context.Background(), pager, fleet.NodeClaimNameTagKey, "nc-1", testTagRG)
	if err != nil {
		t.Fatalf("expected nil err, got %v", err)
	}
	if got == nil || lo.FromPtr(got.Name) != "vm-target" {
		t.Fatalf("expected vm-target, got %#v", got)
	}
}

func TestFindVMByTag_MultiPageHitOnSecond(t *testing.T) {
	miss := makeVM("vm-miss", map[string]string{fleet.NodeClaimNameTagKey: "nc-x"})
	target := makeVM("vm-target", map[string]string{fleet.NodeClaimNameTagKey: "nc-1"})
	pager := pagerFromPages([][]*armcompute.VirtualMachine{
		{&miss},
		{&target},
	})

	got, err := findVMByTag(context.Background(), pager, fleet.NodeClaimNameTagKey, "nc-1", testTagRG)
	if err != nil {
		t.Fatalf("expected nil err, got %v", err)
	}
	if got == nil || lo.FromPtr(got.Name) != "vm-target" {
		t.Fatalf("expected vm-target on second page, got %#v", got)
	}
}

func TestFindVMByTag_NoMatchReturnsNotFound(t *testing.T) {
	other := makeVM("vm-other", map[string]string{fleet.NodeClaimNameTagKey: "nc-2"})
	pager := pagerFromPages([][]*armcompute.VirtualMachine{{&other}})

	got, err := findVMByTag(context.Background(), pager, fleet.NodeClaimNameTagKey, "nc-1", testTagRG)
	if got != nil {
		t.Fatalf("expected nil VM, got %#v", got)
	}
	if err == nil {
		t.Fatalf("expected NodeClaimNotFoundError, got nil")
	}
	if !corecloudprovider.IsNodeClaimNotFoundError(err) {
		t.Fatalf("expected IsNodeClaimNotFoundError, got %v", err)
	}
}

func TestFindVMByTag_PagerErrorPropagatesNotWrappedAsNotFound(t *testing.T) {
	sentinel := errors.New("boom")
	got, err := findVMByTag(context.Background(), errorPager(sentinel), fleet.NodeClaimNameTagKey, "nc-1", testTagRG)
	if got != nil {
		t.Fatalf("expected nil VM, got %#v", got)
	}
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if corecloudprovider.IsNodeClaimNotFoundError(err) {
		t.Fatalf("pager error must NOT be classified as NodeClaimNotFound: %v", err)
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected wrapped sentinel, got %v", err)
	}
	if !strings.Contains(err.Error(), testTagRG) {
		t.Fatalf("expected resource group %q in error, got %q", testTagRG, err.Error())
	}
}

func TestFindVMByTag_NilTagsAndNilVMSkipped(t *testing.T) {
	// VM whose Tags map is nil should not nil-deref; the page slice also
	// contains a nil VM pointer for the same defensive reason.
	nilTags := armcompute.VirtualMachine{
		Name: lo.ToPtr("vm-niltags"),
		Tags: nil,
	}
	target := makeVM("vm-target", map[string]string{fleet.NodeClaimNameTagKey: "nc-1"})
	pager := pagerFromPages([][]*armcompute.VirtualMachine{
		{nil, &nilTags},
		{&target},
	})

	got, err := findVMByTag(context.Background(), pager, fleet.NodeClaimNameTagKey, "nc-1", testTagRG)
	if err != nil {
		t.Fatalf("expected nil err, got %v", err)
	}
	if got == nil || lo.FromPtr(got.Name) != "vm-target" {
		t.Fatalf("expected vm-target after skipping nil/nil-tag entries, got %#v", got)
	}
}
