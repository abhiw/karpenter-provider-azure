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

package fleetvmgc

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork"
	"github.com/samber/lo"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	corecloudprovider "sigs.k8s.io/karpenter/pkg/cloudprovider"

	"github.com/Azure/karpenter-provider-azure/pkg/apis/v1beta1"
	"github.com/Azure/karpenter-provider-azure/pkg/providers/azclient/fleet"
	"github.com/Azure/karpenter-provider-azure/pkg/providers/instance"
	"github.com/Azure/karpenter-provider-azure/pkg/providers/launchtemplate"
)

// stubVMProvider is an in-memory implementation of instance.VMProvider that
// records Delete calls and (optionally) returns errors keyed by VM name.
type stubVMProvider struct {
	mu         sync.Mutex
	listResult []*armcompute.VirtualMachine
	listErr    error
	deleted    []string
	deleteErrs map[string]error
}

var _ instance.VMProvider = (*stubVMProvider)(nil)

func (s *stubVMProvider) List(_ context.Context) ([]*armcompute.VirtualMachine, error) {
	return s.listResult, s.listErr
}

func (s *stubVMProvider) Delete(_ context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deleted = append(s.deleted, name)
	if err, ok := s.deleteErrs[name]; ok {
		return err
	}
	return nil
}

func (s *stubVMProvider) deletedNames() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := append([]string{}, s.deleted...)
	sort.Strings(out)
	return out
}

// Unused interface methods.
func (s *stubVMProvider) BeginCreate(context.Context, *v1beta1.AKSNodeClass, *karpv1.NodeClaim, []*corecloudprovider.InstanceType) (*instance.VirtualMachinePromise, error) {
	return nil, errors.New("not implemented")
}
func (s *stubVMProvider) Get(context.Context, string) (*armcompute.VirtualMachine, error) {
	return nil, errors.New("not implemented")
}
func (s *stubVMProvider) Update(context.Context, string, armcompute.VirtualMachineUpdate) error {
	return errors.New("not implemented")
}
func (s *stubVMProvider) GetNic(context.Context, string, string) (*armnetwork.Interface, error) {
	return nil, errors.New("not implemented")
}
func (s *stubVMProvider) DeleteNic(context.Context, string) error {
	return errors.New("not implemented")
}
func (s *stubVMProvider) ListNics(context.Context) ([]*armnetwork.Interface, error) {
	return nil, errors.New("not implemented")
}
func (s *stubVMProvider) ListFleetVMs(context.Context) ([]*armcompute.VirtualMachine, error) {
	return nil, nil
}

const testClusterName = "test-cluster"

type vmFixtureOpts struct {
	name           string
	hasFleet       bool
	hasNodeClaim   bool
	clusterName    string // "" => default, "-" => omit cluster tag
	age            time.Duration
	nilTimeCreated bool
	nilProperties  bool
	nilName        bool
	nilTags        bool
}

func newFixtureVM(opts vmFixtureOpts) *armcompute.VirtualMachine {
	if opts.name == "" {
		opts.name = "vm"
	}
	var tags map[string]*string
	if !opts.nilTags {
		tags = map[string]*string{}
		cluster := opts.clusterName
		if cluster == "" {
			cluster = testClusterName
		}
		if cluster != "-" {
			tags[launchtemplate.KarpenterManagedTagKey] = lo.ToPtr(cluster)
		}
		if opts.hasFleet {
			tags[fleet.FleetNameTagKey] = lo.ToPtr("fleet-1")
		}
		if opts.hasNodeClaim {
			tags[NodeClaimNameTagKey] = lo.ToPtr("nc-" + opts.name)
		}
	}

	var props *armcompute.VirtualMachineProperties
	if !opts.nilProperties {
		props = &armcompute.VirtualMachineProperties{}
		if !opts.nilTimeCreated {
			props.TimeCreated = lo.ToPtr(time.Now().Add(-opts.age))
		}
	}

	vm := &armcompute.VirtualMachine{
		ID:         lo.ToPtr(fmt.Sprintf("/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/%s", opts.name)),
		Properties: props,
		Tags:       tags,
	}
	if !opts.nilName {
		vm.Name = lo.ToPtr(opts.name)
	}
	return vm
}

func TestFilterOrphans(t *testing.T) {
	c := NewController(nil, testClusterName, 5*time.Minute, 15*time.Minute)
	now := time.Now()

	tests := []struct {
		name      string
		vm        *armcompute.VirtualMachine
		wantKept  bool
		failOnNil bool
	}{
		{name: "nil VM", vm: nil},
		{name: "nil Name", vm: newFixtureVM(vmFixtureOpts{hasFleet: true, age: time.Hour, nilName: true})},
		{name: "nil Tags", vm: newFixtureVM(vmFixtureOpts{hasFleet: true, age: time.Hour, nilTags: true})},
		{name: "missing fleet-name tag", vm: newFixtureVM(vmFixtureOpts{hasFleet: false, age: time.Hour})},
		{name: "has nodeclaim-name tag", vm: newFixtureVM(vmFixtureOpts{hasFleet: true, hasNodeClaim: true, age: time.Hour})},
		{name: "different cluster", vm: newFixtureVM(vmFixtureOpts{hasFleet: true, clusterName: "other", age: time.Hour})},
		{name: "missing cluster tag", vm: newFixtureVM(vmFixtureOpts{hasFleet: true, clusterName: "-", age: time.Hour})},
		{name: "younger than grace", vm: newFixtureVM(vmFixtureOpts{hasFleet: true, age: time.Minute})},
		{name: "exactly at grace boundary", vm: newFixtureVM(vmFixtureOpts{hasFleet: true, age: 15 * time.Minute, name: "edge"})}, // boundary: now-Sub == grace, NOT < grace => orphan
		{name: "nil Properties", vm: newFixtureVM(vmFixtureOpts{hasFleet: true, nilProperties: true})},
		{name: "nil TimeCreated", vm: newFixtureVM(vmFixtureOpts{hasFleet: true, nilTimeCreated: true})},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := []*armcompute.VirtualMachine{tt.vm}
			got := c.filterOrphans(input, now)
			if tt.name == "exactly at grace boundary" {
				if len(got) != 1 {
					t.Fatalf("boundary case: want 1 orphan, got %d", len(got))
				}
				return
			}
			if len(got) != 0 {
				t.Fatalf("want 0 orphans, got %d (%v)", len(got), got)
			}
		})
	}

	t.Run("selects only orphans from a mixed list", func(t *testing.T) {
		input := []*armcompute.VirtualMachine{
			newFixtureVM(vmFixtureOpts{name: "orphan-a", hasFleet: true, age: time.Hour}),
			newFixtureVM(vmFixtureOpts{name: "assigned", hasFleet: true, hasNodeClaim: true, age: time.Hour}),
			newFixtureVM(vmFixtureOpts{name: "orphan-b", hasFleet: true, age: 30 * time.Minute}),
			newFixtureVM(vmFixtureOpts{name: "non-fleet", age: time.Hour}),
			newFixtureVM(vmFixtureOpts{name: "fresh", hasFleet: true, age: time.Minute}),
		}
		orphans := c.filterOrphans(input, now)
		if len(orphans) != 2 {
			t.Fatalf("want 2 orphans, got %d", len(orphans))
		}
		names := []string{lo.FromPtr(orphans[0].Name), lo.FromPtr(orphans[1].Name)}
		sort.Strings(names)
		if names[0] != "orphan-a" || names[1] != "orphan-b" {
			t.Fatalf("wrong orphans selected: %v", names)
		}
	})
}

func TestReconcile_NoVMs_DoesNothing(t *testing.T) {
	stub := &stubVMProvider{}
	c := NewController(stub, testClusterName, 5*time.Minute, 15*time.Minute)

	res, err := c.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.RequeueAfter != 5*time.Minute {
		t.Fatalf("want requeue 5m, got %v", res.RequeueAfter)
	}
	if len(stub.deletedNames()) != 0 {
		t.Fatalf("want no deletes, got %v", stub.deletedNames())
	}
}

func TestReconcile_ListError_Propagates(t *testing.T) {
	stub := &stubVMProvider{listErr: errors.New("boom")}
	c := NewController(stub, testClusterName, 5*time.Minute, 15*time.Minute)

	_, err := c.Reconcile(context.Background())
	if err == nil || err.Error() != "listing VMs: boom" {
		t.Fatalf("want listing error, got %v", err)
	}
}

func TestReconcile_DeletesOrphans(t *testing.T) {
	stub := &stubVMProvider{
		listResult: []*armcompute.VirtualMachine{
			newFixtureVM(vmFixtureOpts{name: "o1", hasFleet: true, age: time.Hour}),
			newFixtureVM(vmFixtureOpts{name: "o2", hasFleet: true, age: time.Hour}),
			newFixtureVM(vmFixtureOpts{name: "keep-assigned", hasFleet: true, hasNodeClaim: true, age: time.Hour}),
			newFixtureVM(vmFixtureOpts{name: "keep-fresh", hasFleet: true, age: time.Minute}),
			newFixtureVM(vmFixtureOpts{name: "keep-other-cluster", hasFleet: true, clusterName: "other", age: time.Hour}),
		},
	}
	c := NewController(stub, testClusterName, 5*time.Minute, 15*time.Minute)

	if _, err := c.Reconcile(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got := stub.deletedNames()
	if len(got) != 2 || got[0] != "o1" || got[1] != "o2" {
		t.Fatalf("want [o1 o2], got %v", got)
	}
}

func TestReconcile_SwallowsNotFound(t *testing.T) {
	stub := &stubVMProvider{
		listResult: []*armcompute.VirtualMachine{
			newFixtureVM(vmFixtureOpts{name: "o1", hasFleet: true, age: time.Hour}),
		},
		deleteErrs: map[string]error{
			"o1": corecloudprovider.NewNodeClaimNotFoundError(errors.New("already gone")),
		},
	}
	c := NewController(stub, testClusterName, 5*time.Minute, 15*time.Minute)

	res, err := c.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile should swallow NotFound, got: %v", err)
	}
	if res.RequeueAfter != 5*time.Minute {
		t.Fatalf("want requeue 5m, got %v", res.RequeueAfter)
	}
}

func TestReconcile_ReturnsDeleteError(t *testing.T) {
	stub := &stubVMProvider{
		listResult: []*armcompute.VirtualMachine{
			newFixtureVM(vmFixtureOpts{name: "o1", hasFleet: true, age: time.Hour}),
		},
		deleteErrs: map[string]error{
			"o1": errors.New("rate limited"),
		},
	}
	c := NewController(stub, testClusterName, 5*time.Minute, 15*time.Minute)

	_, err := c.Reconcile(context.Background())
	if err == nil {
		t.Fatalf("expected error to propagate, got nil")
	}
}

func TestNewController_StoresFields(t *testing.T) {
	stub := &stubVMProvider{}
	c := NewController(stub, "my-cluster", 7*time.Minute, 20*time.Minute)
	if c.clusterName != "my-cluster" {
		t.Fatalf("clusterName not stored")
	}
	if c.interval != 7*time.Minute {
		t.Fatalf("interval not stored")
	}
	if c.grace != 20*time.Minute {
		t.Fatalf("grace not stored")
	}
	if c.vmProvider == nil {
		t.Fatalf("vmProvider not stored")
	}
}
