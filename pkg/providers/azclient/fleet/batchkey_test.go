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
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/computefleet/armcomputefleet/v2"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"

	"github.com/Azure/karpenter-provider-azure/pkg/providers/launchtemplate"
)

func batchKeyFleetBody(
	capacityType string,
	skus []string,
	zones []string,
	launchTemplate *launchtemplate.Template,
) *armcomputefleet.Fleet {
	return BuildFleetBody(FleetBodyOptions{
		CapacityType:    capacityType,
		AcceptableSKUs:  skus,
		AcceptableZones: zones,
		LaunchTemplate:  launchTemplate,
		SSHPublicKey:    "ssh-rsa AAAAB3...",
		AdminUsername:   "azureuser",
		NodeIdentities:  []string{"/subscriptions/sub/resourceGroups/rg/providers/Microsoft.ManagedIdentity/userAssignedIdentities/id1"},
		NSGID:           "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Network/networkSecurityGroups/nsg1",
		Location:        "eastus",
		TargetCapacity:  1,
	})
}

func batchKeyLaunchTemplate() *launchtemplate.Template {
	return &launchtemplate.Template{
		ImageID:              "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/galleries/gallery/images/image/versions/1.0.0",
		SubnetID:             "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Network/virtualNetworks/vnet/subnets/subnet1",
		ScriptlessCustomData: "Y3VzdG9tZGF0YQ==",
		StorageProfileSizeGB: 128,
	}
}

func determineTestBatchKey(fleetBody *armcomputefleet.Fleet) (string, error) {
	return DetermineBatchKey("default", fleetBody)
}

func TestDetermineBatchKey_Deterministic(t *testing.T) {
	fleetBody := batchKeyFleetBody(
		karpv1.CapacityTypeOnDemand,
		[]string{"Standard_D4s_v3", "Standard_D8s_v3"},
		[]string{"2", "1"},
		batchKeyLaunchTemplate(),
	)

	key1, err := determineTestBatchKey(fleetBody)
	require.NoError(t, err)
	key2, err := determineTestBatchKey(fleetBody)
	require.NoError(t, err)

	assert.Equal(t, key1, key2)
}

func TestDetermineBatchKey_Format(t *testing.T) {
	key, err := determineTestBatchKey(batchKeyFleetBody(
		karpv1.CapacityTypeOnDemand,
		[]string{"Standard_D4s_v3", "Standard_D8s_v3"},
		[]string{"2", "1"},
		batchKeyLaunchTemplate(),
	))
	require.NoError(t, err)

	assert.Regexp(t, `^default/[0-9a-f]{16}$`, key)
}

func TestDetermineBatchKey_DifferentCapacityType(t *testing.T) {
	onDemand := batchKeyFleetBody(karpv1.CapacityTypeOnDemand, []string{"Standard_D4s_v3"}, []string{"1"}, batchKeyLaunchTemplate())
	spot := batchKeyFleetBody(karpv1.CapacityTypeSpot, []string{"Standard_D4s_v3"}, []string{"1"}, batchKeyLaunchTemplate())

	onDemandKey, err := determineTestBatchKey(onDemand)
	require.NoError(t, err)
	spotKey, err := determineTestBatchKey(spot)
	require.NoError(t, err)

	assert.NotEqual(t, onDemandKey, spotKey)
}

func TestDetermineBatchKey_DifferentSKUs(t *testing.T) {
	first := batchKeyFleetBody(karpv1.CapacityTypeOnDemand, []string{"Standard_D4s_v3"}, []string{"1"}, batchKeyLaunchTemplate())
	second := batchKeyFleetBody(karpv1.CapacityTypeOnDemand, []string{"Standard_D8s_v3"}, []string{"1"}, batchKeyLaunchTemplate())

	firstKey, err := determineTestBatchKey(first)
	require.NoError(t, err)
	secondKey, err := determineTestBatchKey(second)
	require.NoError(t, err)

	assert.NotEqual(t, firstKey, secondKey)
}

func TestDetermineBatchKey_SKUOrderIrrelevant(t *testing.T) {
	first := batchKeyFleetBody(
		karpv1.CapacityTypeOnDemand,
		[]string{"Standard_D8s_v3", "Standard_D4s_v3"},
		[]string{"1"},
		batchKeyLaunchTemplate(),
	)
	second := batchKeyFleetBody(
		karpv1.CapacityTypeOnDemand,
		[]string{"Standard_D4s_v3", "Standard_D8s_v3"},
		[]string{"1"},
		batchKeyLaunchTemplate(),
	)

	firstKey, err := determineTestBatchKey(first)
	require.NoError(t, err)
	secondKey, err := determineTestBatchKey(second)
	require.NoError(t, err)

	assert.Equal(t, firstKey, secondKey)
}

func TestDetermineBatchKey_ZoneOrderIrrelevant(t *testing.T) {
	first := batchKeyFleetBody(
		karpv1.CapacityTypeOnDemand,
		[]string{"Standard_D4s_v3"},
		[]string{"3", "1", "2"},
		batchKeyLaunchTemplate(),
	)
	second := batchKeyFleetBody(
		karpv1.CapacityTypeOnDemand,
		[]string{"Standard_D4s_v3"},
		[]string{"1", "2", "3"},
		batchKeyLaunchTemplate(),
	)

	firstKey, err := determineTestBatchKey(first)
	require.NoError(t, err)
	secondKey, err := determineTestBatchKey(second)
	require.NoError(t, err)

	assert.Equal(t, firstKey, secondKey)
}

func TestDetermineBatchKey_BackendPoolOrderIrrelevant(t *testing.T) {
	first := batchKeyFleetBody(
		karpv1.CapacityTypeOnDemand,
		[]string{"Standard_D4s_v3"},
		[]string{"1"},
		batchKeyLaunchTemplate(),
	)
	first.Properties.ComputeProfile.BaseVirtualMachineProfile.NetworkProfile =
		buildNetworkProfile("subnet", "nsg", []string{"pool-b", "pool-a"})

	second := batchKeyFleetBody(
		karpv1.CapacityTypeOnDemand,
		[]string{"Standard_D4s_v3"},
		[]string{"1"},
		batchKeyLaunchTemplate(),
	)
	second.Properties.ComputeProfile.BaseVirtualMachineProfile.NetworkProfile =
		buildNetworkProfile("subnet", "nsg", []string{"pool-a", "pool-b"})

	firstKey, err := determineTestBatchKey(first)
	require.NoError(t, err)
	secondKey, err := determineTestBatchKey(second)
	require.NoError(t, err)

	assert.Equal(t, firstKey, secondKey)
}

func TestDetermineBatchKey_EncryptionAtHostAffectsKey(t *testing.T) {
	withoutEncryption := batchKeyFleetBody(
		karpv1.CapacityTypeOnDemand,
		[]string{"Standard_D4s_v3"},
		[]string{"1"},
		batchKeyLaunchTemplate(),
	)
	encryptedLaunchTemplate := batchKeyLaunchTemplate()
	encryptedLaunchTemplate.EncryptionAtHost = lo.ToPtr(true)
	withEncryption := batchKeyFleetBody(
		karpv1.CapacityTypeOnDemand,
		[]string{"Standard_D4s_v3"},
		[]string{"1"},
		encryptedLaunchTemplate,
	)

	withoutEncryptionKey, err := determineTestBatchKey(withoutEncryption)
	require.NoError(t, err)
	withEncryptionKey, err := determineTestBatchKey(withEncryption)
	require.NoError(t, err)

	assert.NotEqual(t, withoutEncryptionKey, withEncryptionKey)
}

func TestDetermineBatchKey_NilFleetBody(t *testing.T) {
	_, err := DetermineBatchKey("pool", nil)
	assert.Error(t, err)
}
