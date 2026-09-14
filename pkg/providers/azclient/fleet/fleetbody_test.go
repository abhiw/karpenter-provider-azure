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
	"encoding/json"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/computefleet/armcomputefleet/v2"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"

	"github.com/Azure/karpenter-provider-azure/pkg/providers/launchtemplate"
)

const (
	testSSHPublicKey  = "ssh-rsa AAAA..."
	testAdminUsername = "azureuser"
	testNSGID         = "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Network/networkSecurityGroups/nsg"
	testLocation      = "eastus"
)

func defaultLaunchTemplate() *launchtemplate.Template {
	return &launchtemplate.Template{
		ScriptlessCustomData: "Y3VzdG9tZGF0YQ==",
		ImageID:              "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/galleries/g/images/i/versions/v",
		SubnetID:             "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Network/virtualNetworks/vnet/subnets/sn",
		StorageProfileSizeGB: 128,
	}
}

func defaultTags() map[string]*string {
	return map[string]*string{
		"karpenter.azure.com_managed-by":     lo.ToPtr("karpenter"),
		"karpenter.azure.com_batch-key-hash": lo.ToPtr("abcdef0123456789"),
	}
}

func defaultFleetBody(targetCapacity int32, tags map[string]*string) *armcomputefleet.Fleet {
	return BuildFleetBody(FleetBodyOptions{
		CapacityType:    karpv1.CapacityTypeOnDemand,
		AcceptableSKUs:  []string{"Standard_D4s_v3", "Standard_D8s_v3"},
		AcceptableZones: []string{"1", "2", "3"},
		LaunchTemplate:  defaultLaunchTemplate(),
		SSHPublicKey:    testSSHPublicKey,
		AdminUsername:   testAdminUsername,
		NSGID:           testNSGID,
		Location:        testLocation,
		TargetCapacity:  targetCapacity,
		Tags:            tags,
	})
}

func TestBuildFleetProperties_SpotCapacityType(t *testing.T) {
	properties := buildFleetProperties(karpv1.CapacityTypeSpot, nil, nil, 5)

	require.NotNil(t, properties.SpotPriorityProfile)
	assert.Nil(t, properties.RegularPriorityProfile)
	assert.Equal(t, lo.ToPtr(armcomputefleet.SpotAllocationStrategyPriceCapacityOptimized), properties.SpotPriorityProfile.AllocationStrategy)
	assert.Equal(t, lo.ToPtr(false), properties.SpotPriorityProfile.Maintain)
	assert.Equal(t, lo.ToPtr(armcomputefleet.EvictionPolicyDelete), properties.SpotPriorityProfile.EvictionPolicy)
	assert.Equal(t, lo.ToPtr(float32(-1)), properties.SpotPriorityProfile.MaxPricePerVM)
}

func TestBuildFleetProperties_OnDemandCapacityType(t *testing.T) {
	properties := buildFleetProperties(karpv1.CapacityTypeOnDemand, nil, nil, 3)

	require.NotNil(t, properties.RegularPriorityProfile)
	assert.Nil(t, properties.SpotPriorityProfile)
	assert.Equal(t, lo.ToPtr(armcomputefleet.RegularPriorityAllocationStrategyLowestPrice), properties.RegularPriorityProfile.AllocationStrategy)
}

func TestBuildFleetProperties_UnknownCapacityTypeDefaultsToRegular(t *testing.T) {
	properties := buildFleetProperties("garbage-type", nil, nil, 2)

	require.NotNil(t, properties.RegularPriorityProfile)
	assert.Nil(t, properties.SpotPriorityProfile)
	assert.Equal(t, int32(2), *properties.RegularPriorityProfile.Capacity)
}

func TestBuildFleetProperties_Capacity(t *testing.T) {
	properties := buildFleetProperties(karpv1.CapacityTypeOnDemand, nil, nil, 7)

	require.NotNil(t, properties.RegularPriorityProfile.Capacity)
	assert.Equal(t, int32(7), *properties.RegularPriorityProfile.Capacity)
}

func TestBuildVMSizesProfile(t *testing.T) {
	profiles := buildVMSizesProfile([]string{"Standard_D8s_v3", "Standard_D2s_v3", "Standard_D4s_v3"})

	require.Len(t, profiles, 3)
	expected := []string{"Standard_D2s_v3", "Standard_D4s_v3", "Standard_D8s_v3"}
	for i, sku := range expected {
		assert.Equal(t, sku, *profiles[i].Name)
		assert.Nil(t, profiles[i].Rank)
	}
}

func TestBuildFleetBody_TagsCarryThrough(t *testing.T) {
	tags := map[string]*string{
		"karpenter.azure.com_managed-by":     lo.ToPtr("karpenter"),
		"karpenter.azure.com_batch-key-hash": lo.ToPtr("deadbeef12345678"),
		"env":                                lo.ToPtr("test"),
	}

	fleetBody := defaultFleetBody(1, tags)

	assert.Equal(t, tags, fleetBody.Tags)
}

func TestBuildFleetBody_LocationAndZones(t *testing.T) {
	fleetBody := BuildFleetBody(FleetBodyOptions{
		CapacityType:    karpv1.CapacityTypeOnDemand,
		AcceptableSKUs:  []string{"Standard_D4s_v3"},
		AcceptableZones: []string{"3", "1", "2"},
		LaunchTemplate:  defaultLaunchTemplate(),
		SSHPublicKey:    testSSHPublicKey,
		AdminUsername:   testAdminUsername,
		NSGID:           testNSGID,
		Location:        "westus2",
		TargetCapacity:  1,
		Tags:            defaultTags(),
	})

	assert.Equal(t, lo.ToPtr("westus2"), fleetBody.Location)
	require.Len(t, fleetBody.Zones, 3)
	assert.Equal(t, "1", *fleetBody.Zones[0])
	assert.Equal(t, "2", *fleetBody.Zones[1])
	assert.Equal(t, "3", *fleetBody.Zones[2])
}

func TestBuildComputeProfile_EncryptionAtHost(t *testing.T) {
	launchTemplate := defaultLaunchTemplate()
	launchTemplate.EncryptionAtHost = lo.ToPtr(true)

	profile := buildComputeProfile(FleetBodyOptions{
		LaunchTemplate: launchTemplate,
		SSHPublicKey:   testSSHPublicKey,
		AdminUsername:  testAdminUsername,
		NSGID:          testNSGID,
	})

	require.NotNil(t, profile.BaseVirtualMachineProfile.SecurityProfile)
	assert.Equal(t, lo.ToPtr(true), profile.BaseVirtualMachineProfile.SecurityProfile.EncryptionAtHost)
}

func TestBuildComputeProfile_EncryptionAtHostDisabled(t *testing.T) {
	launchTemplate := defaultLaunchTemplate()
	launchTemplate.EncryptionAtHost = lo.ToPtr(false)

	profile := buildComputeProfile(FleetBodyOptions{
		LaunchTemplate: launchTemplate,
		SSHPublicKey:   testSSHPublicKey,
		AdminUsername:  testAdminUsername,
		NSGID:          testNSGID,
	})

	assert.Nil(t, profile.BaseVirtualMachineProfile.SecurityProfile)
}

func TestBuildIdentity(t *testing.T) {
	identity := buildIdentity([]string{"/sub/rg/identity2", "/sub/rg/identity1"})

	require.NotNil(t, identity)
	assert.Equal(t, lo.ToPtr(armcomputefleet.ManagedServiceIdentityTypeUserAssigned), identity.Type)
	assert.Len(t, identity.UserAssignedIdentities, 2)
	assert.Contains(t, identity.UserAssignedIdentities, "/sub/rg/identity1")
	assert.Contains(t, identity.UserAssignedIdentities, "/sub/rg/identity2")
}

func TestBuildIdentity_Empty(t *testing.T) {
	assert.Nil(t, buildIdentity(nil))
}

func TestBuildNetworkProfile(t *testing.T) {
	launchTemplate := defaultLaunchTemplate()
	profile := buildNetworkProfile(launchTemplate.SubnetID, testNSGID, nil)

	require.Len(t, profile.NetworkInterfaceConfigurations, 1)
	nic := profile.NetworkInterfaceConfigurations[0]
	assert.Equal(t, lo.ToPtr(nicConfigName), nic.Name)
	require.NotNil(t, nic.Properties)
	assert.Equal(t, lo.ToPtr(true), nic.Properties.Primary)
	assert.Equal(t, lo.ToPtr(true), nic.Properties.EnableAcceleratedNetworking)
	require.Len(t, nic.Properties.IPConfigurations, 1)
	ipConfig := nic.Properties.IPConfigurations[0]
	assert.Equal(t, lo.ToPtr(ipConfigName), ipConfig.Name)
	assert.Equal(t, lo.ToPtr(launchTemplate.SubnetID), ipConfig.Properties.Subnet.ID)
	require.NotNil(t, nic.Properties.NetworkSecurityGroup)
	assert.Equal(t, lo.ToPtr(testNSGID), nic.Properties.NetworkSecurityGroup.ID)
}

func TestBuildNetworkProfile_NoNSG(t *testing.T) {
	profile := buildNetworkProfile(defaultLaunchTemplate().SubnetID, "", nil)

	assert.Nil(t, profile.NetworkInterfaceConfigurations[0].Properties.NetworkSecurityGroup)
}

func TestBuildNetworkProfile_LoadBalancerPools(t *testing.T) {
	profile := buildNetworkProfile(
		defaultLaunchTemplate().SubnetID,
		testNSGID,
		[]string{"/sub/rg/lb/pool2", "/sub/rg/lb/pool1"},
	)

	ipConfig := profile.NetworkInterfaceConfigurations[0].Properties.IPConfigurations[0]
	require.Len(t, ipConfig.Properties.LoadBalancerBackendAddressPools, 2)
	assert.Equal(t, lo.ToPtr("/sub/rg/lb/pool1"), ipConfig.Properties.LoadBalancerBackendAddressPools[0].ID)
	assert.Equal(t, lo.ToPtr("/sub/rg/lb/pool2"), ipConfig.Properties.LoadBalancerBackendAddressPools[1].ID)
}

func TestBuildStorageProfile_CommunityGalleryImage(t *testing.T) {
	launchTemplate := defaultLaunchTemplate()

	profile := buildStorageProfile(launchTemplate, "")

	assert.Nil(t, profile.ImageReference.ID)
	assert.Equal(t, lo.ToPtr(launchTemplate.ImageID), profile.ImageReference.CommunityGalleryImageID)
}

func TestBuildStorageProfile_EphemeralDisk(t *testing.T) {
	launchTemplate := defaultLaunchTemplate()
	launchTemplate.StorageProfileIsEphemeral = true
	launchTemplate.StorageProfilePlacement = armcompute.DiffDiskPlacementCacheDisk

	profile := buildStorageProfile(launchTemplate, "")

	require.NotNil(t, profile.OSDisk.DiffDiskSettings)
	assert.Equal(t, lo.ToPtr(armcomputefleet.DiffDiskOptionsLocal), profile.OSDisk.DiffDiskSettings.Option)
	assert.Equal(t, lo.ToPtr(armcomputefleet.DiffDiskPlacementCacheDisk), profile.OSDisk.DiffDiskSettings.Placement)
	assert.Equal(t, lo.ToPtr(armcomputefleet.CachingTypesReadOnly), profile.OSDisk.Caching)
}

func TestBuildStorageProfile_ManagedDisk(t *testing.T) {
	profile := buildStorageProfile(defaultLaunchTemplate(), "")

	assert.Nil(t, profile.OSDisk.DiffDiskSettings)
	assert.Nil(t, profile.OSDisk.Caching)
}

func TestBuildStorageProfile_DiskEncryptionSet(t *testing.T) {
	diskEncryptionSetID := "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/diskEncryptionSets/des"

	profile := buildStorageProfile(defaultLaunchTemplate(), diskEncryptionSetID)

	require.NotNil(t, profile.OSDisk.ManagedDisk)
	require.NotNil(t, profile.OSDisk.ManagedDisk.DiskEncryptionSet)
	assert.Equal(t, diskEncryptionSetID, *profile.OSDisk.ManagedDisk.DiskEncryptionSet.ID)
}

func TestExtensionsToProfile_Empty(t *testing.T) {
	assert.Nil(t, extensionsToProfile(nil))
}

func TestExtensionsToProfile(t *testing.T) {
	profile := extensionsToProfile([]*armcompute.VirtualMachineExtension{{
		Name: lo.ToPtr("CSE"),
		Properties: &armcompute.VirtualMachineExtensionProperties{
			Publisher:          lo.ToPtr("Microsoft.Azure.Extensions"),
			Type:               lo.ToPtr("CustomScript"),
			TypeHandlerVersion: lo.ToPtr("2.1"),
			Settings:           map[string]any{"commandToExecute": "echo hello"},
		},
	}})

	require.NotNil(t, profile)
	require.Len(t, profile.Extensions, 1)
	assert.Equal(t, "echo hello", profile.Extensions[0].Properties.Settings["commandToExecute"])
}

func TestExtensionsToProfile_PointerToMap(t *testing.T) {
	profile := extensionsToProfile([]*armcompute.VirtualMachineExtension{{
		Name: lo.ToPtr("cse-agent-karpenter"),
		Properties: &armcompute.VirtualMachineExtensionProperties{
			Publisher:               lo.ToPtr("Microsoft.Azure.Extensions"),
			Type:                    lo.ToPtr("CustomScript"),
			TypeHandlerVersion:      lo.ToPtr("2.0"),
			AutoUpgradeMinorVersion: lo.ToPtr(true),
			Settings:                &map[string]interface{}{},
			ProtectedSettings: &map[string]interface{}{
				"commandToExecute": "echo bootstrap",
			},
		},
	}})

	require.NotNil(t, profile)
	extension := profile.Extensions[0]
	assert.Equal(t, "cse-agent-karpenter", *extension.Name)
	assert.Equal(t, "Microsoft.Azure.Extensions", *extension.Properties.Publisher)
	assert.Equal(t, "CustomScript", *extension.Properties.Type)
	assert.Equal(t, "echo bootstrap", extension.Properties.ProtectedSettings["commandToExecute"])
}

func TestBuildFleetBody_RoundTripMarshal(t *testing.T) {
	launchTemplate := defaultLaunchTemplate()
	launchTemplate.StorageProfileIsEphemeral = true
	launchTemplate.StorageProfilePlacement = armcompute.DiffDiskPlacementCacheDisk
	launchTemplate.EncryptionAtHost = lo.ToPtr(true)
	diskEncryptionSetID := "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/diskEncryptionSets/des"

	original := BuildFleetBody(FleetBodyOptions{
		CapacityType:        karpv1.CapacityTypeSpot,
		AcceptableSKUs:      []string{"Standard_D4s_v3", "Standard_D8s_v3"},
		AcceptableZones:     []string{"1", "2", "3"},
		LaunchTemplate:      launchTemplate,
		SSHPublicKey:        testSSHPublicKey,
		AdminUsername:       testAdminUsername,
		NodeIdentities:      []string{"/sub/rg/id1", "/sub/rg/id2"},
		DiskEncryptionSetID: diskEncryptionSetID,
		NSGID:               testNSGID,
		LBBackendPools:      []string{"/sub/rg/lb/pool1"},
		Location:            "eastus2",
		TargetCapacity:      5,
		Tags:                defaultTags(),
	})

	data, err := json.Marshal(original)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	var roundTripped armcomputefleet.Fleet
	require.NoError(t, json.Unmarshal(data, &roundTripped))

	assert.Equal(t, *original.Location, *roundTripped.Location)
	assert.Equal(t, len(original.Zones), len(roundTripped.Zones))
	assert.Equal(t, len(original.Tags), len(roundTripped.Tags))
	require.NotNil(t, roundTripped.Identity)
	assert.Equal(t, *original.Identity.Type, *roundTripped.Identity.Type)
	assert.Equal(t, len(original.Identity.UserAssignedIdentities), len(roundTripped.Identity.UserAssignedIdentities))
	require.NotNil(t, roundTripped.Properties.SpotPriorityProfile)
	assert.Equal(t, *original.Properties.SpotPriorityProfile.Capacity, *roundTripped.Properties.SpotPriorityProfile.Capacity)
	assert.Equal(t, len(original.Properties.VMSizesProfile), len(roundTripped.Properties.VMSizesProfile))

	osDisk := roundTripped.Properties.ComputeProfile.BaseVirtualMachineProfile.StorageProfile.OSDisk
	require.NotNil(t, osDisk.DiffDiskSettings)
	require.NotNil(t, osDisk.ManagedDisk)
	assert.Equal(t, diskEncryptionSetID, *osDisk.ManagedDisk.DiskEncryptionSet.ID)
	require.NotNil(t, roundTripped.Properties.ComputeProfile.BaseVirtualMachineProfile.SecurityProfile)

	ipConfig := roundTripped.Properties.ComputeProfile.BaseVirtualMachineProfile.NetworkProfile.
		NetworkInterfaceConfigurations[0].Properties.IPConfigurations[0]
	require.Len(t, ipConfig.Properties.LoadBalancerBackendAddressPools, 1)
}
