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
	"fmt"
	"sort"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/computefleet/armcomputefleet/v2"
	"github.com/samber/lo"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"

	"github.com/Azure/karpenter-provider-azure/pkg/providers/launchtemplate"
)

const (
	vmNamePrefix       = "aks"
	computerNamePrefix = "aks-"
	nicConfigName      = "nic"
	ipConfigName       = "ipconfig1"
	sshKeyPathTemplate = "/home/%s/.ssh/authorized_keys"
)

// FleetBodyOptions contains the resolved inputs used to construct an Azure Compute Fleet body.
type FleetBodyOptions struct {
	CapacityType        string
	AcceptableSKUs      []string
	AcceptableZones     []string
	LaunchTemplate      *launchtemplate.Template
	SSHPublicKey        string
	AdminUsername       string
	NodeIdentities      []string
	DiskEncryptionSetID string
	NSGID               string
	LBBackendPools      []string
	Location            string
	Extensions          []*armcompute.VirtualMachineExtension
	TargetCapacity      int32
	Tags                map[string]*string
}

// BuildFleetBody constructs the armcomputefleet.Fleet body from resolved provisioning inputs.
// Slices (SKUs, zones) are sorted internally for deterministic JSON serialization.
func BuildFleetBody(options FleetBodyOptions) *armcomputefleet.Fleet {
	return &armcomputefleet.Fleet{
		Location: lo.ToPtr(options.Location),
		Tags:     options.Tags,
		Zones:    buildZones(options.AcceptableZones),
		Identity: buildIdentity(options.NodeIdentities),
		Properties: buildFleetProperties(
			options.CapacityType,
			options.AcceptableSKUs,
			buildComputeProfile(options),
			options.TargetCapacity,
		),
	}
}

// buildZones converts sorted zone strings to the []*string shape required by the SDK.
// Returns nil for an empty/nil zone slice (regional Fleet — no zone pinning).
func buildZones(zones []string) []*string {
	if len(zones) == 0 {
		return nil
	}
	sortedZones := append([]string(nil), zones...)
	sort.Strings(sortedZones)
	return lo.Map(sortedZones, func(z string, _ int) *string {
		return lo.ToPtr(z)
	})
}

// buildIdentity constructs the ManagedServiceIdentity from the NodeIdentities slice.
// Returns nil if no identities are configured.
func buildIdentity(identities []string) *armcomputefleet.ManagedServiceIdentity {
	if len(identities) == 0 {
		return nil
	}
	sortedIdentities := append([]string(nil), identities...)
	sort.Strings(sortedIdentities)

	m := make(map[string]*armcomputefleet.UserAssignedIdentity, len(sortedIdentities))
	for _, id := range sortedIdentities {
		if id == "" {
			continue
		}
		m[id] = &armcomputefleet.UserAssignedIdentity{}
	}
	if len(m) == 0 {
		return nil
	}
	return &armcomputefleet.ManagedServiceIdentity{
		Type:                   lo.ToPtr(armcomputefleet.ManagedServiceIdentityTypeUserAssigned),
		UserAssignedIdentities: m,
	}
}

// buildFleetProperties assembles the core FleetProperties with the appropriate priority profile.
func buildFleetProperties(
	capacityType string,
	acceptableSKUs []string,
	computeProfile *armcomputefleet.ComputeProfile,
	targetCapacity int32,
) *armcomputefleet.FleetProperties {
	props := &armcomputefleet.FleetProperties{
		VMSizesProfile: buildVMSizesProfile(acceptableSKUs),
		ComputeProfile: computeProfile,
		Mode:           lo.ToPtr(armcomputefleet.FleetModeLaunch),
		VMNamePrefix:   lo.ToPtr(vmNamePrefix),
	}

	switch capacityType {
	case karpv1.CapacityTypeSpot:
		props.SpotPriorityProfile = buildSpotProfile(targetCapacity)
	case karpv1.CapacityTypeOnDemand:
		props.RegularPriorityProfile = buildRegularProfile(targetCapacity)
	default:
		props.RegularPriorityProfile = buildRegularProfile(targetCapacity)
	}

	return props
}

// buildVMSizesProfile creates one VMSizeProfile entry per candidate SKU, sorted.
func buildVMSizesProfile(skus []string) []*armcomputefleet.VMSizeProfile {
	sortedSKUs := append([]string(nil), skus...)
	sort.Strings(sortedSKUs)

	out := make([]*armcomputefleet.VMSizeProfile, 0, len(sortedSKUs))
	for _, s := range sortedSKUs {
		out = append(out, &armcomputefleet.VMSizeProfile{Name: lo.ToPtr(s)})
	}
	return out
}

// buildSpotProfile constructs the spot priority profile.
func buildSpotProfile(capacity int32) *armcomputefleet.SpotPriorityProfile {
	return &armcomputefleet.SpotPriorityProfile{
		Capacity:           lo.ToPtr(capacity),
		AllocationStrategy: lo.ToPtr(armcomputefleet.SpotAllocationStrategyPriceCapacityOptimized),
		EvictionPolicy:     lo.ToPtr(armcomputefleet.EvictionPolicyDelete),
		Maintain:           lo.ToPtr(false),
		MaxPricePerVM:      lo.ToPtr(float32(-1)),
	}
}

// buildRegularProfile constructs the on-demand (regular) priority profile.
func buildRegularProfile(capacity int32) *armcomputefleet.RegularPriorityProfile {
	return &armcomputefleet.RegularPriorityProfile{
		Capacity:           lo.ToPtr(capacity),
		AllocationStrategy: lo.ToPtr(armcomputefleet.RegularPriorityAllocationStrategyLowestPrice),
		MinCapacity:        lo.ToPtr(int32(0)),
	}
}

// buildComputeProfile constructs the BaseVirtualMachineProfile containing OS, storage,
// network, security, and extension profiles.
func buildComputeProfile(options FleetBodyOptions) *armcomputefleet.ComputeProfile {
	var encryptionAtHost *bool
	subnetID := ""
	if options.LaunchTemplate != nil {
		encryptionAtHost = options.LaunchTemplate.EncryptionAtHost
		subnetID = options.LaunchTemplate.SubnetID
	}

	baseProfile := &armcomputefleet.BaseVirtualMachineProfile{
		OSProfile:        buildOSProfile(options.LaunchTemplate, options.AdminUsername, options.SSHPublicKey),
		StorageProfile:   buildStorageProfile(options.LaunchTemplate, options.DiskEncryptionSetID),
		NetworkProfile:   buildNetworkProfile(subnetID, options.NSGID, options.LBBackendPools),
		SecurityProfile:  buildSecurityProfile(encryptionAtHost),
		ExtensionProfile: extensionsToProfile(options.Extensions),
	}

	return &armcomputefleet.ComputeProfile{
		BaseVirtualMachineProfile: baseProfile,
	}
}

// buildOSProfile constructs the Linux OS profile with SSH key and custom data.
func buildOSProfile(
	launchTemplate *launchtemplate.Template,
	adminUsername string,
	sshPublicKey string,
) *armcomputefleet.VirtualMachineScaleSetOSProfile {
	sshPath := fmt.Sprintf(sshKeyPathTemplate, adminUsername)

	profile := &armcomputefleet.VirtualMachineScaleSetOSProfile{
		AdminUsername:      lo.ToPtr(adminUsername),
		ComputerNamePrefix: lo.ToPtr(computerNamePrefix),
		LinuxConfiguration: &armcomputefleet.LinuxConfiguration{
			DisablePasswordAuthentication: lo.ToPtr(true),
			SSH: &armcomputefleet.SSHConfiguration{
				PublicKeys: []*armcomputefleet.SSHPublicKey{{
					KeyData: lo.ToPtr(sshPublicKey),
					Path:    lo.ToPtr(sshPath),
				}},
			},
		},
	}

	if launchTemplate != nil {
		customData := launchTemplate.ScriptlessCustomData
		if launchTemplate.CustomScriptsCustomData != "" {
			customData = launchTemplate.CustomScriptsCustomData
		}
		if customData != "" {
			profile.CustomData = lo.ToPtr(customData)
		}
	}
	return profile
}

// buildStorageProfile constructs the OS disk and image reference.
func buildStorageProfile(
	launchTemplate *launchtemplate.Template,
	diskEncryptionSetID string,
) *armcomputefleet.VirtualMachineScaleSetStorageProfile {
	imageRef := &armcomputefleet.ImageReference{}
	var imageID string
	var sizeGB int32
	var isEphemeral bool
	var placement armcompute.DiffDiskPlacement

	if launchTemplate != nil {
		imageID = launchTemplate.ImageID
		sizeGB = launchTemplate.StorageProfileSizeGB
		isEphemeral = launchTemplate.StorageProfileIsEphemeral
		placement = launchTemplate.StorageProfilePlacement
	}

	imageRef.CommunityGalleryImageID = lo.ToPtr(imageID)

	osDisk := &armcomputefleet.VirtualMachineScaleSetOSDisk{
		CreateOption: lo.ToPtr(armcomputefleet.DiskCreateOptionTypesFromImage),
		DiskSizeGB:   lo.ToPtr(sizeGB),
		OSType:       lo.ToPtr(armcomputefleet.OperatingSystemTypesLinux),
	}

	// Ephemeral disk settings
	if isEphemeral || placement != "" {
		diffDiskPlacement := armcomputefleet.DiffDiskPlacement(placement)
		if diffDiskPlacement == "" {
			diffDiskPlacement = armcomputefleet.DiffDiskPlacementCacheDisk
		}
		osDisk.DiffDiskSettings = &armcomputefleet.DiffDiskSettings{
			Option:    lo.ToPtr(armcomputefleet.DiffDiskOptionsLocal),
			Placement: lo.ToPtr(diffDiskPlacement),
		}
		osDisk.Caching = lo.ToPtr(armcomputefleet.CachingTypesReadOnly)
	}

	// Disk encryption set
	if diskEncryptionSetID != "" {
		osDisk.ManagedDisk = &armcomputefleet.VirtualMachineScaleSetManagedDiskParameters{
			StorageAccountType: lo.ToPtr(armcomputefleet.StorageAccountTypesStandardLRS),
			DiskEncryptionSet: &armcomputefleet.DiskEncryptionSetParameters{
				ID: lo.ToPtr(diskEncryptionSetID),
			},
		}
	}

	return &armcomputefleet.VirtualMachineScaleSetStorageProfile{
		ImageReference: imageRef,
		OSDisk:         osDisk,
	}
}

// buildNetworkProfile constructs the network profile with subnet, NSG, and LB backend pools.
func buildNetworkProfile(subnetID, nsgID string, lbBackendPools []string) *armcomputefleet.VirtualMachineScaleSetNetworkProfile {
	nicProps := &armcomputefleet.VirtualMachineScaleSetNetworkConfigurationProperties{
		Primary:                     lo.ToPtr(true),
		EnableAcceleratedNetworking: lo.ToPtr(true),
		EnableIPForwarding:          lo.ToPtr(false),
		DeleteOption:                lo.ToPtr(armcomputefleet.DeleteOptionsDelete),
		IPConfigurations: []*armcomputefleet.VirtualMachineScaleSetIPConfiguration{{
			Name: lo.ToPtr(ipConfigName),
			Properties: &armcomputefleet.VirtualMachineScaleSetIPConfigurationProperties{
				Primary:                         lo.ToPtr(true),
				Subnet:                          &armcomputefleet.APIEntityReference{ID: lo.ToPtr(subnetID)},
				LoadBalancerBackendAddressPools: buildPoolRefs(lbBackendPools),
			},
		}},
	}
	if nsgID != "" {
		nicProps.NetworkSecurityGroup = &armcomputefleet.SubResource{ID: lo.ToPtr(nsgID)}
	}
	return &armcomputefleet.VirtualMachineScaleSetNetworkProfile{
		NetworkInterfaceConfigurations: []*armcomputefleet.VirtualMachineScaleSetNetworkConfiguration{{
			Name:       lo.ToPtr(nicConfigName),
			Properties: nicProps,
		}},
	}
}

// buildSecurityProfile returns the security profile only when encryption at host is enabled.
// When nil or false, returns nil matching the VM path which only sets it when enabled.
func buildSecurityProfile(encryptionAtHost *bool) *armcomputefleet.SecurityProfile {
	if encryptionAtHost == nil || !*encryptionAtHost {
		return nil
	}
	return &armcomputefleet.SecurityProfile{
		EncryptionAtHost: lo.ToPtr(true),
	}
}

// extensionsToProfile converts armcompute VM extensions to the armcomputefleet VMSS extension
// profile format. Returns nil when no extensions are provided.
func extensionsToProfile(exts []*armcompute.VirtualMachineExtension) *armcomputefleet.VirtualMachineScaleSetExtensionProfile {
	if len(exts) == 0 {
		return nil
	}
	converted := make([]*armcomputefleet.VirtualMachineScaleSetExtension, 0, len(exts))
	for _, e := range exts {
		if e == nil {
			continue
		}
		converted = append(converted, convertToScaleSetExtension(e))
	}
	if len(converted) == 0 {
		return nil
	}
	return &armcomputefleet.VirtualMachineScaleSetExtensionProfile{Extensions: converted}
}

// convertToScaleSetExtension converts a single armcompute.VirtualMachineExtension to the
// armcomputefleet.VirtualMachineScaleSetExtension format.
func convertToScaleSetExtension(ext *armcompute.VirtualMachineExtension) *armcomputefleet.VirtualMachineScaleSetExtension {
	if ext == nil || ext.Properties == nil {
		return &armcomputefleet.VirtualMachineScaleSetExtension{}
	}
	props := ext.Properties

	return &armcomputefleet.VirtualMachineScaleSetExtension{
		Name: ext.Name,
		Properties: &armcomputefleet.VirtualMachineScaleSetExtensionProperties{
			Publisher:               props.Publisher,
			Type:                    props.Type,
			TypeHandlerVersion:      props.TypeHandlerVersion,
			AutoUpgradeMinorVersion: props.AutoUpgradeMinorVersion,
			Settings:                toMapStringAny(props.Settings),
			ProtectedSettings:       toMapStringAny(props.ProtectedSettings),
		},
	}
}

// toMapStringAny extracts a map[string]any from the armcompute Settings/ProtectedSettings field.
// The SDK declares these as `any`; the actual runtime value may be:
//   - map[string]any — direct JSON unmarshalling
//   - *map[string]interface{} — the pattern used in extension constructors (getCSExtension, etc.)
//   - nil — no settings
func toMapStringAny(v any) map[string]any {
	if v == nil {
		return nil
	}
	switch m := v.(type) {
	case map[string]any:
		return m
	case *map[string]interface{}:
		if m == nil {
			return nil
		}
		return *m
	default:
		return nil
	}
}

// buildPoolRefs converts load balancer backend pool IDs to SubResource references.
func buildPoolRefs(pools []string) []*armcomputefleet.SubResource {
	if len(pools) == 0 {
		return nil
	}
	sortedPools := append([]string(nil), pools...)
	sort.Strings(sortedPools)

	out := make([]*armcomputefleet.SubResource, 0, len(sortedPools))
	for _, id := range sortedPools {
		out = append(out, &armcomputefleet.SubResource{ID: lo.ToPtr(id)})
	}
	return out
}
