#!/bin/bash

set -o allexport # export all variables to subshells
echo '#EOF' >> /opt/azure/manifest.json # wait_for_file looks for this
mkdir -p /var/log/azure/Microsoft.Azure.Extensions.CustomScript/events # expected, but not created w/o CSE

echo $(date),$(hostname) > /var/log/azure/cluster-provision-cse-output.log;
for i in $(seq 1 1200); do
grep -Fq "EOF" /opt/azure/containers/provision.sh && break;
if [ $i -eq 1200 ]; then exit 100; else sleep 1; fi;
done;
{{if .IsAKSCustomCloud}}
for i in $(seq 1 1200); do
grep -Fq "EOF" {{.InitAKSCustomCloudFilepath}} && break;
if [ $i -eq 1200 ]; then exit 100; else sleep 1; fi;
done;
REPO_DEPOT_ENDPOINT="{{.AKSCustomCloudRepoDepotEndpoint}}"
{{.InitAKSCustomCloudFilepath}} >> /var/log/azure/cluster-provision.log 2>&1;
{{end}}
ADMINUSER={{.AdminUsername}}
MOBY_VERSION={{.MobyVersion}}
TENANT_ID={{.TenantID}}
KUBERNETES_VERSION={{.KubernetesVersion}}
HYPERKUBE_URL={{.HyperkubeURL}}
KUBE_BINARY_URL={{.KubeBinaryURL}}
CUSTOM_KUBE_BINARY_URL={{.CustomKubeBinaryURL}}
CREDENTIAL_PROVIDER_DOWNLOAD_URL={{.CredentialProviderDownloadURL}}
KUBEPROXY_URL={{.KubeproxyURL}}
APISERVER_PUBLIC_KEY={{.APIServerPublicKey}}
SUBSCRIPTION_ID={{.SubscriptionID}}
RESOURCE_GROUP={{.ResourceGroup}}
LOCATION={{.Location}}
VM_TYPE={{.VMType}}
SUBNET={{.Subnet}}
NETWORK_SECURITY_GROUP={{.NetworkSecurityGroup}}
VIRTUAL_NETWORK={{.VirtualNetwork}}
VIRTUAL_NETWORK_RESOURCE_GROUP={{.VirtualNetworkResourceGroup}}
ROUTE_TABLE={{.RouteTable}}
PRIMARY_AVAILABILITY_SET={{.PrimaryAvailabilitySet}}
PRIMARY_SCALE_SET={{.PrimaryScaleSet}}
SERVICE_PRINCIPAL_CLIENT_ID={{.ServicePrincipalClientID}}
NETWORK_PLUGIN={{.NetworkPlugin}}
NETWORK_POLICY="{{.NetworkPolicy}}"
VNET_CNI_PLUGINS_URL={{.VNETCNILinuxPluginsURL}}
CNI_PLUGINS_URL={{.CNIPluginsURL}}
CLOUDPROVIDER_BACKOFF={{.CloudProviderBackoff}}
CLOUDPROVIDER_BACKOFF_MODE={{.CloudProviderBackoffMode}}
CLOUDPROVIDER_BACKOFF_RETRIES={{.CloudProviderBackoffRetries}}
CLOUDPROVIDER_BACKOFF_EXPONENT={{.CloudProviderBackoffExponent}}
CLOUDPROVIDER_BACKOFF_DURATION={{.CloudProviderBackoffDuration}}
CLOUDPROVIDER_BACKOFF_JITTER={{.CloudProviderBackoffJitter}}
CLOUDPROVIDER_RATELIMIT={{.CloudProviderRatelimit}}
CLOUDPROVIDER_RATELIMIT_QPS={{.CloudProviderRatelimitQPS}}
CLOUDPROVIDER_RATELIMIT_QPS_WRITE={{.CloudProviderRatelimitQPSWrite}}
CLOUDPROVIDER_RATELIMIT_BUCKET={{.CloudProviderRatelimitBucket}}
CLOUDPROVIDER_RATELIMIT_BUCKET_WRITE={{.CloudProviderRatelimitBucketWrite}}
LOAD_BALANCER_DISABLE_OUTBOUND_SNAT={{.LoadBalancerDisableOutboundSNAT}}
USE_MANAGED_IDENTITY_EXTENSION={{.UseManagedIdentityExtension}}
USE_INSTANCE_METADATA={{.UseInstanceMetadata}}
LOAD_BALANCER_SKU={{.LoadBalancerSKU}}
EXCLUDE_MASTER_FROM_STANDARD_LB={{.ExcludeMasterFromStandardLB}}
MAXIMUM_LOADBALANCER_RULE_COUNT={{.MaximumLoadbalancerRuleCount}}
CONTAINER_RUNTIME={{.ContainerRuntime}}
CLI_TOOL={{.CLITool}}
CONTAINERD_DOWNLOAD_URL_BASE={{.ContainerdDownloadURLBase}}
NETWORK_MODE={{.NetworkMode}}
KUBE_BINARY_URL={{.KubeBinaryURL}}
USER_ASSIGNED_IDENTITY_ID={{.UserAssignedIdentityID}}
API_SERVER_NAME={{.APIServerName}}
IS_VHD={{.IsVHD}}
GPU_NODE={{.GPUNode}}
SGX_NODE={{.SGXNode}}
MIG_NODE={{.MIGNode}}
CONFIG_GPU_DRIVER_IF_NEEDED={{.ConfigGPUDriverIfNeeded}}
ENABLE_GPU_DEVICE_PLUGIN_IF_NEEDED={{.EnableGPUDevicePluginIfNeeded}}
TELEPORTD_PLUGIN_DOWNLOAD_URL={{.TeleportdPluginDownloadURL}}
CONTAINERD_VERSION={{.ContainerdVersion}}
CONTAINERD_PACKAGE_URL={{.ContainerdPackageURL}}
RUNC_VERSION={{.RuncVersion}}
RUNC_PACKAGE_URL={{.RuncPackageURL}}
ENABLE_HOSTS_CONFIG_AGENT="{{.EnableHostsConfigAgent}}"
DISABLE_SSH="{{.DisableSSH}}"
NEEDS_CONTAINERD="{{.NeedsContainerd}}"
TELEPORT_ENABLED="{{.TeleportEnabled}}"
SHOULD_CONFIGURE_HTTP_PROXY="{{.ShouldConfigureHTTPProxy}}"
SHOULD_CONFIGURE_HTTP_PROXY_CA="{{.ShouldConfigureHTTPProxyCA}}"
HTTP_PROXY_TRUSTED_CA="{{.HTTPProxyTrustedCA}}"
SHOULD_CONFIGURE_CUSTOM_CA_TRUST="{{.ShouldConfigureCustomCATrust}}"
CUSTOM_CA_TRUST_COUNT="{{len .CustomCATrustConfigCerts}}"
{{range $i, $cert := .CustomCATrustConfigCerts}}
CUSTOM_CA_CERT_{{$i}}="{{$cert}}"
{{end}}
IS_KRUSTLET="{{.IsKrustlet}}"
GPU_NEEDS_FABRIC_MANAGER="{{.GPUNeedsFabricManager}}"
NEEDS_DOCKER_LOGIN="{{.NeedsDockerLogin}}"
IPV6_DUAL_STACK_ENABLED="{{.IPv6DualStackEnabled}}"
OUTBOUND_COMMAND="{{.OutboundCommand}}"
ENABLE_UNATTENDED_UPGRADES="{{.EnableUnattendedUpgrades}}"
ENSURE_NO_DUPE_PROMISCUOUS_BRIDGE="{{.EnsureNoDupePromiscuousBridge}}"
SHOULD_CONFIG_SWAP_FILE="{{.ShouldConfigSwapFile}}"
SHOULD_CONFIG_TRANSPARENT_HUGE_PAGE="{{.ShouldConfigTransparentHugePage}}"
TARGET_CLOUD="{{.TargetCloud}}"
TARGET_ENVIRONMENT="{{.TargetEnvironment}}"
CUSTOM_ENV_JSON="{{.CustomEnvJSON}}"
IS_CUSTOM_CLOUD="{{.IsCustomCloud}}"
CSE_HELPERS_FILEPATH="{{.CSEHelpersFilepath}}"
CSE_DISTRO_HELPERS_FILEPATH="{{.CSEDistroHelpersFilepath}}"
CSE_INSTALL_FILEPATH="{{.CSEInstallFilepath}}"
CSE_DISTRO_INSTALL_FILEPATH="{{.CSEDistroInstallFilepath}}"
CSE_CONFIG_FILEPATH="{{.CSEConfigFilepath}}"
AZURE_PRIVATE_REGISTRY_SERVER="{{.AzurePrivateRegistryServer}}"
HAS_CUSTOM_SEARCH_DOMAIN="{{.HasCustomSearchDomain}}"
CUSTOM_SEARCH_DOMAIN_FILEPATH="{{.CustomSearchDomainFilepath}}"
HTTP_PROXY_URLS="{{.HTTPProxyURLs}}"
HTTPS_PROXY_URLS="{{.HTTPSProxyURLs}}"
NO_PROXY_URLS="{{.NoProxyURLs}}"
ENABLE_TLS_BOOTSTRAPPING="{{.TLSBootstrappingEnabled}}"
ENABLE_SECURE_TLS_BOOTSTRAPPING="{{.SecureTLSBootstrappingEnabled}}"
ENABLE_KUBELET_SERVING_CERTIFICATE_ROTATION="{{.EnableKubeletServingCertificateRotation}}"
DHCPV6_SERVICE_FILEPATH="{{.DHCPv6ServiceFilepath}}"
DHCPV6_CONFIG_FILEPATH="{{.DHCPv6ConfigFilepath}}"
THP_ENABLED="{{.THPEnabled}}"
THP_DEFRAG="{{.THPDefrag}}"
SERVICE_PRINCIPAL_FILE_CONTENT="{{.ServicePrincipalFileContent}}"
KUBELET_CLIENT_CONTENT="{{.KubeletClientContent}}"
KUBELET_CLIENT_CERT_CONTENT="{{.KubeletClientCertContent}}"
KUBELET_CONFIG_FILE_ENABLED="{{.KubeletConfigFileEnabled}}"
KUBELET_CONFIG_FILE_CONTENT="{{.KubeletConfigFileContent}}"
SWAP_FILE_SIZE_MB="{{.SwapFileSizeMB}}"
GPU_IMAGE_SHA="{{.GPUImageSHA}}"
GPU_DRIVER_VERSION="{{.GPUDriverVersion}}"
GPU_DRIVER_TYPE="{{.GPUDriverType}}"
GPU_INSTANCE_PROFILE="{{.GPUInstanceProfile}}"
CUSTOM_SEARCH_DOMAIN_NAME="{{.CustomSearchDomainName}}"
CUSTOM_SEARCH_REALM_USER="{{.CustomSearchRealmUser}}"
CUSTOM_SEARCH_REALM_PASSWORD="{{.CustomSearchRealmPassword}}"
MESSAGE_OF_THE_DAY="{{.MessageOfTheDay}}"
HAS_KUBELET_DISK_TYPE="{{.HasKubeletDiskType}}"
NEEDS_CGROUPV2="{{.NeedsCgroupV2}}"
SYSCTL_CONTENT="{{.SysctlContent}}"
TLS_BOOTSTRAP_TOKEN="{{.TLSBootstrapToken}}"
KUBELET_FLAGS="{{.KubeletFlags}}"
# In Fleet Launch mode, the VM resource name (e.g. "aks_1238fb96_0", from vmNamePrefix)
# contains underscores which are invalid in K8s node names (RFC 1123). The OS hostname
# (from computerNamePrefix, e.g. "aks-AZNYK9") also differs from the VM name. Without
# an explicit provider-id, the cloud controller manager (CCM) tries to look up the VM by
# node name and fails because neither hostname nor sanitized VM name will match.
# Setting --provider-id directly from IMDS bypasses CCM's name-based VM lookup entirely,
# allowing the node to be initialized with the correct Azure VM identity. For VMSS-backed
# nodes this is a no-op since CCM already resolves them, but an explicit provider-id is
# harmless and removes the dependency on CCM's lookup path.
IMDS_RESOURCE_ID=$(curl -s -H Metadata:true --max-time 5 \
  "http://169.254.169.254/metadata/instance/compute/resourceId?api-version=2021-02-01&format=text" 2>/dev/null)
if [ -n "$IMDS_RESOURCE_ID" ]; then
  IMDS_RG=$(echo "$IMDS_RESOURCE_ID" | sed -n 's|.*/resourceGroups/\([^/]*\)/.*|\1|p')
  IMDS_RG_LOWER=$(echo "$IMDS_RG" | tr '[:upper:]' '[:lower:]')
  PROVIDER_ID="azure://$(echo "$IMDS_RESOURCE_ID" | sed "s|/resourceGroups/$IMDS_RG/|/resourceGroups/$IMDS_RG_LOWER/|")"
  KUBELET_FLAGS="$KUBELET_FLAGS --provider-id=$PROVIDER_ID"
fi
KUBELET_NODE_LABELS="{{.KubeletNodeLabels}}"
AZURE_ENVIRONMENT_FILEPATH="{{.AzureEnvironmentFilepath}}"
KUBE_CA_CRT="{{.KubeCACrt}}"
CONTAINERD_CONFIG_CONTENT="{{.ContainerdConfigContent}}"
IS_KATA="{{.IsKata}}"
MCR_REPOSITORY_BASE="mcr.microsoft.com"
ENABLE_IMDS_RESTRICTION=false
INSERT_IMDS_RESTRICTION_RULE_TO_MANGLE_TABLE=false
CSE_TIMEOUT=15m
/usr/bin/nohup /bin/bash -c "/bin/bash /opt/azure/containers/provision_start.sh"
