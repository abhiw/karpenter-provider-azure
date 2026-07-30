#!/usr/bin/env bash
# Fleet Launch Mode Demo — Provisions 10 nodes via Karpenter Fleet batching
# Usage: ./hack/demo/fleet-demo.sh [step]
#   Steps: setup | deploy | provision | interconnect | observe | cleanup | all (default: all)
set -euo pipefail

###############################################################################
# Configuration
###############################################################################
export AZURE_SUBSCRIPTION_ID="${AZURE_SUBSCRIPTION_ID:-2994199d-5716-49a3-80aa-eb2ff114e431}"
export AZURE_RESOURCE_GROUP="${AZURE_RESOURCE_GROUP:-awarhekar-aks-karpenter-rg}"
export AZURE_CLUSTER_NAME="${AZURE_CLUSTER_NAME:-karpenter}"
export AZURE_LOCATION="${AZURE_LOCATION:-eastus2euap}"
export AZURE_ACR_NAME="${AZURE_ACR_NAME:-awarhekarfleetacr}"
export AZURE_ACR_URL="${AZURE_ACR_NAME}.azurecr.io"
export AZURE_NODE_RESOURCE_GROUP="${AZURE_NODE_RESOURCE_GROUP:-MC_awarhekar-aks-karpenter-rg_karpenter_eastus2euap}"
export KARPENTER_MSI_NAME="${KARPENTER_MSI_NAME:-karpentermsi}"
export KARPENTER_SA_NAME="${KARPENTER_SA_NAME:-karpenter-sa}"
export PROVISION_MODE="fleet"
export DEMO_REPLICAS="${DEMO_REPLICAS:-100}"
export LOG_LEVEL="${LOG_LEVEL:-debug}"

# Interconnect demo parameters (Fleet-only). These are placeholder ARM resource IDs —
# real values must be supplied by the caller before running `step_provision_interconnect`
# (see hack/demo/fleet-demo.sh interconnect). VM size defaults to the GPU SKU this
# feature targets; override via AZURE_GPU_VM_SIZE if needed for testing.
export AZURE_INTERCONNECT_BLOCK_ID="${AZURE_INTERCONNECT_BLOCK_ID:-}"
export AZURE_INTERCONNECT_GROUP_ID="${AZURE_INTERCONNECT_GROUP_ID:-/subscriptions/2994199d-5716-49a3-80aa-eb2ff114e431/resourceGroups/awarhekar-aks-karpenter-rg/providers/Microsoft.Network/interconnectGroups/awarhekar-aks-karpenter-rg-icg}"
export AZURE_INTERCONNECT_SUBGROUP_ID="${AZURE_INTERCONNECT_SUBGROUP_ID:-}"
export AZURE_GPU_VM_SIZE="${AZURE_GPU_VM_SIZE:-Standard_ND128isr_GB300_v6}"

# Build settings (WSL)
export PATH="$HOME/sdk/go/bin:$HOME/go/bin:$PATH"
export CGO_ENABLED=0
export GOFLAGS="-buildvcs=false"
export KO_DOCKER_REPO="${AZURE_ACR_URL}/karpenter"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
LOG_DIR="$HOME/fleet-demo-logs"
mkdir -p "$LOG_DIR"

FMT='+%Y-%m-%dT%H:%M:%SZ'

info()  { echo -e "\033[1;34m[INFO]\033[0m  $*"; }
ok()    { echo -e "\033[1;32m[OK]\033[0m    $*"; }
err()   { echo -e "\033[1;31m[ERROR]\033[0m $*" >&2; }
wait_msg() { echo -e "\033[1;33m[WAIT]\033[0m  $*"; }
header(){ echo -e "\n\033[1;36m══════════════════════════════════════════════════════\033[0m"; echo -e "\033[1;36m  $*\033[0m"; echo -e "\033[1;36m══════════════════════════════════════════════════════\033[0m\n"; }

# apply_zone_placement_policy_any patches the AKS cluster's default agent pool with a
# speculative, undocumented "placement.zonePlacementPolicy": "Any" property via a raw
# ARM GET+PUT (no CLI flag or documented ARM property exists for this — see the caller's
# comment in step_setup for details). Best-effort only: failures are logged and ignored
# so they never block the rest of the demo.
apply_zone_placement_policy_any() {
    local api_version="2025-08-01"
    local pool_name
    pool_name=$(az aks nodepool list \
        --cluster-name "$AZURE_CLUSTER_NAME" \
        --resource-group "$AZURE_RESOURCE_GROUP" \
        --query "[0].name" -o tsv 2>/dev/null) || true
    if [[ -z "${pool_name:-}" ]]; then
        info "Skipping speculative zonePlacementPolicy patch: could not resolve default node pool name."
        return 0
    fi

    local pool_id="/subscriptions/${AZURE_SUBSCRIPTION_ID}/resourceGroups/${AZURE_RESOURCE_GROUP}/providers/Microsoft.ContainerService/managedClusters/${AZURE_CLUSTER_NAME}/agentPools/${pool_name}"

    info "Attempting speculative 'placement.zonePlacementPolicy: Any' patch on agent pool '$pool_name' (undocumented property, may be ignored/rejected by the API)..."
    local current
    if ! current=$(az rest --method get --url "https://management.azure.com${pool_id}?api-version=${api_version}" 2>/dev/null); then
        info "Skipping speculative zonePlacementPolicy patch: failed to fetch current agent pool state."
        return 0
    fi

    local patched
    patched=$(echo "$current" | jq '.properties.placement = {"zonePlacementPolicy": "Any"}')

    if az rest --method put \
        --url "https://management.azure.com${pool_id}?api-version=${api_version}" \
        --body "$patched" --output none 2>/dev/null; then
        ok "Speculative zonePlacementPolicy patch accepted by the API."
    else
        info "Speculative zonePlacementPolicy patch was rejected or ignored by the API (expected, since it is undocumented) — continuing."
    fi
}

###############################################################################
# Step 1: Setup — Cluster creation & infrastructure
###############################################################################
step_setup() {
    header "Step 1: Infrastructure Setup"
    echo "This step creates the Azure resource group, managed identity, ACR, AKS cluster,"
    echo "workload identity federation, and role assignments needed for Karpenter Fleet mode."
    echo ""

    info "Setting active subscription to $AZURE_SUBSCRIPTION_ID..."
    az account set --subscription "$AZURE_SUBSCRIPTION_ID"

    # Resource Group
    echo ""
    info "Creating resource group '$AZURE_RESOURCE_GROUP' in region '$AZURE_LOCATION'..."
    echo "  This resource group holds the AKS cluster, managed identity, and ACR."
    az group create --name "$AZURE_RESOURCE_GROUP" --location "$AZURE_LOCATION" --output none 2>/dev/null || true
    ok "Resource group ready."

    # Managed Identity for Karpenter
    echo ""
    info "Creating managed identity '$KARPENTER_MSI_NAME' for Karpenter..."
    echo "  Karpenter uses this identity (via workload identity) to call Azure APIs"
    echo "  (Fleet, VM, NIC creation in the MC resource group)."
    az identity create --name "$KARPENTER_MSI_NAME" --resource-group "$AZURE_RESOURCE_GROUP" --location "$AZURE_LOCATION" --output none 2>/dev/null || true
    KARPENTER_MSI_CLIENT_ID=$(az identity show --name "$KARPENTER_MSI_NAME" --resource-group "$AZURE_RESOURCE_GROUP" --query clientId -o tsv)
    KARPENTER_MSI_OBJECT_ID=$(az identity show --name "$KARPENTER_MSI_NAME" --resource-group "$AZURE_RESOURCE_GROUP" --query principalId -o tsv)
    ok "MSI ready. Client ID: $KARPENTER_MSI_CLIENT_ID"

    # ACR
    echo ""
    info "Creating ACR '$AZURE_ACR_NAME' to host the Karpenter controller image..."
    az acr create --name "$AZURE_ACR_NAME" --resource-group "$AZURE_RESOURCE_GROUP" --sku Basic --output none 2>/dev/null || true
    ok "ACR ready: ${AZURE_ACR_URL}"

    # AKS Cluster
    echo ""
    info "Creating AKS cluster '$AZURE_CLUSTER_NAME'..."
    echo "  Config: 2 system nodes, azure-cni overlay, cilium dataplane, OIDC issuer enabled."
    if ! az aks show --name "$AZURE_CLUSTER_NAME" --resource-group "$AZURE_RESOURCE_GROUP" &>/dev/null; then
        wait_msg "This takes 3-5 minutes..."
        az aks create \
            --name "$AZURE_CLUSTER_NAME" \
            --resource-group "$AZURE_RESOURCE_GROUP" \
            --location "$AZURE_LOCATION" \
            --node-resource-group "$AZURE_NODE_RESOURCE_GROUP" \
            --node-count 1 \
            --node-vm-size Standard_D4ads_v5 \
            --network-plugin azure \
            --network-plugin-mode overlay \
            --network-dataplane cilium \
            --network-policy cilium \
            --enable-oidc-issuer \
            --enable-workload-identity \
            --generate-ssh-keys \
            --attach-acr "$AZURE_ACR_NAME" \
            --output none
        ok "AKS cluster created."
    else
        info "Cluster already exists, skipping creation."
    fi

    # zonePlacementPolicy: Any (speculative)
    # NOTE: "placement.zonePlacementPolicy" is NOT a documented property of
    # Microsoft.ContainerService/managedClusters/agentPools in any published ARM API
    # version (verified against the ARM schema reference up to 2026-04-02-preview,
    # where only "availabilityZones" exists for zone selection). No az CLI flag exists
    # for it either (checked `az aks create --help` / `az aks nodepool add --help`,
    # with and without the aks-preview extension). This block is added purely at the
    # user's explicit request as a best-effort/speculative raw ARM PATCH — it is
    # expected to be silently ignored or rejected by the API and is NOT required by
    # the Fleet interconnect feature itself (see spec-design-fleet-interconnect-vmsize-params.md §12).
    apply_zone_placement_policy_any

    # Get credentials
    echo ""
    info "Fetching kubeconfig credentials..."
    az aks get-credentials --name "$AZURE_CLUSTER_NAME" --resource-group "$AZURE_RESOURCE_GROUP" --overwrite-existing
    ok "kubectl now points to cluster '$AZURE_CLUSTER_NAME'."

    # Federated credential for workload identity
    echo ""
    info "Creating federated credential for workload identity..."
    echo "  This links the Kubernetes service account 'kube-system/${KARPENTER_SA_NAME}'"
    echo "  to the Azure managed identity, enabling pod-based authentication."
    AKS_OIDC_ISSUER=$(az aks show --name "$AZURE_CLUSTER_NAME" --resource-group "$AZURE_RESOURCE_GROUP" --query "oidcIssuerProfile.issuerUrl" -o tsv)
    az identity federated-credential create \
        --name "karpenter-federated-cred" \
        --identity-name "$KARPENTER_MSI_NAME" \
        --resource-group "$AZURE_RESOURCE_GROUP" \
        --issuer "$AKS_OIDC_ISSUER" \
        --subject "system:serviceaccount:kube-system:${KARPENTER_SA_NAME}" \
        --audiences "api://AzureADTokenExchange" \
        --output none 2>/dev/null || true
    ok "Federated credential configured."

    # Role assignments
    echo ""
    info "Assigning RBAC roles to Karpenter identity..."
    MC_RG_ID=$(az group show --name "$AZURE_NODE_RESOURCE_GROUP" --query id -o tsv)

    echo "  • Contributor on MC resource group (for Fleet/VM/NIC operations)..."
    az role assignment create --assignee-object-id "$KARPENTER_MSI_OBJECT_ID" --assignee-principal-type ServicePrincipal \
        --role "Contributor" --scope "$MC_RG_ID" --output none 2>/dev/null || true

    VNET_ID=$(az network vnet list --resource-group "$AZURE_NODE_RESOURCE_GROUP" --query "[0].id" -o tsv 2>/dev/null || true)
    if [[ -n "$VNET_ID" ]]; then
        echo "  • Network Contributor on VNET (for NIC subnet attachment)..."
        az role assignment create --assignee-object-id "$KARPENTER_MSI_OBJECT_ID" --assignee-principal-type ServicePrincipal \
            --role "Network Contributor" --scope "$VNET_ID" --output none 2>/dev/null || true
    fi
    ok "Role assignments complete."

    # Register Microsoft.AzureFleet provider
    echo ""
    info "Registering Microsoft.AzureFleet resource provider on subscription..."
    echo "  This is required for the Fleet API (Microsoft.AzureFleet/fleets) to be callable."
    az provider register --namespace Microsoft.AzureFleet --output none 2>/dev/null || true
    wait_msg "Provider registration may take 1-2 minutes in the background (continuing)..."

    echo ""
    ok "Infrastructure setup complete!"
    echo ""
    echo "  Resource Group:     $AZURE_RESOURCE_GROUP"
    echo "  AKS Cluster:        $AZURE_CLUSTER_NAME"
    echo "  Node Resource Group: $AZURE_NODE_RESOURCE_GROUP"
    echo "  MSI:                $KARPENTER_MSI_NAME ($KARPENTER_MSI_CLIENT_ID)"
    echo "  ACR:                $AZURE_ACR_URL"
}

###############################################################################
# Step 2: Build & Deploy Karpenter in Fleet mode
###############################################################################
step_deploy() {
    header "Step 2: Build & Deploy Karpenter (Fleet Mode)"
    echo "This step builds the Karpenter controller image from source (using ko),"
    echo "pushes it to ACR, and deploys it to the cluster via Helm (using skaffold)."
    echo "The controller will run with PROVISION_MODE=fleet, using the Azure Compute Fleet"
    echo "API for node provisioning instead of individual VM PUTs."
    echo ""
    cd "$REPO_DIR"

    # Login to ACR (token-based, no Docker daemon required)
    info "Authenticating to ACR '$AZURE_ACR_NAME' (token-based, no Docker needed)..."
    echo "  Using 'az acr login --expose-token' to get an access token for ko/skaffold."
    ACR_TOKEN=$(az acr login -n "$AZURE_ACR_NAME" --expose-token --query accessToken -o tsv)
    # Write auth to ~/.docker/config.json so ko (via skaffold) can push
    mkdir -p "$HOME/.docker"
    AUTH_B64=$(echo -n "00000000-0000-0000-0000-000000000000:${ACR_TOKEN}" | base64 -w0)
    # Merge into existing config.json if present
    if [[ -f "$HOME/.docker/config.json" ]]; then
        jq --arg url "$AZURE_ACR_URL" --arg auth "$AUTH_B64" \
            '.auths[$url] = {"auth": $auth}' "$HOME/.docker/config.json" > "$HOME/.docker/config.json.tmp" \
            && mv "$HOME/.docker/config.json.tmp" "$HOME/.docker/config.json"
    else
        echo "{\"auths\":{\"${AZURE_ACR_URL}\":{\"auth\":\"${AUTH_B64}\"}}}" | jq '.' > "$HOME/.docker/config.json"
    fi
    ok "ACR auth configured in ~/.docker/config.json (ko/skaffold will use this)."

    # Generate helm values
    echo ""
    info "Generating karpenter-values.yaml from cluster configuration..."
    echo "  This queries the AKS cluster for endpoints, tokens, network config, etc."
    echo "  Key setting: PROVISION_MODE=fleet"
    bash hack/deploy/configure-values.sh \
        "$AZURE_CLUSTER_NAME" \
        "$AZURE_RESOURCE_GROUP" \
        "$KARPENTER_SA_NAME" \
        "$KARPENTER_MSI_NAME" \
        "true" \
        "fleet"
    ok "Helm values generated."

    # Build and deploy via skaffold
    echo ""
    info "Building controller image and deploying via skaffold..."
    echo "  • ko builds the Go binary into a container image"
    echo "  • Image is pushed to $AZURE_ACR_URL/karpenter"
    echo "  • Helm chart is installed/upgraded in kube-system namespace"
    wait_msg "This takes 1-3 minutes (build + push + rollout)..."
    skaffold run 2>&1 | tee "$LOG_DIR/skaffold-deploy.log"
    ok "Skaffold deploy completed."

    # Wait for rollout
    echo ""
    info "Waiting for Karpenter pods to become ready..."
    wait_msg "Waiting up to 5 minutes for deployment rollout..."
    kubectl -n kube-system rollout status deploy/karpenter --timeout=300s
    ok "Karpenter deployment is ready."

    # Verify fleet mode
    echo ""
    info "Verifying PROVISION_MODE=fleet is set on the controller..."
    ACTUAL_MODE=$(kubectl -n kube-system get deploy/karpenter -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="PROVISION_MODE")].value}')
    if [[ "$ACTUAL_MODE" != "fleet" ]]; then
        err "PROVISION_MODE is '$ACTUAL_MODE', expected 'fleet'. Check karpenter-values.yaml."
        exit 1
    fi
    ok "Confirmed: PROVISION_MODE=fleet"

    echo ""
    info "Controller pod status:"
    kubectl -n kube-system get pods -l app.kubernetes.io/name=karpenter -o wide
    echo ""
    info "Last few log lines:"
    kubectl -n kube-system logs deploy/karpenter --tail=5
}

###############################################################################
# Step 3: Provision 10 nodes
###############################################################################
step_provision() {
    header "Step 3: Provision $DEMO_REPLICAS Nodes via Fleet (Gradual, Multi-BatchKey)"
    echo "This step creates 3 NodePools (different batch keys) and deploys pods in"
    echo "random-sized waves with delays between them. This tests:"
    echo "  • Batching across multiple batch keys"
    echo "  • Inflight coalescing when waves arrive during in-flight LROs"
    echo "  • Cooldown blocking duplicate Fleets after LRO completion"
    echo ""
    echo "Batch settings: idleTimeout=1s, maxTimeout=5s"
    echo "Wave delay: 8-15s (exceeds maxTimeout → each wave is a separate batcher firing)"
    echo ""
    cd "$REPO_DIR"

    # Clean any leftover resources
    info "Cleaning up any leftover resources from previous runs..."
    kubectl delete deployments --all -n default --ignore-not-found 2>/dev/null || true
    kubectl delete pods --all -n default --ignore-not-found 2>/dev/null || true
    kubectl delete nodeclaims --all --ignore-not-found 2>/dev/null || true
    kubectl delete nodepools --all --ignore-not-found 2>/dev/null || true
    kubectl delete aksnodeclasses --all --ignore-not-found 2>/dev/null || true
    wait_msg "Waiting 5s for cleanup to propagate..."
    sleep 5
    ok "Cleanup done."

    # Apply AKSNodeClass
    echo ""
    info "Applying AKSNodeClass 'default'..."
    kubectl apply -f - <<'EOF'
apiVersion: karpenter.azure.com/v1beta1
kind: AKSNodeClass
metadata:
  name: default
  annotations:
    kubernetes.io/description: "Fleet demo — Ubuntu2204 Gen2 nodes"
spec:
  imageFamily: Ubuntu2204
EOF
    ok "AKSNodeClass applied."

    # Apply 3 NodePools (each produces a different batch key)
    echo ""
    info "Applying 3 NodePools (fleet-alpha, fleet-beta, fleet-gamma)..."
    echo "  Each NodePool has the same SKU constraints but different names → different batch keys."
    echo "  This simulates real workloads with multiple NodePools."

    for POOL in alpha beta gamma; do
        kubectl apply -f - <<EOF
apiVersion: karpenter.sh/v1
kind: NodePool
metadata:
  name: fleet-${POOL}
  annotations:
    kubernetes.io/description: "Fleet demo NodePool ${POOL}"
spec:
  disruption:
    consolidationPolicy: WhenEmptyOrUnderutilized
    consolidateAfter: Never
    budgets:
      - nodes: "0"
  limits:
    cpu: "1000"
    memory: 2000Gi
  template:
    metadata:
      labels:
        kubernetes.azure.com/ebpf-dataplane: cilium
        demo: fleet
        pool: ${POOL}
    spec:
      expireAfter: Never
      startupTaints:
        - key: node.cilium.io/agent-not-ready
          effect: NoExecute
          value: "true"
      requirements:
        - key: kubernetes.io/arch
          operator: In
          values: ["amd64"]
        - key: kubernetes.io/os
          operator: In
          values: ["linux"]
        - key: karpenter.sh/capacity-type
          operator: In
          values: ["on-demand"]
        - key: karpenter.azure.com/sku-family
          operator: In
          values: ["D"]
        - key: karpenter.azure.com/sku-cpu
          operator: Lt
          values: ["3"]
        - key: karpenter.azure.com/sku-version
          operator: In
          values: ["4", "5", "6"]
      nodeClassRef:
        group: karpenter.azure.com
        kind: AKSNodeClass
        name: default
EOF
    done
    ok "3 NodePools applied (fleet-alpha, fleet-beta, fleet-gamma)."

    # Deploy pods in random-sized waves targeting random pools
    echo ""
    info "Deploying $DEMO_REPLICAS pods in random waves across 3 NodePools..."
    echo ""

    POOLS=("alpha" "beta" "gamma")
    TOTAL_DEPLOYED=0
    WAVE=1
    
    # Record start time (since first pods/NodeClaims)
    START_TIME=$(date +%s)

    while [[ "$TOTAL_DEPLOYED" -lt "$DEMO_REPLICAS" ]]; do
        # Random wave size: 5-25 pods
        REMAINING=$((DEMO_REPLICAS - TOTAL_DEPLOYED))
        MAX_WAVE=$((REMAINING < 25 ? REMAINING : 25))
        MIN_WAVE=$((REMAINING < 5 ? REMAINING : 5))
        WAVE_SIZE=$(( (RANDOM % (MAX_WAVE - MIN_WAVE + 1)) + MIN_WAVE ))

        # Pick a random pool
        POOL_IDX=$((RANDOM % 3))
        POOL=${POOLS[$POOL_IDX]}

        info "  Wave $WAVE: deploying $WAVE_SIZE pods → pool=fleet-${POOL}  (total so far: $TOTAL_DEPLOYED)"

        kubectl apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fleet-inflate-w${WAVE}-${POOL}
  namespace: default
spec:
  replicas: ${WAVE_SIZE}
  selector:
    matchLabels:
      app: fleet-inflate-w${WAVE}-${POOL}
  template:
    metadata:
      labels:
        app: fleet-inflate-w${WAVE}-${POOL}
        demo-wave: "wave-${WAVE}"
    spec:
      nodeSelector:
        pool: ${POOL}
      tolerations:
        - key: node.cilium.io/agent-not-ready
          operator: Exists
          effect: NoExecute
          tolerationSeconds: 120
      containers:
        - name: pause
          image: mcr.microsoft.com/oss/kubernetes/pause:3.6
          resources:
            requests:
              cpu: "1"
              memory: 256Mi
EOF

        TOTAL_DEPLOYED=$((TOTAL_DEPLOYED + WAVE_SIZE))
        WAVE=$((WAVE + 1))

        if [[ "$TOTAL_DEPLOYED" -lt "$DEMO_REPLICAS" ]]; then
            # Random delay: 3-10 seconds between waves
            DELAY=$(( (RANDOM % 8) + 3 ))
            wait_msg "    Waiting ${DELAY}s before next wave..."
            sleep "$DELAY"
        fi
    done

    echo ""
    ok "All $TOTAL_DEPLOYED pods deployed across $((WAVE - 1)) waves."
    echo ""
    # Wait for nodes to be provisioned — show per-wave status
    echo ""
    wait_msg "Waiting for $DEMO_REPLICAS nodes to become Ready (timeout: 15 minutes)..."
    echo "  Karpenter flow: Pending pods → NodeClaims → Fleet PUT → VMs created → Nodes join"
    echo ""
    echo "  Monitor commands (run in another terminal):"
    echo "    watch 'kubectl get nodeclaims --no-headers | wc -l'"
    echo "    watch 'kubectl get nodes -l demo=fleet --no-headers'"
    echo "    kubectl -n kube-system logs -f deploy/karpenter | grep -i \"fleet\\|batch\\|launched\""
    echo ""
    TIMEOUT=900
    TOTAL_WAVES=$((WAVE - 1))
    
    # Track metrics
    FIRST_NODE_TIME=""
    ALL_NODES_JOINED_TIME=""
    FIRST_READY_TIME=""
    ALL_READY_TIME=""
    
    while true; do
        CURRENT_TIME=$(date +%s)
        ELAPSED=$((CURRENT_TIME - START_TIME))

        # Fetch status snapshot to reduce API calls
        NC_STATUS=$(kubectl get nodeclaims -o custom-columns=NAME:.metadata.name,TYPE:.status.instanceType,NODE:.status.nodeName,READY:.status.conditions[0].status --no-headers 2>/dev/null || echo "")

        # Print per-wave status
        TOTAL_READY=0
        TOTAL_NODES=0
        STATUS_LINES=""
        for W in $(seq 1 $TOTAL_WAVES); do
            # Find deployments for this wave (could be any pool)
            WAVE_DEPS=$(kubectl get deployments -n default --no-headers 2>/dev/null | awk "/fleet-inflate-w${W}-/ {print \$1}")
            WAVE_POOL=""
            WAVE_REPLICAS=0
            for DEP in $WAVE_DEPS; do
                WAVE_POOL=$(echo "$DEP" | sed "s/fleet-inflate-w${W}-//")
                WAVE_REPLICAS=$(kubectl get deployment "$DEP" -n default -o jsonpath='{.spec.replicas}' 2>/dev/null || echo 0)
            done
            # Count nodeclaims for this pool from our snapshot
            POOL_CLAIMS=$(echo "$NC_STATUS" | awk "/fleet-${WAVE_POOL}/" | wc -l || echo 0)
            POOL_READY=$(echo "$NC_STATUS" | awk "/fleet-${WAVE_POOL} .* True/" | wc -l || echo 0)
            # For per-wave display, show pool-level stats since waves share pools
            POOL_NODES=$(kubectl get nodes -l karpenter.sh/nodepool=fleet-${WAVE_POOL} --no-headers 2>/dev/null | wc -l || echo 0)
            POOL_NODES_READY=$(kubectl get nodes -l karpenter.sh/nodepool=fleet-${WAVE_POOL} --no-headers 2>/dev/null | awk "/ Ready/" | wc -l || echo 0)
            STATUS_LINES+="    Wave $W (pool=${WAVE_POOL}, size=${WAVE_REPLICAS}): Nodes ${POOL_NODES_READY}/${POOL_NODES} Ready"$'\n'
        done

        # Overall counts
        NODE_COUNT=$(kubectl get nodes -l demo=fleet --no-headers 2>/dev/null | wc -l || echo 0)
        READY_COUNT=$(kubectl get nodes -l demo=fleet --no-headers 2>/dev/null | awk "/ Ready/" | wc -l || echo 0)
        NODECLAIM_COUNT=$(echo "$NC_STATUS" | wc -l || echo 0)

        # Record metrics
        if [[ -z "$FIRST_NODE_TIME" ]] && [[ "$NODE_COUNT" -gt 0 ]]; then FIRST_NODE_TIME=$ELAPSED; fi
        if [[ -z "$ALL_NODES_JOINED_TIME" ]] && [[ "$NODE_COUNT" -ge "$DEMO_REPLICAS" ]]; then ALL_NODES_JOINED_TIME=$ELAPSED; fi
        if [[ -z "$FIRST_READY_TIME" ]] && [[ "$READY_COUNT" -gt 0 ]]; then FIRST_READY_TIME=$ELAPSED; fi
        if [[ -z "$ALL_READY_TIME" ]] && [[ "$READY_COUNT" -ge "$DEMO_REPLICAS" ]]; then ALL_READY_TIME=$ELAPSED; fi

        # Clear screen area and print status
        printf "\033[2K\r"
        echo "  ── Status (${ELAPSED}s) ──────────────────────────────────"
        # Per-pool summary
        for P in alpha beta gamma; do
            P_CLAIMS=$(echo "$NC_STATUS" | awk "/fleet-${P}/" | wc -l || echo 0)
            P_READY=$(kubectl get nodes -l karpenter.sh/nodepool=fleet-${P} --no-headers 2>/dev/null | awk "/ Ready/" | wc -l || echo 0)
            P_TOTAL=$(kubectl get nodes -l karpenter.sh/nodepool=fleet-${P} --no-headers 2>/dev/null | wc -l || echo 0)
            
            # Count nodeclaims in various states for this pool
            P_VMS=$(echo "$NC_STATUS" | awk "/fleet-${P}/ && \$2 != \"<none>\" && \$2 != \"\" {print}" | wc -l || echo 0)
            
            if [[ "$P_CLAIMS" -gt 0 ]]; then
                echo "    pool=fleet-${P}: NodeClaims=${P_CLAIMS} | Nodes=${P_TOTAL} | Ready=${P_READY}"
            fi
        done
        # Show fleet resources
        FLEET_NAMES=$(az rest --method GET \
            --url "https://management.azure.com/subscriptions/${AZURE_SUBSCRIPTION_ID}/resourceGroups/${AZURE_NODE_RESOURCE_GROUP}/providers/Microsoft.AzureFleet/fleets?api-version=2024-11-01" \
            --query "value[].name" -o tsv 2>/dev/null || echo "")
        FLEET_CT=$(echo "$FLEET_NAMES" | grep -c . 2>/dev/null || echo 0)
        if [[ "$FLEET_CT" -gt 0 ]]; then
            echo "    ── Fleets (${FLEET_CT}) ──"
            echo "$FLEET_NAMES" | while read -r fn; do echo "      $fn"; done
        fi

        if [[ "$READY_COUNT" -ge "$DEMO_REPLICAS" ]]; then
            echo "  ── Ready=${READY_COUNT}/${DEMO_REPLICAS} ──────────────────────────"
            echo ""
            break
        fi
        if [[ "$ELAPSED" -ge "$TIMEOUT" ]]; then
            echo ""
            err "Timeout! Got $READY_COUNT/$DEMO_REPLICAS Ready nodes after ${ELAPSED}s."
            echo ""
            info "Debug info:"
            echo "  NodeClaims:"
            kubectl get nodeclaims -o custom-columns=NAME:.metadata.name,TYPE:.status.instanceType,NODE:.status.nodeName,READY:.status.conditions[0].status 2>/dev/null || true
            echo ""
            echo "  Karpenter logs (last 20 lines):"
            kubectl -n kube-system logs deploy/karpenter --tail=20
            exit 1
        fi
        sleep 5
    done

    echo ""
    ok "All $DEMO_REPLICAS nodes provisioned and Ready!"
    echo ""
    info "Timing Metrics:"
    echo "  - First node joined:         ${FIRST_NODE_TIME}s"
    echo "  - All nodes joined:          ${ALL_NODES_JOINED_TIME}s"
    echo "  - First node Ready:          ${FIRST_READY_TIME}s"
    echo "  - All nodes Ready:           ${ALL_READY_TIME}s"

    # Fleet summary
    echo ""
    info "Fleet Resources Created:"
    FLEETS_JSON=$(az rest --method GET \
        --url "https://management.azure.com/subscriptions/${AZURE_SUBSCRIPTION_ID}/resourceGroups/${AZURE_NODE_RESOURCE_GROUP}/providers/Microsoft.AzureFleet/fleets?api-version=2024-11-01" \
        2>/dev/null || echo '{"value":[]}')
    FLEET_COUNT=$(echo "$FLEETS_JSON" | jq '.value | length')
    echo "  Total Fleets: $FLEET_COUNT"
    echo "$FLEETS_JSON" | jq -r '.value[] | "  - \(.name) | VMs: \(.properties.targetCapacity // "N/A") | State: \(.properties.provisioningState // "N/A")"' 2>/dev/null
    echo ""

    info "Fleet LRO log (from controller):"
    kubectl -n kube-system logs deploy/karpenter --tail=5000 2>/dev/null | \
        grep -E "fleet LRO completed|listed fleet VMs|coalescing" | sed 's/^/  /' | tail -20
    echo ""

    # Show results
    echo ""
    info "Provisioned nodes:"
    kubectl get nodes -l demo=fleet -o wide
    echo ""
    info "NodeClaims:"
    kubectl get nodeclaims -o wide
    echo ""
    info "Pods (sample):"
    kubectl get pods -n default -o wide | head -12
}

###############################################################################
# Step 3b: Provision a single GPU node exercising interconnect placement fields
###############################################################################
step_provision_interconnect() {
    header "Step 3b: Provision GPU Node with Interconnect Placement (Fleet-only)"
    echo "This step applies an AKSNodeClass + NodePool exercising the interconnect"
    echo "placement fields. AZURE_INTERCONNECT_GROUP_ID is required."
    echo "AZURE_INTERCONNECT_SUBGROUP_ID and AZURE_INTERCONNECT_BLOCK_ID are optional."
    echo "sku-name requirement pinned to \$AZURE_GPU_VM_SIZE ($AZURE_GPU_VM_SIZE)."
    echo ""
    cd "$REPO_DIR"

    if [[ -z "${AZURE_INTERCONNECT_GROUP_ID:-}" ]]; then
        err "AZURE_INTERCONNECT_GROUP_ID must be set."
        echo "  Example:"
        echo "    export AZURE_INTERCONNECT_GROUP_ID=/subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.Network/interconnectGroups/<name>"
        echo "    # optional:"
        echo "    export AZURE_INTERCONNECT_SUBGROUP_ID=/subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.Network/interconnectGroups/<name>/subgroups/subgroup0"
        echo "    export AZURE_INTERCONNECT_BLOCK_ID=/subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.Compute/interconnectBlocks/<name>"
        echo "    ./hack/demo/fleet-demo.sh interconnect"
        exit 1
    fi

    # Build AKSNodeClass spec conditionally — all three fields are optional except ICG.
    ICB_FIELD=""
    ICG_SUBGROUP_FIELD=""
    if [[ -n "${AZURE_INTERCONNECT_BLOCK_ID:-}" ]]; then
        ICB_FIELD="  interconnectBlockID: \"${AZURE_INTERCONNECT_BLOCK_ID}\""
        info "interconnectBlockID set — will include in AKSNodeClass."
    else
        info "AZURE_INTERCONNECT_BLOCK_ID not set — skipping."
    fi
    if [[ -n "${AZURE_INTERCONNECT_SUBGROUP_ID:-}" ]]; then
        ICG_SUBGROUP_FIELD="  interconnectSubgroupID: \"${AZURE_INTERCONNECT_SUBGROUP_ID}\""
        info "interconnectSubgroupID set — will include in AKSNodeClass."
    else
        info "AZURE_INTERCONNECT_SUBGROUP_ID not set — skipping."
    fi

    info "Applying AKSNodeClass 'gpu-interconnect'..."
    kubectl apply -f - <<EOF
apiVersion: karpenter.azure.com/v1beta1
kind: AKSNodeClass
metadata:
  name: gpu-interconnect
  annotations:
    kubernetes.io/description: "Fleet demo - GPU nodes pinned to an interconnect topology"
spec:
  imageFamily: Ubuntu2204
${ICB_FIELD}
  interconnectGroupID: "${AZURE_INTERCONNECT_GROUP_ID}"
${ICG_SUBGROUP_FIELD}
EOF
    ok "AKSNodeClass 'gpu-interconnect' applied."

    info "Applying NodePool 'fleet-gpu-interconnect' (sku-name=${AZURE_GPU_VM_SIZE})..."
    kubectl apply -f - <<EOF
apiVersion: karpenter.sh/v1
kind: NodePool
metadata:
  name: fleet-gpu-interconnect
  annotations:
    kubernetes.io/description: "Fleet demo NodePool for GPU interconnect placement"
spec:
  disruption:
    consolidationPolicy: WhenEmptyOrUnderutilized
    consolidateAfter: Never
    budgets:
      - nodes: "0"
  limits:
    cpu: "2000"
    memory: 4000Gi
  template:
    metadata:
      labels:
        kubernetes.azure.com/ebpf-dataplane: cilium
        demo: fleet-interconnect
    spec:
      expireAfter: Never
      startupTaints:
        - key: node.cilium.io/agent-not-ready
          effect: NoExecute
          value: "true"
      requirements:
        - key: kubernetes.io/arch
          operator: In
          values: ["arm64"]
        - key: kubernetes.io/os
          operator: In
          values: ["linux"]
        - key: karpenter.sh/capacity-type
          operator: In
          values: ["on-demand"]
        - key: karpenter.azure.com/sku-name
          operator: In
          values: ["${AZURE_GPU_VM_SIZE}"]
      nodeClassRef:
        group: karpenter.azure.com
        kind: AKSNodeClass
        name: gpu-interconnect
EOF
    ok "NodePool 'fleet-gpu-interconnect' applied."

    info "Deploying 1 pod to trigger 1 GPU node..."
    kubectl apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fleet-inflate-gpu-interconnect
  namespace: default
spec:
  replicas: 1
  selector:
    matchLabels:
      app: fleet-inflate-gpu-interconnect
  template:
    metadata:
      labels:
        app: fleet-inflate-gpu-interconnect
        demo: fleet-interconnect
    spec:
      nodeSelector:
        karpenter.azure.com/sku-name: "${AZURE_GPU_VM_SIZE}"
      tolerations:
        - key: node.cilium.io/agent-not-ready
          operator: Exists
          effect: NoExecute
          tolerationSeconds: 120
      containers:
        - name: pause
          image: mcr.microsoft.com/oss/kubernetes/pause:3.6
          resources:
            requests:
              cpu: "1"
              memory: 256Mi
EOF
    ok "Deployment applied."

    echo ""
    wait_msg "Waiting up to 10 minutes for the NodeClaim to become Ready..."
    if kubectl wait nodeclaim -l karpenter.sh/nodepool=fleet-gpu-interconnect \
        --for=condition=Ready --timeout=600s 2>/dev/null; then
        ok "GPU interconnect NodeClaim is Ready."
    else
        err "Timed out waiting for the GPU interconnect NodeClaim to become Ready."
    fi

    echo ""
    info "NodeClaim status:"
    kubectl get nodeclaims -l karpenter.sh/nodepool=fleet-gpu-interconnect -o wide
    echo ""
    info "To verify the Fleet resource body includes the interconnect properties, run:"
    echo "  ./hack/demo/fleet-demo.sh observe"
    echo "  (then inspect the saved Fleet PUT body for interconnectBlockProfile / networkProfile.interconnectGroupProfile)"
}

###############################################################################
# Step 4: Observe — Fleet resources & request bodies
###############################################################################
step_observe() {
    header "Step 4: Observe Fleet Resources & Request Bodies"
    echo "This step queries Azure for the Fleet resources that Karpenter created,"
    echo "retrieves their full JSON bodies (the actual ARM request/response), and"
    echo "shows the VMs that were provisioned through them."
    echo ""

    # List Fleet resources in the MC resource group
    info "Querying Fleet resources in resource group '$AZURE_NODE_RESOURCE_GROUP'..."
    FLEETS_JSON=$(az rest --method GET \
        --url "https://management.azure.com/subscriptions/${AZURE_SUBSCRIPTION_ID}/resourceGroups/${AZURE_NODE_RESOURCE_GROUP}/providers/Microsoft.AzureFleet/fleets?api-version=2024-11-01" \
        2>/dev/null || echo '{"value":[]}')

    FLEET_COUNT=$(echo "$FLEETS_JSON" | jq '.value | length')
    ok "Found $FLEET_COUNT Fleet resource(s)"
    echo ""

    # Save full list
    echo "$FLEETS_JSON" | jq '.' > "$LOG_DIR/fleets-list.json"
    info "Full list saved: $LOG_DIR/fleets-list.json"

    # For each fleet, show details
    echo ""
    echo "$FLEETS_JSON" | jq -r '.value[].name' | while read -r FLEET_NAME; do
        info "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        info "Fleet: $FLEET_NAME"
        info "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

        # Get full fleet body
        FLEET_BODY=$(az rest --method GET \
            --url "https://management.azure.com/subscriptions/${AZURE_SUBSCRIPTION_ID}/resourceGroups/${AZURE_NODE_RESOURCE_GROUP}/providers/Microsoft.AzureFleet/fleets/${FLEET_NAME}?api-version=2024-11-01" \
            2>/dev/null)

        # Save to file
        FLEET_FILE="$LOG_DIR/fleet-${FLEET_NAME}.json"
        echo "$FLEET_BODY" | jq '.' > "$FLEET_FILE"
        ok "Full Fleet body saved: $FLEET_FILE"

        # Show summary
        echo ""
        echo "  Key properties:"
        echo "$FLEET_BODY" | jq '{
            name: .name,
            location: .location,
            provisioningState: .properties.provisioningState,
            vmSizesProfile: .properties.vmSizesProfile,
            regularPriorityProfile: .properties.regularPriorityProfile,
            spotPriorityProfile: .properties.spotPriorityProfile,
            computeApiVersion: .properties.computeProfile.computeApiVersion,
            vmNamePrefix: .properties.computeProfile.baseVirtualMachineProfile.osProfile.computerNamePrefix,
            imageReference: .properties.computeProfile.baseVirtualMachineProfile.storageProfile.imageReference,
            networkConfig: .properties.computeProfile.baseVirtualMachineProfile.networkProfile.networkInterfaceConfigurations[0].properties.ipConfigurations[0].properties.subnet,
            tags: .tags
        }' | sed 's/^/  /'
        echo ""
    done

    # Karpenter logs
    echo ""
    info "Karpenter controller logs (fleet/batch related):"
    echo "  (grep for 'fleet', 'batch', 'launched', 'provision')"
    echo ""
    kubectl -n kube-system logs deploy/karpenter --tail=300 2>/dev/null | \
        grep -i "fleet\|batch\|launched\|provision\|created fleet\|assignment" | tail -30 | sed 's/^/  /'
    echo ""

    # Save full Karpenter logs
    KARPENTER_LOG="$LOG_DIR/karpenter-logs.txt"
    kubectl -n kube-system logs deploy/karpenter > "$KARPENTER_LOG" 2>/dev/null
    ok "Full Karpenter logs: $KARPENTER_LOG"

    # Activity Log
    echo ""
    info "Azure Activity Log — Fleet operations (last 1 hour):"
    echo "  Shows the actual ARM operations (PUT/GET/DELETE) on Fleet resources."
    echo ""
    az monitor activity-log list \
        --resource-group "$AZURE_NODE_RESOURCE_GROUP" \
        --offset 1h \
        --query "[?contains(resourceType.value || '', 'AzureFleet')].{operation:operationName.value, status:status.value, time:eventTimestamp}" \
        --output table 2>/dev/null || info "  (Activity log not yet available — takes ~5 min to propagate)"

    # VMs
    echo ""
    info "Fleet-provisioned VMs in Azure (with Karpenter tags):"
    echo ""
    az vm list --resource-group "$AZURE_NODE_RESOURCE_GROUP" \
        --query "[?tags.\"karpenter.azure.com_managed-by\"=='karpenter'].{Name:name, Size:hardwareProfile.vmSize, Zone:zones[0], NodePool:tags.\"karpenter.sh_nodepool\"}" \
        --output table 2>/dev/null

    # Provider ID verification
    echo ""
    info "Provider ID verification (confirms IMDS-based provider-id is working):"
    echo "  Node providerID must match NodeClaim providerID for Karpenter to link them."
    echo ""
    echo "  Sample (first 3 nodes):"
    kubectl get nodes -l demo=fleet -o jsonpath='{range .items[*]}  {.metadata.name} → {.spec.providerID}{"\n"}{end}' 2>/dev/null | head -3

    echo ""
    echo ""
    ok "All observation artifacts saved in: $LOG_DIR/"
    echo ""
    ls -la "$LOG_DIR/"
}

###############################################################################
# Step 5: Cleanup
###############################################################################
step_cleanup() {
    header "Step 5: Cleanup"
    echo "This step removes the demo workload, NodePool, and AKSNodeClass."
    echo "Karpenter will delete the underlying VMs via its finalizer on NodeClaims."
    echo ""

    info "Scaling down all workloads..."
    kubectl scale deployments --all --replicas=0 -n default 2>/dev/null || true

    echo ""
    info "Deleting Kubernetes resources (deployments, nodepools, nodeclass)..."
    kubectl delete deployments --all -n default --ignore-not-found
    kubectl delete nodepools --all --ignore-not-found
    kubectl delete aksnodeclasses default --ignore-not-found
    # kubectl delete aksnodeclasses gpu-interconnect --ignore-not-found  # interconnect disabled
    ok "Kubernetes resources deleted."

    echo ""
    info "Karpenter is now processing NodeClaim deletions (VM cleanup via finalizers)..."
    wait_msg "Waiting 10s for Karpenter to begin processing..."
    sleep 10
    kubectl delete nodeclaims --all --ignore-not-found 2>/dev/null || true

    echo ""
    wait_msg "Waiting for all fleet-demo nodes to be removed (timeout: 5 minutes)..."
    TIMEOUT=300
    ELAPSED=0
    while true; do
        NODE_COUNT=$(kubectl get nodes -l demo=fleet --no-headers 2>/dev/null | wc -l)
        if [[ "$NODE_COUNT" -eq 0 ]]; then
            echo ""
            break
        fi
        if [[ "$ELAPSED" -ge "$TIMEOUT" ]]; then
            echo ""
            err "Timeout! $NODE_COUNT nodes still remain after ${ELAPSED}s."
            echo "  You may need to manually delete VMs in $AZURE_NODE_RESOURCE_GROUP."
            break
        fi
        printf "\r  Nodes remaining: %d (elapsed: %ds)  " "$NODE_COUNT" "$ELAPSED"
        sleep 10
        ELAPSED=$((ELAPSED + 10))
    done

    # Delete Fleet resources in the node resource group
    echo ""
    info "Deleting Fleet resources in '$AZURE_NODE_RESOURCE_GROUP'..."
    FLEET_NAMES=$(az rest --method GET \
        --url "https://management.azure.com/subscriptions/${AZURE_SUBSCRIPTION_ID}/resourceGroups/${AZURE_NODE_RESOURCE_GROUP}/providers/Microsoft.AzureFleet/fleets?api-version=2024-11-01" \
        --query "value[].name" -o tsv 2>/dev/null || echo "")
    if [[ -n "$FLEET_NAMES" ]]; then
        echo "$FLEET_NAMES" | while read -r FLEET_NAME; do
            info "  Deleting fleet: $FLEET_NAME"
            az rest --method DELETE \
                --url "https://management.azure.com/subscriptions/${AZURE_SUBSCRIPTION_ID}/resourceGroups/${AZURE_NODE_RESOURCE_GROUP}/providers/Microsoft.AzureFleet/fleets/${FLEET_NAME}?api-version=2024-11-01" \
                2>/dev/null || true
        done
        ok "Fleet resources deleted."
    else
        info "No Fleet resources found."
    fi

    ok "Cleanup complete. All demo resources removed."
}

###############################################################################
# Main
###############################################################################
STEP="${1:-all}"

case "$STEP" in
    setup)        step_setup ;;
    deploy)       step_deploy ;;
    provision)    step_provision ;;
    interconnect) step_provision_interconnect ;;
    observe)      step_observe ;;
    cleanup)      step_cleanup ;;
    all)
        step_setup
        step_deploy
        step_provision
        step_observe
        ;;
    *)
        echo "Usage: $0 {setup|deploy|provision|interconnect|observe|cleanup|all}"
        echo ""
        echo "Steps:"
        echo "  setup        Create Azure infra (RG, MSI, ACR, AKS, roles)"
        echo "  deploy       Build Karpenter image and deploy with PROVISION_MODE=fleet"
        echo "  provision    Apply NodePool + workload to trigger 10 Fleet-provisioned nodes"
        echo "  interconnect Apply a GPU NodePool exercising interconnectBlockID/GroupID/SubgroupID"
        echo "               (requires AZURE_INTERCONNECT_BLOCK_ID/GROUP_ID/SUBGROUP_ID env vars)"
        echo "  observe      Query Fleet resources, save request bodies, show VM details"
        echo "  cleanup      Remove workload and nodes"
        echo "  all          Run setup → deploy → provision → observe (no cleanup)"
        exit 1
        ;;
esac

echo ""
ok "Done! All logs and Fleet bodies saved in: $LOG_DIR/"
