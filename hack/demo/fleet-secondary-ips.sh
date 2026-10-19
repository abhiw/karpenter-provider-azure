#!/usr/bin/env bash
# Sourced only by fleet-demo.sh secondary-* steps. Never runs the general demo's
# broad provisioning/cleanup. Use a dedicated test cluster; setup/deploy cost money.
# Subscription and resource group default in fleet-demo.sh; environment overrides
# take precedence. Keep the same cluster, registry and overrides across steps.
# SECONDARY_NODES=1 is a smoke test; default 2 also checks cross-node networking.
# SECONDARY_MAX_PODS defaults to 30; SECONDARY_VM_SIZE defaults to Standard_D4ads_v5.
# SECONDARY_IMAGE must provide sh, httpd, wget, nslookup (default busybox:1.36.1).

SECONDARY_NAME=fleet-secondary-ips
SECONDARY_NODES="${SECONDARY_NODES:-2}"
SECONDARY_MAX_PODS="${SECONDARY_MAX_PODS:-30}"
SECONDARY_VM_SIZE="${SECONDARY_VM_SIZE:-Standard_D4ads_v5}"
SECONDARY_IMAGE="${SECONDARY_IMAGE:-busybox:1.36.1}"
SECONDARY_TIMEOUT=900

secondary_fail() {
    err "$*"
    return 1
}

secondary_validate_cluster() {
    local cluster
    cluster=$(az aks show --subscription "$AZURE_SUBSCRIPTION_ID" \
        --resource-group "$AZURE_RESOURCE_GROUP" --name "$AZURE_CLUSTER_NAME" --output json)
    if ! jq -e '
        .networkProfile.networkPlugin == "azure" and
        ((.networkProfile.networkPluginMode // "") | . == "" or . == "none") and
        ((.networkProfile.networkDataplane // "azure") == "azure") and
        all(.agentPoolProfiles[]; (.podSubnetId // "") == "")
    ' <<< "$cluster" >/dev/null; then
        secondary_fail "Requires Azure CNI node-subnet networking, no Overlay, pod subnet or Cilium. Use a separate cluster."
        return 1
    fi
    AZURE_NODE_RESOURCE_GROUP=$(jq -er '.nodeResourceGroup' <<< "$cluster")
    SECONDARY_SUBNET=$(jq -r '.agentPoolProfiles[0].vnetSubnetId // empty' <<< "$cluster")
    if [[ -z "$SECONDARY_SUBNET" ]]; then
        info "Resolving the AKS-managed node subnet from the node resource group."
        SECONDARY_SUBNET=$(az network vnet list --subscription "$AZURE_SUBSCRIPTION_ID" \
            --resource-group "$AZURE_NODE_RESOURCE_GROUP" --output json |
            jq -er '[.[].subnets[] | select(.name == "aks-subnet") | .id] |
                if length == 1 then .[0] else error("Expected exactly one AKS-managed node subnet") end')
    fi
    SECONDARY_FQDN=$(jq -er '.fqdn // .privateFqdn' <<< "$cluster")
}

secondary_validate_context() {
    local server
    server=$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')
    [[ "$server" == "https://$SECONDARY_FQDN" || "$server" == "https://$SECONDARY_FQDN:443" ]] ||
        secondary_fail "kubectl context does not point at the selected test cluster."
}

secondary_validate_controller() {
    local deployment
    deployment=$(kubectl -n kube-system get deployment karpenter -o json)
    jq -e --arg subnet "$SECONDARY_SUBNET" '
        [.spec.template.spec.containers[].env[]?] |
        any(.[]; .name == "PROVISION_MODE" and .value == "fleet") and
        any(.[]; .name == "NETWORK_PLUGIN" and .value == "azure") and
        all(.[]; .name != "NETWORK_PLUGIN_MODE" or ((.value // "") == "")) and
        all(.[]; .name != "NETWORK_DATAPLANE" or ((.value // "azure") != "cilium")) and
        any(.[]; .name == "VNET_SUBNET_ID" and
            ((.value | ascii_downcase) == ($subnet | ascii_downcase)))
    ' <<< "$deployment" >/dev/null ||
        secondary_fail "Controller must use Fleet with NETWORK_PLUGIN=azure and empty NETWORK_PLUGIN_MODE."
}

secondary_owned() {
    local kind=$1 name=$2 existing
    existing=$(kubectl get "$kind" "$name" --ignore-not-found -o json)
    if [[ -n "$existing" ]]; then
        jq -e --arg name "$SECONDARY_NAME" '.metadata.labels["fleet-demo-scenario"] == $name' \
            <<< "$existing" >/dev/null ||
            secondary_fail "Refusing to modify existing unowned $kind/$name."
    fi
}

secondary_provision() {
    secondary_validate_controller
    secondary_owned namespace "$SECONDARY_NAME"
    secondary_owned nodepool "$SECONDARY_NAME"
    secondary_owned aksnodeclass "$SECONDARY_NAME"
    kubectl apply -f - <<EOF
apiVersion: v1
kind: Namespace
metadata:
  name: ${SECONDARY_NAME}
  labels:
    fleet-demo-scenario: ${SECONDARY_NAME}
---
apiVersion: karpenter.azure.com/v1beta1
kind: AKSNodeClass
metadata:
  name: ${SECONDARY_NAME}
  labels:
    fleet-demo-scenario: ${SECONDARY_NAME}
spec:
  imageFamily: Ubuntu2204
  maxPods: ${SECONDARY_MAX_PODS}
  vnetSubnetID: "${SECONDARY_SUBNET}"
---
apiVersion: karpenter.sh/v1
kind: NodePool
metadata:
  name: ${SECONDARY_NAME}
  labels:
    fleet-demo-scenario: ${SECONDARY_NAME}
spec:
  limits:
    cpu: "16"
  disruption:
    consolidationPolicy: WhenEmptyOrUnderutilized
    consolidateAfter: Never
    budgets:
      - nodes: "0"
  template:
    metadata:
      labels:
        fleet-demo-scenario: ${SECONDARY_NAME}
    spec:
      expireAfter: Never
      taints:
        - key: fleet-demo-scenario
          value: ${SECONDARY_NAME}
          effect: NoSchedule
      nodeClassRef:
        group: karpenter.azure.com
        kind: AKSNodeClass
        name: ${SECONDARY_NAME}
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
        - key: node.kubernetes.io/instance-type
          operator: In
          values: ["${SECONDARY_VM_SIZE}"]
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: server
  namespace: ${SECONDARY_NAME}
spec:
  replicas: ${SECONDARY_NODES}
  selector:
    matchLabels:
      app: secondary-server
  template:
    metadata:
      labels:
        app: secondary-server
    spec:
      hostNetwork: false
      nodeSelector:
        karpenter.sh/nodepool: ${SECONDARY_NAME}
      tolerations:
        - key: fleet-demo-scenario
          operator: Equal
          value: ${SECONDARY_NAME}
          effect: NoSchedule
      affinity:
        podAntiAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            - labelSelector:
                matchLabels:
                  app: secondary-server
              topologyKey: kubernetes.io/hostname
      containers:
        - name: http
          image: "${SECONDARY_IMAGE}"
          command: ["sh", "-c", "mkdir -p /www; echo fleet-secondary-ips > /www/index.html; exec httpd -f -p 8080 -h /www"]
          resources:
            requests:
              cpu: 100m
              memory: 32Mi
          readinessProbe:
            httpGet:
              path: /
              port: 8080
---
apiVersion: v1
kind: Service
metadata:
  name: server
  namespace: ${SECONDARY_NAME}
spec:
  selector:
    app: secondary-server
  ports:
    - port: 8080
      targetPort: 8080
---
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: probe
  namespace: ${SECONDARY_NAME}
spec:
  selector:
    matchLabels:
      app: secondary-probe
  template:
    metadata:
      labels:
        app: secondary-probe
    spec:
      hostNetwork: false
      nodeSelector:
        karpenter.sh/nodepool: ${SECONDARY_NAME}
      tolerations:
        - key: fleet-demo-scenario
          operator: Equal
          value: ${SECONDARY_NAME}
          effect: NoSchedule
      containers:
        - name: probe
          image: "${SECONDARY_IMAGE}"
          command: ["sh", "-c", "while true; do sleep 3600; done"]
          resources:
            requests:
              cpu: 50m
              memory: 32Mi
EOF
    kubectl -n "$SECONDARY_NAME" rollout status deployment/server --timeout="${SECONDARY_TIMEOUT}s"
    kubectl -n "$SECONDARY_NAME" rollout status daemonset/probe --timeout="${SECONDARY_TIMEOUT}s"
}

# Only persist resource identifiers, never customData or extension settings.
secondary_inventory() {
    local vms
    vms=$(az vm list --subscription "$AZURE_SUBSCRIPTION_ID" \
        --resource-group "$AZURE_NODE_RESOURCE_GROUP" \
        --query "[].{id:id,tags:tags,nics:networkProfile.networkInterfaces[].id,osDisk:storageProfile.osDisk.managedDisk.id,dataDisks:storageProfile.dataDisks[].managedDisk.id}" --output json)
    jq --arg pool "$SECONDARY_NAME" '[.[] | select(.tags["karpenter.sh_nodepool"] == $pool)]' <<< "$vms"
}

secondary_verify_nics() {
    local nodes node provider vm_id vm nic_id nic count
    nodes=$(kubectl get nodes -l "karpenter.sh/nodepool=$SECONDARY_NAME" -o json)
    jq -e --argjson expected "$SECONDARY_NODES" '.items | length == $expected' <<< "$nodes" >/dev/null ||
        { secondary_fail "Expected exactly $SECONDARY_NODES test nodes."; return 1; }
    while IFS=$'\t' read -r node provider; do
        kubectl wait --for=condition=Ready "node/$node" --timeout=120s
        vm_id="${provider#azure://}"
        vm=$(az vm show --ids "$vm_id" --query '{id:id,tags:tags,nics:networkProfile.networkInterfaces[].id}' --output json)
        jq -e --arg pool "$SECONDARY_NAME" '.tags["karpenter.sh_nodepool"] == $pool and (.nics | length == 1)' \
            <<< "$vm" >/dev/null || { secondary_fail "Unexpected VM/NIC ownership for $node."; return 1; }
        nic_id=$(jq -er '.nics[0]' <<< "$vm")
        nic=$(az network nic show --ids "$nic_id" --output json)
        count="$SECONDARY_MAX_PODS"
        if ! jq -e --arg subnet "$SECONDARY_SUBNET" --argjson count "$count" '
            .ipConfigurations as $ips |
            ($ips | length) == $count and
            ([$ips[].name] | unique | length) == $count and
            ([$ips[].privateIPAddress] | unique | length) == $count and
            ([$ips[] | select(.primary == true)] | length) == 1 and
            all($ips[];
                (.subnet.id | ascii_downcase) == ($subnet | ascii_downcase) and
                (.privateIPAddress // "" | test("^[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+$")) and
                (.privateIPAllocationMethod == "Dynamic") and
                (if .primary then true else (.loadBalancerBackendAddressPools // [] | length) == 0 end))
        ' <<< "$nic" >/dev/null; then
            secondary_fail "NIC layout mismatch on $node ($nic_id)."
            return 1
        fi
        jq '{id, ipConfigurations: [.ipConfigurations[] | {name, primary, privateIPAddress, privateIPAllocationMethod, subnet, loadBalancerBackendAddressPools}]}' \
            <<< "$nic" > "$SECONDARY_LOG_DIR/$node-nic.json"
        ok "$node: $count allocated IPv4 configurations, one primary, secondary IPs excluded from LB pools."
    done < <(jq -r '.items[] | [.metadata.name, .spec.providerID] | @tsv' <<< "$nodes")
}

secondary_verify_pods() {
    local pods servers probes pod ip service_ip node
    kubectl -n "$SECONDARY_NAME" rollout status deployment/server --timeout="${SECONDARY_TIMEOUT}s"
    kubectl -n "$SECONDARY_NAME" rollout status daemonset/probe --timeout="${SECONDARY_TIMEOUT}s"
    pods=$(kubectl -n "$SECONDARY_NAME" get pods -o json)
    jq -e --argjson count "$SECONDARY_NODES" '
        [.items[] | select(.metadata.deletionTimestamp == null)] as $pods |
        [$pods[] | select(.metadata.labels.app == "secondary-server")] as $servers |
        [$pods[] | select(.metadata.labels.app == "secondary-probe")] as $probes |
        ($servers | length) == $count and ($probes | length) == $count and
        ([$servers[].spec.nodeName] | unique | length) == $count and
        ([$servers[].spec.nodeName] | sort) == ([$probes[].spec.nodeName] | sort) and
        ([$pods[].status.podIP] | unique | length) == ($pods | length) and
        all($pods[]; .spec.hostNetwork != true and .status.podIP != .status.hostIP and
            (.status.podIP // "" | length) > 0)
    ' <<< "$pods" >/dev/null || { secondary_fail "Expected server and probe pods with distinct pod IPs on each test node."; return 1; }
    while IFS=$'\t' read -r pod node ip; do
        jq -e --arg ip "$ip" 'any(.ipConfigurations[]; .primary != true and .privateIPAddress == $ip)' \
            "$SECONDARY_LOG_DIR/$node-nic.json" >/dev/null ||
            { secondary_fail "$pod did not receive an address from $node's secondary NIC configurations."; return 1; }
    done < <(jq -r '.items[] | select(.metadata.deletionTimestamp == null) |
        [.metadata.name, .spec.nodeName, .status.podIP] | @tsv' <<< "$pods")
    servers=$(jq -r '.items[] | select(.metadata.labels.app == "secondary-server" and .metadata.deletionTimestamp == null) | .status.podIP' <<< "$pods")
    probes=$(jq -r '.items[] | select(.metadata.labels.app == "secondary-probe" and .metadata.deletionTimestamp == null) | .metadata.name' <<< "$pods")
    service_ip=$(kubectl -n "$SECONDARY_NAME" get service server -o jsonpath='{.spec.clusterIP}')
    while read -r pod; do
        kubectl -n "$SECONDARY_NAME" exec "$pod" -- nslookup kubernetes.default.svc.cluster.local
        for ip in $servers "$service_ip" "server.${SECONDARY_NAME}.svc.cluster.local"; do
            kubectl -n "$SECONDARY_NAME" exec "$pod" -- sh -ec \
                'test "$(wget -T 10 -qO- "$1")" = fleet-secondary-ips' sh "http://$ip:8080/"
        done
    done <<< "$probes"
    kubectl -n "$SECONDARY_NAME" get pods -o wide
    ok "DNS, same-node, Service and all test-node pod paths passed."
}

secondary_cleanup() {
    local inventory resources remaining deadline id fleets
    secondary_owned namespace "$SECONDARY_NAME"
    secondary_owned nodepool "$SECONDARY_NAME"
    secondary_owned aksnodeclass "$SECONDARY_NAME"
    inventory=$(secondary_inventory)
    if [[ -f "$SECONDARY_LOG_DIR/inventory.json" ]]; then
        inventory=$(jq -s 'add | unique_by(.id)' "$SECONDARY_LOG_DIR/inventory.json" <(printf '%s' "$inventory"))
    fi
    printf '%s\n' "$inventory" > "$SECONDARY_LOG_DIR/inventory.json"
    # Stop only this scenario; finalizers handle normal VM/NIC/disk deletion.
    kubectl delete namespace "$SECONDARY_NAME" --ignore-not-found --timeout=180s
    kubectl delete nodepool "$SECONDARY_NAME" --ignore-not-found --wait=false
    kubectl delete nodeclaims -l "karpenter.sh/nodepool=$SECONDARY_NAME" --ignore-not-found --timeout="${SECONDARY_TIMEOUT}s"
    kubectl delete aksnodeclass "$SECONDARY_NAME" --ignore-not-found --timeout=180s
    resources=$(jq '[.[] | .id, .nics[]?, .osDisk, .dataDisks[]? | select(. != null) | ascii_downcase] | unique' <<< "$inventory")
    deadline=$((SECONDS + SECONDARY_TIMEOUT))
    while true; do
        remaining=$(az resource list --subscription "$AZURE_SUBSCRIPTION_ID" \
            --resource-group "$AZURE_NODE_RESOURCE_GROUP" --output json |
            jq --argjson ids "$resources" --arg pool "$SECONDARY_NAME" \
            '[.[] | select((.id | ascii_downcase) as $id | ($ids | index($id)) != null or
                (.tags["karpenter.sh_nodepool"] == $pool and (.type | ascii_downcase) != "microsoft.azurefleet/fleets")) | .id]')
        [[ "$remaining" == "[]" ]] && break
        if (( SECONDS >= deadline )); then
            secondary_fail "Cleanup timed out; remaining resource IDs: $remaining"
            return 1
        fi
        sleep 10
    done
    fleets=$(az resource list --subscription "$AZURE_SUBSCRIPTION_ID" --resource-group "$AZURE_NODE_RESOURCE_GROUP" \
        --resource-type Microsoft.AzureFleet/fleets --output json |
        jq -r --arg pool "$SECONDARY_NAME" '.[] | select(.tags["karpenter.sh_nodepool"] == $pool) | .id')
    while read -r id; do
        [[ -z "$id" ]] && continue
        az resource delete --ids "$id" --api-version 2026-06-01-preview --output none
    done <<< "$fleets"
    ok "Scenario resources removed; test cluster, registry and identity retained."
}

secondary_main() {
    [[ "$SECONDARY_NODES" =~ ^[12]$ ]] || { secondary_fail "SECONDARY_NODES must be 1 or 2."; return 1; }
    [[ "$SECONDARY_MAX_PODS" =~ ^[1-9][0-9]*$ ]] &&
        (( SECONDARY_MAX_PODS >= 10 && SECONDARY_MAX_PODS <= 250 )) ||
        { secondary_fail "SECONDARY_MAX_PODS must be 10..250 for this demo."; return 1; }
    [[ "$SECONDARY_VM_SIZE" =~ ^Standard_[A-Za-z0-9_]+$ ]] ||
        { secondary_fail "Invalid SECONDARY_VM_SIZE."; return 1; }
    [[ "$SECONDARY_IMAGE" =~ ^[a-zA-Z0-9./:@_-]+$ ]] ||
        { secondary_fail "Invalid SECONDARY_IMAGE."; return 1; }
    SECONDARY_LOG_DIR="$LOG_DIR/secondary-$AZURE_RESOURCE_GROUP-$AZURE_CLUSTER_NAME"
    mkdir -p "$SECONDARY_LOG_DIR"
    az account set --subscription "$AZURE_SUBSCRIPTION_ID"
    case "$1" in
        secondary-setup)
            local clusters
            clusters=$(az aks list --subscription "$AZURE_SUBSCRIPTION_ID" --output json)
            if jq -e --arg rg "$AZURE_RESOURCE_GROUP" --arg name "$AZURE_CLUSTER_NAME" \
                'any(.[]; (.resourceGroup | ascii_downcase) == ($rg | ascii_downcase) and .name == $name)' <<< "$clusters" >/dev/null; then
                secondary_validate_cluster
            fi
            step_setup
            secondary_validate_cluster
            ;;
        secondary-deploy)
            secondary_validate_cluster
            secondary_validate_context
            step_deploy
            secondary_validate_controller
            ;;
        secondary-provision|secondary-verify|secondary-cleanup)
            secondary_validate_cluster
            secondary_validate_context
            case "$1" in
                secondary-provision) secondary_provision ;;
                secondary-verify)
                    secondary_validate_controller
                    local inventory
                    inventory=$(secondary_inventory)
                    if [[ -f "$SECONDARY_LOG_DIR/inventory.json" ]]; then
                        inventory=$(jq -s 'add | unique_by(.id)' "$SECONDARY_LOG_DIR/inventory.json" <(printf '%s' "$inventory"))
                    fi
                    printf '%s\n' "$inventory" > "$SECONDARY_LOG_DIR/inventory.json"
                    secondary_verify_nics
                    secondary_verify_pods
                    kubectl -n "$SECONDARY_NAME" rollout restart daemonset/probe
                    secondary_verify_pods
                    ;;
                secondary-cleanup) secondary_cleanup ;;
            esac
            ;;
        *) secondary_fail "Unknown secondary-IP step: $1"; return 1 ;;
    esac
}
