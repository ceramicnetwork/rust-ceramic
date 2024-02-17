#!/bin/bash
set -euxo pipefail

SIM_NAME=${JOB_NAMESPACE}-$(date +%Y-%m-%d-%H)
export SIM_NAME
NETWORK_NAME=${JOB_NAMESPACE}-$(date +%Y-%m-%d-%H)
export NETWORK_NAME
NETWORK_NAMESPACE=keramik-${NETWORK_NAME}
export NETWORK_NAMESPACE

curl -L https://github.com/mikefarah/yq/releases/download/v4.40.7/yq_linux_amd64 -o yq
chmod +x yq

# Customize the network name
./yq -e '.metadata.name = env(NETWORK_NAME)' /config/network.yaml > network.yaml

cat network.yaml

# If an affintiy file exists, apply it to the network.yaml
if [ -n "$AFFINITY_FILE" ] && [ -f "$AFFINITY_FILE" ]; then
  ./yq eval '.spec += load(env(AFFINITY_FILE))' network.yaml > network-merged.yaml
else
  cp network.yaml network-merged.yaml
fi

cat network-merged.yaml

# Apply the network
kubectl apply -f network-merged.yaml
kubectl apply -n "${NETWORK_NAMESPACE}" -f /config/podmonitors.yaml

# Wait for CAS to be ready
while true; do
  sleep 10
  READY_REPLICAS=$(kubectl get statefulset cas -n "${NETWORK_NAMESPACE}" -o jsonpath='{.status.readyReplicas}')
  if [[ "$READY_REPLICAS" -ge 1 ]]; then
    echo "StatefulSet 'cas' has at least 1 ready replica."
    break
  else
    echo "Waiting for StatefulSet 'cas' to have at least 1 ready replica..."
  fi
done

# Label the workload with a storageClass, if it exists
STORAGECLASS_NAME=$(./yq '.spec.ceramic[0].ipfs.rust.storageClass' network-merged.yaml)
if [ -n "$STORAGECLASS_NAME" ]; then
  kubectl label pods -l app=ceramic storageClass="$STORAGECLASS_NAME" -n "${NETWORK_NAMESPACE}"
  kubectl label pods -l app=otel storageClass="$STORAGECLASS_NAME" -n "${NETWORK_NAMESPACE}"
fi

# Label the workload with an affinity tag, if it exists
if [ -n "$AFFINITY_TAG" ]; then
  kubectl label pods -l app=ceramic affinity="$AFFINITY_TAG" -n "${NETWORK_NAMESPACE}"
  kubectl label pods -l app=otel affinity="$AFFINITY_TAG" -n "${NETWORK_NAMESPACE}"
fi

sleep 300 # wait for the network to stabilize and bootstrap
kubectl wait --for=condition=complete job/bootstrap -n "${NETWORK_NAMESPACE}"

./yq -e '.metadata.name = env(SIM_NAME), .metadata.namespace = env(NETWORK_NAMESPACE)' \
  /config/sim.yaml > simulation.yaml
kubectl apply -f simulation.yaml
SIMULATION_RUNTIME=$(./yq e '.spec.runTime' simulation.yaml)
sleep $((SIMULATION_RUNTIME * 60))

KERAMIK_SIMULATE_NAME=$(kubectl get job simulate-manager \
  -o jsonpath='{.spec.template.spec.containers[?(@.name=="manager")].env[?(@.name=="SIMULATE_NAME")].value}' -n "${NETWORK_NAMESPACE}")

# why is this not working?
# Maybe loop and watch for conditions
sleep 300 # wait for the simulation to finish
kubectl  get job simulate-manager -n "${NETWORK_NAMESPACE}" -o jsonpath='{.status}'

SUCCEEDED=$(kubectl  get job simulate-manager -n "${NETWORK_NAMESPACE}" -o jsonpath='{.status.succeeded}')
FAILED=$(kubectl  get job simulate-manager -n "${NETWORK_NAMESPACE}" -o jsonpath='{.status.failed}')
if [[ "$SUCCEEDED" -gt 0 ]]; then
  SIMULATION_STATUS_TAG="succeeded"
  if [[ -n $RETAIN_NETWORK ]] && [[ $RETAIN_NETWORK -eq 1 ]]; then
    kubectl delete -f network-merged.yaml
  fi
elif [[ "$FAILED" -gt 0 ]]; then
  SIMULATION_STATUS_TAG="failed"
else
  SIMULATION_STATUS_TAG="unknown"
fi

ANNOTATION=$(cat <<EOF
{
  "tags": ["nightly-performance","$CLUSTER_NAME","$KERAMIK_SIMULATE_NAME","$NETWORK_NAMESPACE","$SIMULATION_STATUS_TAG"],
  "text": "text about the test"
}
EOF
)

curl -H "Content-Type: application/json" -X POST \
  https://threebox.grafana.net/api/annotations \
  -H "Authorization: Bearer $GRAFANA_API_KEY" \
  -d "$ANNOTATION"
