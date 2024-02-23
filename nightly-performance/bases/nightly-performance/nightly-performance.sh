#!/bin/bash
set -euxo pipefail

apt-get update && apt-get install -y jq gettext-base

SIM_NAME=${JOB_NAMESPACE}-$(date +%Y-%m-%d-%H)
export SIM_NAME
NETWORK_NAME=${JOB_NAMESPACE}-$(date +%Y-%m-%d-%H)
export NETWORK_NAME
NETWORK_NAMESPACE=keramik-${NETWORK_NAME}
export NETWORK_NAMESPACE

curl -L https://github.com/mikefarah/yq/releases/download/v4.40.7/yq_linux_amd64 -o yq
chmod +x yq

# Customize the network name, use custom network.yaml if it exists
if [ -f /network/network.yaml ]; then
  ./yq -e '.metadata.name = env(NETWORK_NAME)' /network/network.yaml > network.yaml
else
  ./yq -e '.metadata.name = env(NETWORK_NAME)' /config/network.yaml > network.yaml
fi

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
CAS=$(./yq e '.spec.ceramic[0].cas' network-merged.yaml)
## Only wait for CAS if it is enabled
if [ -n "$CAS" ] && [ "$CAS" != "null" ]; then
  echo "Waiting for StatefulSet 'cas' to have at least 1 ready replica..."
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
fi

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

echo "Waiting for the network to stabilize and bootstrap"
sleep 300
kubectl wait --for=condition=complete job/bootstrap -n "${NETWORK_NAMESPACE}"

# Start the simulation and tag resources
if [ -f /simulation/simulation.yaml ]; then
  ./yq -e '.metadata.name = env(SIM_NAME), .metadata.namespace = env(NETWORK_NAMESPACE)' \
  /simulation/simulation.yaml > simulation.yaml
else
  ./yq -e '.metadata.name = env(SIM_NAME), .metadata.namespace = env(NETWORK_NAMESPACE)' \
    /config/simulation.yaml > simulation.yaml
fi

kubectl apply -f simulation.yaml
SIMULATION_RUNTIME=$(./yq e '.spec.runTime' simulation.yaml)
export SIMULATION_RUNTIME
sleep 60 # wait for the simulation to start
KERAMIK_SIMULATE_NAME=$(kubectl get job simulate-manager \
  -o jsonpath='{.spec.template.spec.containers[?(@.name=="manager")].env[?(@.name=="SIMULATE_NAME")].value}' -n "${NETWORK_NAMESPACE}")
export KERAMIK_SIMULATE_NAME
if [ -n "$KERAMIK_SIMULATE_NAME" ]; then
  kubectl label pods -l app=ceramic simulation="$KERAMIK_SIMULATE_NAME" -n "${NETWORK_NAMESPACE}"
  kubectl label pods -l app=otel simulation="$KERAMIK_SIMULATE_NAME" -n "${NETWORK_NAMESPACE}"
fi

echo "Simulation will run for $SIMULATION_RUNTIME minutes"
sleep $((SIMULATION_RUNTIME * 60))

# why is this not working?
# Maybe loop and watch for conditions
sleep 300 # wait for the simulation to finish
kubectl  get job simulate-manager -n "${NETWORK_NAMESPACE}" -o jsonpath='{.status}'

SUCCEEDED=$(kubectl  get job simulate-manager -n "${NETWORK_NAMESPACE}" -o jsonpath='{.status.succeeded}')
FAILED=$(kubectl  get job simulate-manager -n "${NETWORK_NAMESPACE}" -o jsonpath='{.status.failed}')
if [[ "$SUCCEEDED" -gt 0 ]]; then
  SIMULATION_STATUS_TAG="succeeded"
  SIMULATION_COLOR=5763719
  kubectl delete -f network-merged.yaml
elif [[ "$FAILED" -gt 0 ]]; then
  SIMULATION_STATUS_TAG="failed"
  SIMULATION_COLOR=15548997
else
  SIMULATION_STATUS_TAG="unknown"
  SIMULATION_COLOR=10070709
fi

export SIMULATION_STATUS_TAG
export SIMULATION_COLOR

# Send Discord notification
envsubst < /notifications/notification-template.json  > message.json
cat message.json
curl -v -H "Content-Type: application/json" -X POST -d @./message.json "$DISCORD_WEBHOOK_URL"

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
