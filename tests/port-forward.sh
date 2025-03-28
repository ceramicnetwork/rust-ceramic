#!/bin/bash
# Setup port-forward to each pod in the Keramik network
# prints env vars for use in the tests using the port-forwarded values.

# Enable job control within the script
set -m

pids=$(jobs -p)
if [ -n "$pids" ]
then
    kill "$pids"
    wait "$pids"
fi

if [ -n "$1" ]
then
  namespace_flag="-n $1"
fi

composedb=7007
ceramic=5101
flight=5102
offset=10
step=10

admin_private_key=$(kubectl $namespace_flag get secret ceramic-admin -o jsonpath="{.data['private-key']}" | base64 -d )

COMPOSEDB_URLS=''
CERAMIC_URLS=''
CERAMIC_FLIGHT_URLS=''
COMPOSEDB_ADMIN_DID_SEEDS=''

for pod in $(kubectl $namespace_flag get pods -l app=ceramic -o json | jq -r '.items[].metadata.name')
do
    composedb_local=$((composedb + offset))
    ceramic_local=$((ceramic + offset))
    ceramic_flight=$((flight + offset))

    if [ -n "$CERAMIC_URLS" ]
    then
        COMPOSEDB_URLS="$COMPOSEDB_URLS,"
        CERAMIC_URLS="$CERAMIC_URLS,"
        CERAMIC_FLIGHT_URLS="$CERAMIC_FLIGHT_URLS,"
        COMPOSEDB_ADMIN_DID_SEEDS="$COMPOSEDB_ADMIN_DID_SEEDS,"
    fi


    COMPOSEDB_URLS="${COMPOSEDB_URLS}http://localhost:$composedb_local"
    CERAMIC_URLS="${CERAMIC_URLS}http://localhost:$ceramic_local"
    CERAMIC_FLIGHT_URLS="${CERAMIC_FLIGHT_URLS}http://localhost:$ceramic_flight"
    COMPOSEDB_ADMIN_DID_SEEDS="${COMPOSEDB_ADMIN_DID_SEEDS}${admin_private_key}"

    kubectl port-forward $namespace_flag "$pod" $composedb_local:$composedb $ceramic_local:$ceramic $ceramic_flight:$flight >/dev/null  &

    offset=$((offset + step))
done


export COMPOSEDB_URLS
export CERAMIC_URLS
export CERAMIC_FLIGHT_URLS
export COMPOSEDB_ADMIN_DID_SEEDS
