#!/bin/bash

get_available_peers() {
  jq -r '. | length' < /peers/peers.json
}

# Wait for 5 minutes, or till the peers are available.
n=0
until [ "$n" -ge 30 ];
  do
    available_peers=$(get_available_peers)
    if [ "$available_peers" == "0" ]; then
      sleep 10
      n=$((n+1))
    else
      mkdir /config/env

      CERAMIC_URLS=$(jq -j '[.[].ceramic.ipfsRpcAddr | select(.)] | join(",")' < /peers/peers.json)
      COMPOSEDB_URLS=$(jq -j '[.[].ceramic.ceramicAddr | select(.)] | join(",")' < /peers/peers.json)
      CERAMIC_FLIGHT_URLS=$(jq -j '[.[].ceramic.flightAddr | select(.)] | join(",")' < /peers/peers.json)

      # Set up the env var with admin DID seeds to be a comma separated list with the same secret
      # repeated based on the number of nodes being started
      COMPOSEDB_ADMIN_DID_SEEDS=""
      for ((i=0; i<available_peers; i++)); do
          if ((i == available_peers - 1))
          then
              COMPOSEDB_ADMIN_DID_SEEDS+="$CERAMIC_ADMIN_DID_SECRET";
          else
              COMPOSEDB_ADMIN_DID_SEEDS+="$CERAMIC_ADMIN_DID_SECRET,";
          fi
      done

      echo "CERAMIC_URLS=$CERAMIC_URLS" > /config/.env
      echo "COMPOSEDB_URLS=$COMPOSEDB_URLS" >> /config/.env
      echo "CERAMIC_FLIGHT_URLS=$CERAMIC_FLIGHT_URLS" >> /config/.env
      echo "COMPOSEDB_ADMIN_DID_SEEDS=$COMPOSEDB_ADMIN_DID_SEEDS" >> /config/.env

      echo "Populated env"
      cat /config/.env

      exit 0
    fi
done

exit 1
