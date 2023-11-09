#!/bin/bash

id=$(uuidgen)
job_id=$(uuidgen)
now=$(date +%s%N)
ttl=$(date +%s -d "14 days")
network="${1-dev}"
target="${2-latest}"
tag="${3-latest}"
manual="${4-false}"

docker run --rm -i \
  -e "AWS_REGION=$AWS_REGION" \
  -e "AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID" \
  -e "AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY" \
  -v ~/.aws:/root/.aws \
  -v "$PWD":/aws \
  amazon/aws-cli dynamodb put-item --table-name "ceramic-$network-ops" --item \
  "{                                              \
    \"id\":     {\"S\": \"$id\"},                 \
    \"job\":    {\"S\": \"$job_id\"},             \
    \"ts\":     {\"N\": \"$now\"},                \
    \"ttl\":    {\"N\": \"$ttl\"},                \
    \"stage\":  {\"S\": \"queued\"},              \
    \"type\":   {\"S\": \"deploy\"},              \
    \"params\": {                                 \
      \"M\": {                                    \
        \"component\": {\"S\": \"rust-ceramic\"}, \
        \"sha\":       {\"S\": \"$target\"},      \
        \"shaTag\":    {\"S\": \"$tag\"},         \
        \"manual\":    {\"BOOL\": $manual}        \
      }                                           \
    }                                             \
  }"
