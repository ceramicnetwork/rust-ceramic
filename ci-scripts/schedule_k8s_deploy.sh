#!/bin/bash

id=$(uuidgen)
job_id=$(uuidgen)
now=$(date +%s%N)
ttl=$(date +%s -d "14 days")
image=public.ecr.aws/r5b3e0r5/3box/ceramic-one:${2-latest}
branch=${TEST_BRANCH-main}
network=${1-dev}
environment=ceramic-v4-${network}

docker run --rm -i \
  -e "AWS_REGION=$AWS_REGION" \
  -e "AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID" \
  -e "AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY" \
  -v ~/.aws:/root/.aws \
  -v "$PWD":/aws \
  amazon/aws-cli dynamodb put-item --table-name "ceramic-$network-ops" --item \
  "{                                                                \
    \"id\":     {\"S\": \"$id\"},                                   \
    \"job\":    {\"S\": \"$job_id\"},                               \
    \"ts\":     {\"N\": \"$now\"},                                  \
    \"ttl\":    {\"N\": \"$ttl\"},                                  \
    \"stage\":  {\"S\": \"queued\"},                                \
    \"type\":   {\"S\": \"workflow\"},                              \
    \"params\": {                                                   \
      \"M\": {                                                      \
        \"name\":     {\"S\": \"Deploy k8s $network CERAMIC ONE\"}, \
        \"org\":      {\"S\": \"3box\"},                            \
        \"repo\":     {\"S\": \"ceramic-infra\"},                   \
        \"ref\":      {\"S\": \"$branch\"},                         \
        \"workflow\": {\"S\": \"update_image.yml\"},                \
        \"labels\":   {\"L\": [{\"S\": \"deploy\"}]},               \
        \"inputs\":   {                                             \
          \"M\": {                                                  \
            \"ceramic_one_image\": {\"S\": \"$image\"},             \
            \"environment\":       {\"S\": \"$environment\"}        \
          }                                                         \
        }                                                           \
      }                                                             \
    }                                                               \
  }"
