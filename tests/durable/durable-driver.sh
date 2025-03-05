#!/bin/bash

tag=${BUILD_TAG-latest}
IMAGE_NAME=${IMAGE_NAME-public.ecr.aws/r5b3e0r5/3box/ceramic-tests-suite}

# This script won't launch any daemons. It will assume the test suite is being pointed to some instance(s) of ComposeDB
# and Ceramic running in durable infrastructure.
#
# The script also assumes that AWS credentials (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and AWS_REGION) have been
# configured.
DURABLE_ENV=${1-dev}
TEST_SELECTOR=${2-.}

case "${DURABLE_ENV}" in
  tnet) ;;
  prod) ;;
  *)
    echo "Usage: $0 {dev|qa|tnet|prod}"
    exit 1
    ;;
esac

AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID-.}
AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY-.}
AWS_REGION=${AWS_REGION-us-east-1}

# TODO: We are using the host network to make port forwarding work. This can be removed once we have public URLs for
# Keramik endpoints.
docker run --network="host" \
    -e AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID}" \
    -e AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY}" \
    -e AWS_REGION="${AWS_REGION}" \
    -e DB_ENDPOINT="${DB_ENDPOINT}" \
    -v $(pwd)/suite/env/.env."${DURABLE_ENV}":/config/.env \
    -e DOTENV_CONFIG_PATH="/config/.env" \
    -e TEST_SELECTOR="${TEST_SELECTOR}" \
    -e COMPOSEDB_ADMIN_DID_SEEDS="${COMPOSEDB_ADMIN_DID_SEEDS}" \
    "${IMAGE_NAME}":"$tag"
