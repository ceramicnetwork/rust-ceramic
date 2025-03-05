#!/bin/bash

# Build and publish a docker image to run the tests suite
#
# DOCKER_PASSWORD must be set
# Use:
#
#   export DOCKER_PASSWORD=$(aws ecr-public get-login-password --region us-east-1)
#   echo "${DOCKER_PASSWORD}" | docker login --username AWS --password-stdin public.ecr.aws/r5b3e0r5
#
# to login to docker. That password will be valid for 12h.

# Change to the suite directory
cd $(dirname $0)/../suite

tag=${1-latest}
BUILD_PROFILE=${BUILD_PROFILE-release}
IMAGE_NAME=${IMAGE_NAME-public.ecr.aws/r5b3e0r5/3box/ceramic-tests-suite}

PUSH_ARGS="--push"
if [ "$NO_PUSH" = "true" ]
then
    PUSH_ARGS=""
fi

CACHE_ARGS=""
if [ -n "$ACTIONS_CACHE_URL" ]
then
    # Use Github Actions cache
    CACHE_ARGS="--cache-to type=gha --cache-from type=gha"
fi

docker buildx build \
    $PUSH_ARGS \
    $CACHE_ARGS \
    --build-arg BUILD_PROFILE=${BUILD_PROFILE} \
    -t ${IMAGE_NAME}:$tag \
    .
