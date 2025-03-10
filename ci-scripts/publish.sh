#!/usr/bin/env bash

# Build and publish a docker image run running ceramic-one
#
# DOCKER_PASSWORD must be set
# Use:
#
#   export DOCKER_PASSWORD=$(aws ecr-public get-login-password --region us-east-1)
#   echo "${DOCKER_PASSWORD}" | docker login --username AWS --password-stdin public.ecr.aws/r5b3e0r5
#
# to login to docker. That password will be valid for 12h.

BUILD_MODE=${BUILD_MODE-release}
IMAGE_NAME=${IMAGE_NAME-public.ecr.aws/r5b3e0r5/3box/ceramic-one}

docker buildx build --load --build-arg="BUILD_MODE=$BUILD_MODE" -t 3box/ceramic-one .

if [[ -n "$SHA" ]]; then
  docker tag 3box/ceramic-one:latest ${IMAGE_NAME}:"$SHA"
fi
if [[ -n "$SHA_TAG" ]]; then
  docker tag 3box/ceramic-one:latest ${IMAGE_NAME}:"$SHA_TAG"
fi
if [[ -n "$RELEASE_TAG" ]]; then
  docker tag 3box/ceramic-one:latest ${IMAGE_NAME}:"$RELEASE_TAG"
fi
if [[ "$TAG_LATEST" == "true" ]]; then
  docker tag 3box/ceramic-one:latest ${IMAGE_NAME}:latest
fi
if [[ -n "$CUSTOM_TAG" ]]; then
  docker tag 3box/ceramic-one:latest ${IMAGE_NAME}:"$CUSTOM_TAG"
fi

if [ "$NO_PUSH" != "true" ]
then
    docker push -a public.ecr.aws/r5b3e0r5/3box/ceramic-one
fi

