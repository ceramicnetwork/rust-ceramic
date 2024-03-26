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

docker buildx build --load -t 3box/ceramic-one .

if [[ -n "$SHA" ]]; then
  docker tag 3box/ceramic-one:latest public.ecr.aws/r5b3e0r5/3box/ceramic-one:"$SHA"
fi
if [[ -n "$SHA_TAG" ]]; then
  docker tag 3box/ceramic-one:latest public.ecr.aws/r5b3e0r5/3box/ceramic-one:"$SHA_TAG"
fi
if [[ -n "$RELEASE_TAG" ]]; then
  docker tag 3box/ceramic-one:latest public.ecr.aws/r5b3e0r5/3box/ceramic-one:"$RELEASE_TAG"
  docker tag 3box/ceramic-one:latest public.ecr.aws/r5b3e0r5/3box/ceramic-one:latest
fi

docker push -a public.ecr.aws/r5b3e0r5/3box/ceramic-one
