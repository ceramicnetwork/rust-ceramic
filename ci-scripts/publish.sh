#!/bin/bash

# Build and publish a docker image run running ceramic-one
#
# DOCKER_PASSWORD must be set
# Use:
#
#   export DOCKER_PASSWORD=$(aws ecr-public get-login-password --region us-east-1)
#   echo "${DOCKER_PASSWORD}" | docker login --username AWS --password-stdin public.ecr.aws/r5b3e0r5
#
# to login to docker. That password will be valid for 12h.

docker buildx build -t 3box/ceramic-one .
docker tag 3box/ceramic-one:latest public.ecr.aws/r5b3e0r5/3box/ceramic-one:latest
docker push public.ecr.aws/r5b3e0r5/3box/ceramic-one:latest
