FROM public.ecr.aws/r5b3e0r5/3box/rust-builder:latest as builder

RUN mkdir -p /home/builder/ceramic-tests
WORKDIR /home/builder/ceramic-tests

# Use the same ids as the parent docker image by default
ARG UID=1001
ARG GID=1001

# Define the type of build to make. One of release or dev.
ARG BUILD_PROFILE=release

# Copy in source code
COPY . .

# Build application using a docker cache
# To clear the cache use:
#   docker builder prune --filter type=exec.cachemount
RUN --mount=type=cache,target=/home/builder/.cargo,uid=$UID,gid=$GID \
	--mount=type=cache,target=/home/builder/ceramic-tests/target,uid=$UID,gid=$GID \
    make $BUILD_PROFILE

FROM ubuntu:latest as tester

COPY --from=builder /home/builder/ceramic-tests/property/env/.env /usr/bin/.env
COPY --from=builder /home/builder/ceramic-tests/test-binaries /test-binaries
COPY --from=builder /home/builder/ceramic-tests/entrypoint.sh /usr/bin/entrypoint.sh

ENV RUST_BACKTRACE=1
ENV ENV_PATH="/usr/bin/.env"

ENTRYPOINT ["/usr/bin/entrypoint.sh"]
