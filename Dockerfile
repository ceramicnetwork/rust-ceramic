FROM public.ecr.aws/r5b3e0r5/3box/rust-builder:latest as builder

RUN mkdir -p /home/builder/rust-ceramic
WORKDIR /home/builder/rust-ceramic

# Define the type of build to make. One of release or debug.
ARG BUILD_MODE=release

# Copy in source code
COPY . .

# Build application using a docker cache
# To clear the cache use:
#   docker builder prune --filter type=exec.cachemount
RUN --mount=type=cache,target=/home/builder/.cargo \
	--mount=type=cache,target=/home/builder/rust-ceramic/target \
    make $BUILD_MODE && \
    cp ./target/release/ceramic-one ./

FROM debian:bookworm-slim

COPY --from=builder /home/builder/rust-ceramic/ceramic-one /usr/bin

# Adding this step after copying the ceramic-one binary so that we always take the newest libs from the builder if the
# main binary has changed. Updated dependencies will result in an updated binary, which in turn will result in the
# latest versions of the dependencies being pulled from the builder.
COPY --from=builder /usr/lib/*-linux-gnu*/libsqlite3.so* /usr/lib/
COPY --from=builder /usr/lib/*-linux-gnu*/libssl.so* /usr/lib/
COPY --from=builder /usr/lib/*-linux-gnu*/libcrypto.so* /usr/lib/

ENTRYPOINT ["/usr/bin/ceramic-one", "daemon"]
