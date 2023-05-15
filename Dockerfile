FROM public.ecr.aws/r5b3e0r5/3box/rust-builder:latest as builder

RUN mkdir -p /home/builder/rust-ceramic
WORKDIR /home/builder/rust-ceramic

# Use the same ids as the parent docker image by default
ARG UID=1001
ARG GID=1001

# Copy in source code
COPY . .

# Build application using a docker cache
# To clear the cache use:
#   docker builder prune --filter type=exec.cachemount
RUN --mount=type=cache,target=/home/builder/.cargo,uid=$UID,gid=$GID \
	--mount=type=cache,target=/home/builder/rust-ceramic/target,uid=$UID,gid=$GID \
    make release && \
    cp ./target/release/ceramic-one ./

FROM ubuntu:latest

COPY --from=builder /home/builder/rust-ceramic/ceramic-one /usr/bin

ENTRYPOINT ["/usr/bin/ceramic-one", "daemon"]
