FROM public.ecr.aws/r5b3e0r5/3box/rust-builder:latest as chef

RUN mkdir -p /home/builder/rust-ceramic
WORKDIR /home/builder/rust-ceramic

# TODO add to parent builder image
RUN cargo install cargo-chef

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /home/builder/rust-ceramic/recipe.json recipe.json

# Build dependencies - this is the caching Docker layer!
RUN cargo chef cook --release --recipe-path recipe.json

# Build application
COPY . .
RUN make release

FROM ubuntu:latest

COPY --from=builder /home/builder/rust-ceramic/target/release/ceramic-one /usr/bin

ENTRYPOINT ["/usr/bin/ceramic-one", "daemon"]
