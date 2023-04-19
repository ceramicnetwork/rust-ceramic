FROM public.ecr.aws/r5b3e0r5/3box/rust-builder:latest as builder

RUN mkdir -p /home/builder/rust-ceramic
WORKDIR /home/builder/rust-ceramic
ADD . .
RUN make release

FROM ubuntu:latest

COPY --from=builder /home/builder/rust-ceramic/target/release/ceramic-one /usr/bin

ENTRYPOINT ["/usr/bin/ceramic-one", "daemon"]
