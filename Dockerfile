FROM public.ecr.aws/r5b3e0r5/3box/rust-builder:latest as builder

RUN mkdir -p /home/builder/rust-ceramic
WORKDIR /home/builder/rust-ceramic
ADD . .
RUN make release
CMD ["/home/builder/rust-ceramic/target/release/ceramic-one"]

FROM ubuntu:latest

COPY --from=builder /home/builder/rust-ceramic/target/release/ceramic-one /usr/bin

CMD ["/usr/bin/ceramic-one"]
