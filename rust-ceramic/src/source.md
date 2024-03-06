# Source

## Pre-requisites

Building from source requires the following dependencies:
- rust
- make
- protobuf
- protoc
- libssl-dev
- pkg-config

### Packages
On Ubuntu, you can install most dependencies by running the following command:

```bash
sudo apt-get install -y build-essential pkg-config openssl libssl-dev unzip
```

On MacOS, you can install most dependencies by running the following command:

```bash
brew install openssl
```

### Protobuf
Installing `protobuf` and `protoc` can be done by following the instructions on the [protobuf website](https://developers.google.com/protocol-buffers).

Currently, `rust-ceramic` uses version 3.20.1.
Install this version on x86 Linux by running the following commands:

```bash
PROTOC_VERSION=3.20.1
PROTOC_ZIP=protoc-$PROTOC_VERSION-linux-x86_64.zip
curl --retry 3 --retry-max-time 90 -OL https://github.com/protocolbuffers/protobuf/releases/download/v$PROTOC_VERSION/$PROTOC_ZIP \
    && unzip -o $PROTOC_ZIP -d /usr/local bin/protoc \
    && unzip -o $PROTOC_ZIP -d /usr/local 'include/*' \
    && rm -f $PROTOC_ZIP
```

## Rust

To install from source, you will need to have Rust installed. You can install Rust by following the instructions on the [Rust website](https://www.rust-lang.org/tools/install).

## Building
Once the pre-requisites are satisfied, you can build `rust-ceramic` by running the following command:

```bash
make build
```

The generated binary will be located at `target/release/ceramic`.

Verify it's functioning by running:

```bash
./target/release/ceramic --help
```