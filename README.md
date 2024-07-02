# Rust Ceramic

Implementation of the Ceramic protocol in Rust.

Current status is that the `ceramic-one` binary only mimics the Kubo RPC API and relies on https://github.com/ceramicnetwork/js-ceramic for the remaining logic.

## Installation

The following section covers several ways one can install Rust-Ceramic contingent on the recieving environment:

### Linux - from Binary Distribution

Install a Github release (Debian-based distributions):

```bash
# get deb.tar.gz
curl -LO https://github.com/ceramicnetwork/rust-ceramic/releases/download/latest/ceramic-one_x86_64-unknown-linux-gnu.tar.gz
# untar the Debian software package file
tar zxvf ceramic-one_x86_64-unknown-linux-gnu.tar.gz
# install with dpkg - package manager for Debian
dpkg -i ceramic-one.deb
```

### Linux - from Source

```bash
# Install rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
# Install build tools
apt-get install -y build-essential pkg-config openssl libssl-dev unzip
# Update environment
source "$HOME/.cargo/env"
# Install protobuf
PROTOC_VERSION=3.20.1
PROTOC_ZIP=protoc-$PROTOC_VERSION-linux-x86_64.zip
curl --retry 3 --retry-max-time 90 -OL https://github.com/protocolbuffers/protobuf/releases/download/v$PROTOC_VERSION/$PROTOC_ZIP \
    && unzip -o $PROTOC_ZIP -d /usr/local bin/protoc \
    && unzip -o $PROTOC_ZIP -d /usr/local 'include/*' \
    && rm -f $PROTOC_ZIP
# Checkout rust-ceramic repo
git clone https://github.com/ceramicnetwork/rust-ceramic
# Enter repo and build
cd rust-ceramic
make build
cp ./target/release/ceramic-one /usr/local/bin/ceramic-one
```

### MacOS - Local System

Install from [Homebrew](https://brew.sh/)

```bash
brew install ceramicnetwork/tap/ceramic-one
```

### Docker

Start rust-ceramic using the host network:

```bash
docker run --network=host \
  public.ecr.aws/r5b3e0r5/3box/ceramic-one:latest
```

### Docker-Compose

1. Create a testing directory, and enter it:

```bash
mkdir ceramic-recon
cd ceramic-recon
```

2. Save the following docker-compose.yaml there:

```YAML
version: '3.8'

services:
  ceramic-one:
    image: public.ecr.aws/r5b3e0r5/3box/ceramic-one:latest
    network_mode: "host"
    volumes:
      - ceramic-one-data:/root/.ceramic-one
volumes:
  ceramic-one-data:
    driver: local
```

3. Run `docker-compose up -d`

## Usage

Running the ceramic-one binary is simply passing the `daemon` cli option.

```sh
# if necessary, include the path/to/the/binary e.g. /usr/local/bin/ceramic-one or ./target/release/ceramic-one
$ ceramic-one daemon

# There are many flags for the daemon CLI that can be passed directly or set as environment variables.
# See `DaemonOpts` in one/src/lib.rs for the complete list or pass the -h flag
$ ceramic-one daemon -h

# A few common options are overriding the log level:
$ RUST_LOG=warn,ceramic_one=info,ceramic_service=debug ceramic-one daemon
# Or modifying the network
$ ceramic-one daemon --network testnet-clay
$ ceramic-one daemon --network local --local-network-id 0
# Or changing where directory where all data is stored. This folder SHOULD be backed up in production
# and if you change the defaults, you MUST specify it every time you start the daemon.
$ ceramic-one daemon --store-dir ./custom-store-dir
```

The process honors RUST_LOG env variable for controlling its logging output.
For example, to enable debug logging for code from this repo but error logging for all other code use:

## License

Fully open source and dual-licensed under MIT and Apache 2.

