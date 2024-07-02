# Rust Ceramic

Implementation of the Ceramic protocol in Rust.

Current status is that the `ceramic-one` binary only mimics the Kubo RPC API and relies on https://github.com/ceramicnetwork/js-ceramic for the remaining logic.

## Installation

The following section covers several ways one can install Rust-Ceramic contingent on the recieving environment:

### MacOS

Install from [Homebrew](https://brew.sh/):

```bash
brew install ceramicnetwork/tap/ceramic-one
```

### Linux - Debian-based distributions

Install a the latest release using dpkg:

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

### Migration
This repo also contains the kubo to ceramic-one migration script.

This script will read ipfs repo files matching ~/.ipfs/blocks/**/*
and insert them into the ceramic-one database ~/.ceramic-one/db.sqlite3

The migration script will scan the input-ipfs-path for any file that has
a b32 multibase as the filename not including the extension. When a file matches its hash it will be copied into the database at output-ceramic-path.

you can run it with cargo

    $ cargo run --bin migration -- -h
    Usage: migration [OPTIONS]

    Options:
    -i, --input-ipfs-path <INPUT_IPFS_PATH>
            The path to the ipfs_repo [eg: ~/.ipfs/blocks]
    -c, --input-ceramic-db <INPUT_CERAMIC_DB>
            The path to the input_ceramic_db [eg: ~/.ceramic-one/db.sqlite3]
    -o, --output-ceramic-path <OUTPUT_CERAMIC_PATH>
            The path to the output_ceramic_db [eg: ~/.ceramic-one/db.sqlite3]
    -v, --verbose...
            More output per occurrence
    -q, --quiet...
            Less output per occurrence
    -h, --help
            Print help
    -V, --version
            Print version

or build it, move migration to you ceramic box, run it there.

    $ cargo build --frozen --release --bin migration
    $ cp target/release/migration ./migration
    $ ./migration -h
    Usage: migration [OPTIONS]

    Options:
    -i, --input-ipfs-path <INPUT_IPFS_PATH>
            The path to the ipfs_repo [eg: ~/.ipfs/blocks]
    -c, --input-ceramic-db <INPUT_CERAMIC_DB>
            The path to the input_ceramic_db [eg: ~/.ceramic-one/db.sqlite3]
    -o, --output-ceramic-path <OUTPUT_CERAMIC_PATH>
            The path to the output_ceramic_db [eg: ~/.ceramic-one/db.sqlite3]
    -v, --verbose...
            More output per occurrence
    -q, --quiet...
            Less output per occurrence
    -h, --help
            Print help
    -V, --version
            Print version

Migration

* Ingest all the SHA256 blocks from your local filesystem block store to a sqlite3 database
    ```zsh
    ./migration --input-ipfs-path '~/.ipfs/blocks' --output-ceramic-path '~/.ceramic-one/db.sqlite3'
    ```
  * Pass in the input path to the blocks folder the script will import all blocks 
    in the directory that is named with its multihash. 
  * Pass in the where you would like the sqlite database as the output path.
* Move the sqlite3 database to the new rust-ceramic node.
* Start the new rust-ceramic server and point the compose DB node to it.
    ```zsh
    ./ceramic-one daemon --bind-address '127.0.0.1:5001'
    ```
* Once the traffic is cut over to the new node and we know there are not new block being crated on the old node.
Re-run the migration to pick up any new block that were crated after the first migration
    ```zsh
    ./migration --input-ipfs-path '~/.ipfs/blocks' --output-ceramic-path '~/.ceramic-one/db.sqlite3.bck'
    ```
* Move the second sqlite3 database to the new rust-ceramic node.
* Ingest the new block from this second sqlite3 database.
    ```zsh
    ./migration  --input-ceramic-db 'db.sqlite3.bck' --output-ceramic-path '~/.ceramic-one/db.sqlite3.bck'
    ```

## License

Fully open source and dual-licensed under MIT and Apache 2.

