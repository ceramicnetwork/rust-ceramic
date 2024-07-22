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

### API
[OpenAPI Generated Docs](./api-server/docs/default_api.md)

[Ceramic specification](https://developers.ceramic.network/docs/protocol/js-ceramic/overview)

#### POST `/ceramic/events`
Creates a new event
```sh
curl -X POST --data-binary @path/to/event.car "http://127.0.0.1:5101/ceramic/events"
```

The event payload is a multibase encoded Ceramic [event](https://developers.ceramic.network/docs/protocol/js-ceramic/streams/event-log#jws--dag-jose).
The Ceramic event is a car file with the envelope as the root.
The envelope is a dag-jose with a CID as the payload.
The event data is the dag-cbor object at payload CID.
For details on signing a ceramic event see [link](./todo.md) #TODO

#### GET `/ceramic/events/{event_id}`
```
curl http://127.0.0.1:5101/ceramic/events/{event_id}
curl "http://127.0.0.1:5101/ceramic/events/bagcqcerahknurn4svfcquedzefgtljgf5qvavja5wttwqq3wnnrsavhzwx7a"

{"id":"bagcqc...","data":"mO6Jlcm..."}
```
Gets the event data by id.
Event_id is the CID of the envelope.

The endpoint will return the id event_id.
The include data arg is not yet implemented.
When it is data will be a car file with the event blocks and the envelope as the root is include data is set.
For now it will always return `"data": ""`

```rs
pub struct Event {
    /// Multibase encoding of event root CID.
    #[serde(rename = "id")]
    pub id: String,

    /// Multibase encoding of event data CAR file.
    #[serde(rename = "data")]
    pub data: String,
}
```

#### GET `/feed/events?resumeAt=0&limit=10000`
```sh
> curl "http://127.0.0.1:5101/ceramic/feed/events?resumeAt=0&limit=10000"
```
```json
{"events":[{  "id": event_cid,
            "data": ""}],
 "resumeToken":"1"}
```
Get all new event keys since resume token
resumeAt: token that designates the point to resume from, that is find keys added after this point
limit: the maximum number of events to return, default is 10000.

The feed API delivers event is the order that the node learned them.
A node will deliver events in a consistent order but the order will differ between nodes.

```rs
pub struct EventFeed {
    /// An array of events.
    #[serde(rename = "events")]
    pub events: Vec<models::Event>,

    /// The token/high water mark to used as resumeAt on a future request
    #[serde(rename = "resumeToken")]
    pub resume_token: String,
}

pub struct Event {
    /// Multibase encoding of event root CID.
    #[serde(rename = "id")]
    pub id: String,

    /// Multibase encoding of event data CAR file.
    #[serde(rename = "data")]
    pub data: String,
}
```

#### POST `ceramic/interests/{sort_key}/{sort_value}?controller={}&streamId={}`
```
http://127.0.0.1:5101/ceramic/interests/model/{sort_value}?controller={}&streamId={}

> curl -X POST "http://127.0.0.1:5101/ceramic/interests/model/kh4q0ozorrgaq2mezktnrmdwleo1d"
```
 Instruct your node to begin synchronizing a model by its modelID.

model_ID of the model to subscribe to.
controller DID to subscribe to is missing all controllers.
streamId of the specific stream to subscribe to if missing all streams.

## License

Fully open source and dual-licensed under MIT and Apache 2.

