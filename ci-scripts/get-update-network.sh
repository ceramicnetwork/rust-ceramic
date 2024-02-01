#!/bin/bash
set -ex
export RUST_CERAMIC_IMAGE=${RUST_CERAMIC_IMAGE_REPO}:${RUST_CERAMIC_IMAGE_TAG}
export KERAMIK_NETWORK_NAME=hermetic-rust-ceramic-${RUST_CERAMIC_IMAGE_TAG}
mkdir ./bin
curl -L https://github.com/mikefarah/yq/releases/download/v4.40.5/yq_linux_amd64 -o ./bin/yq
chmod +x ./bin/yq
curl -L https://github.com/3box/ceramic-tests/releases/download/v0.2.0/hermetic-driver-x86_64-linux -o ./bin/hermetic-driver
chmod +x ./bin/hermetic-driver
curl -LO https://raw.githubusercontent.com/3box/ceramic-tests/main/networks/basic-rust.yaml
./bin/yq -i '.spec.ceramic[0].ipfs.rust.image = strenv(RUST_CERAMIC_IMAGE)' basic-rust.yaml
./bin/yq -i '.metadata.name = strenv(KERAMIK_NETWORK_NAME)' basic-rust.yaml
./bin/yq --no-colors basic-rust.yaml