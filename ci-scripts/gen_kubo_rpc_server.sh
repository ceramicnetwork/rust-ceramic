#!/usr/bin/env bash

# Script to generate kubo-server crate from OpenAPI definition.
# Requires augeas/augtool to be installed

set -e

DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd $DIR/..


# Generate crate
./ci-scripts/openapi-generator-cli.sh \
    generate \
    -i kubo-rpc/kubo-rpc.yaml \
    -g rust-server  \
    --additional-properties=packageName=ceramic-kubo-rpc-server \
    -o kubo-rpc-server

# Add missing clippy allow directive to example code
# This can be removed once the openapi-generator-cli generates code that passes clippy.
echo "#![allow(suspicious_double_ref_op)]" | cat - ./kubo-rpc-server/examples/server/server.rs > ./kubo-rpc-server/examples/server/server.rs.tmp
echo "#![allow(clippy::to_string_trait_impl)]" | cat - - ./kubo-rpc-server/src/models.rs > ./kubo-rpc-server/src/models.rs.tmp
mv ./kubo-rpc-server/examples/server/server.rs.tmp ./kubo-rpc-server/examples/server/server.rs
mv ./kubo-rpc-server/src/models.rs.tmp ./kubo-rpc-server/src/models.rs

# Remove conversion feature from generated code because it doesn't build and we do not use it.
augtool -s -L \
    -r ./kubo-rpc-server/ \
    -f ./ci-scripts/remove_conversion.augt

# Remove the serde_ignored dependency as it is unused
augtool -s -L \
    -r ./kubo-rpc-server/ \
    -f ./ci-scripts/remove_serde_ignored.augt


# Format the generated code
cargo fmt -p ceramic-kubo-rpc-server

