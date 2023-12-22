#!/usr/bin/env bash
# Script to generate kubo-server crate from OpenAPI definition.

set -e

DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd $DIR/..


# Generate crate
npx @openapitools/openapi-generator-cli \
    generate \
    -i kubo-rpc/kubo-rpc.yaml \
    -g rust-server  \
    --additional-properties=packageName=ceramic-kubo-rpc-server \
    -o kubo-rpc-server

# Add missing clippy allow directive to example code
# This can be removed once the openapi-generator-cli generates code that passes clippy.
echo "#![allow(suspicious_double_ref_op)]" | cat - ./kubo-rpc-server/examples/server/server.rs > ./kubo-rpc-server/examples/server/server.rs.tmp
mv ./kubo-rpc-server/examples/server/server.rs.tmp ./kubo-rpc-server/examples/server/server.rs

# Remove conversion feature from generated code because it doesn't build and we do not use it.
sed -i 's/conversion = .*//' ./kubo-rpc-server/Cargo.toml

# Format the generated code
cargo fmt -p ceramic-kubo-rpc-server

