#!/usr/bin/env bash

# Script to generate api-server crate from OpenAPI definition.
# Requires augeas/augtool to be installed

set -e

DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd $DIR/..


# Generate crate
./ci-scripts/openapi-generator-cli.sh \
    generate \
    -i api/ceramic.yaml \
    -g rust-server  \
    --additional-properties=packageName=ceramic-api-server \
    -o api-server

# Add missing clippy allow directive to example code
# This can be removed once the openapi-generator-cli generates code that passes clippy.
echo "#![allow(suspicious_double_ref_op)]" | cat - ./api-server/examples/server/server.rs > ./api-server/examples/server/server.rs.tmp
echo "#![allow(clippy::useless_vec)]" | cat - - ./api-server/src/models.rs > ./api-server/src/models.rs.tmp
mv ./api-server/examples/server/server.rs.tmp ./api-server/examples/server/server.rs
mv ./api-server/src/models.rs.tmp ./api-server/src/models.rs

# Remove conversion feature from generated code because it doesn't build and we do not use it.
#augtool -s -L \
#    -r ./api-server/ \
#    -f ./ci-scripts/remove_conversion.augt

# Format the generated code
cargo fmt -p ceramic-api-server

