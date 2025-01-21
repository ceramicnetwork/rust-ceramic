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
echo "#![allow(clippy::to_string_trait_impl)] #![allow(unexpected_cfgs)]" | cat - ./api-server/src/models.rs > ./api-server/src/models.rs.tmp
echo "#![allow(clippy::blocks_in_conditions)]" | cat - ./api-server/src/server/mod.rs > ./api-server/src/server/mod.rs.tmp
echo "#![allow(clippy::duplicated_attributes)]" | cat - ./api-server/src/lib.rs > ./api-server/src/lib.rs.tmp
mv ./api-server/examples/server/server.rs.tmp ./api-server/examples/server/server.rs
mv ./api-server/src/models.rs.tmp ./api-server/src/models.rs
mv ./api-server/src/server/mod.rs.tmp ./api-server/src/server/mod.rs
mv ./api-server/src/lib.rs.tmp ./api-server/src/lib.rs

# Remove conversion feature from generated code because it doesn't build and we do not use it.
augtool -s -L \
    -r ./api-server/ \
    -f ./ci-scripts/remove_conversion.augt
# Add percent-encoding as client dependency
augtool -s -L \
    -r ./api-server/ \
    -f ./ci-scripts/add_percent_encoding.augt

# Format the generated code
cargo fmt -p ceramic-api-server

