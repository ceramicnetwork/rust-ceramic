#!/bin/bash
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

# Format the generated code
cargo fmt -p ceramic-kubo-rpc-server

# HACK: We patch the generated code to fix two issues:
#
#  1. Allow for a streaming response to pubsub subscribe endpoint
#  2. The `type` field name breaks the `conversion` feature of the generated code. It is renamed to `typ`.
#
# Steps to generate this patch file:
#     1. Get the code into the desired state and commit changes.
#     2. Comment out the following `patch` line and run the script.
#     3. Run `git diff -R kubo-rpc-server > ./kubo-rpc-server.patch`.
#     4. Manually edit the patch file to remove the patch of the date in the README file.
#     5. Run `git checkout kubo-rpc-server`.
#     6. Revert and run this script to validate it works. It should run without errors or warnings.
patch -p1 <./kubo-rpc-server.patch
