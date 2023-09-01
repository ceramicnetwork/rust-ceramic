#!/usr/bin/env bash
# Script to generate kubo-rpc-server crate from OpenAPI definition.

DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd $DIR/..

# First generate
$DIR/gen_kubo_rpc_server.sh > /dev/null

# Check if anything changed, the README contains a generation date, ignore that file.
changes=$(git status --porcelain kubo-rpc-server | grep -v README.md)
if [[ -n "$changes" ]]
then
    echo "Found kubo-rpc-server changes:"
    echo "$changes"
    exit 1
fi
echo "Generated kubo-rpc-server is up-to-date"
exit 0
