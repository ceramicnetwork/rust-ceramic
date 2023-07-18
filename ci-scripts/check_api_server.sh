#!/bin/bash
# Script to generate api-server crate from OpenAPI definition.

DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd $DIR/..

# First generate
$DIR/gen_api_server.sh > /dev/null

# Check if anything changed, the README contains a generation date, ignore that file.
changes=$(git status --porcelain api-server | grep -v README.md)
if [[ -n "$changes" ]]
then
    echo "Found api-server changes:"
    echo "$changes"
    exit 1
fi
echo "Generated api-server is up-to-date"
exit 0
