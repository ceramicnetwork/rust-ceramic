#!/usr/bin/env bash
# Script to check a generated server has not changed

server_dir=$1
server_gen_cmd=$2

DIR=$(dirname $0)
cd "$DIR/.."

# First generate
$server_gen_cmd > /dev/null

# Check if anything changed, the README contains a generation date, ignore that file.
status=$(git status --porcelain $server_dir)
if [ $? -ne 0 ]
then
    echo "Failed to get git status"
    exit 1
fi

changes=$(echo "$status" | grep -v README.md)
if [[ -n "$changes" ]]
then
    echo "Found ${server_dir} changes:"
    echo "$changes"
    exit 1
fi
echo "Generated ${server_dir} is up-to-date"
exit 0
