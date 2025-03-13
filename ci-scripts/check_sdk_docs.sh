#!/usr/bin/env bash
# Script to check a generated SDK docs have not changed

DIR=$(dirname $0)
cd "$DIR/../sdk"

# First generate
pnpm run docs

# Check if anything changed, the README contains a generation date, ignore that file.
changes=$(git status --porcelain .)
if [ $? -ne 0 ]
then
    echo "Failed to get git status"
    exit 1
fi

if [[ -n "$changes" ]]
then
    echo "Found changes:"
    echo "$changes"
    exit 1
fi
echo "Generated SDK docs are up-to-date"
exit 0
