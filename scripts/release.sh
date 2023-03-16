#!/bin/bash
# Script to perform a release once a release PR is merged
#
# Performing a release does the following:
# * Tags the git repo with the new release version
# * Publishes new release to crates.io
# * Publishes new release to Github releases
#
# Assumptions:
# * git is installed
# * git-cliff is installed
# * grep is installed
# * cargo-release is installed
# * gh is installed
# * jq is installed
# * GITHUB_TOKEN is set or gh is authenticated
# * CARGO_REGISTRY_TOKEN is set or cargo is authenticated
# * Cargo.toml has already been updated with the new version

# Ensure we are in the git root
cd $(git rev-parse --show-toplevel)

# Check if we need to publish anything
cargo release --unpublished
ret=$?
case $ret in
    0) echo "Publish needed";;
    2) echo "Nothing to publish"; exit 0;;
    *) exit 1;;
esac

# Now fail if any of the remaining commands fail
set -e

# Publish the specified packages
cargo release publish \
    --verbose \
    --execute \
    --allow-branch main \
    --no-confirm

# Tag the released commits
cargo release tag \
    --verbose \
    --execute \
    --allow-branch main \
    --no-confirm

# Push tags to remote
cargo release push \
    --verbose \
    --execute \
    --allow-branch main \
    --no-confirm

# Build release binaries
cargo build --release

# Version determined by cargo release (without the 'v' prefix)
version=$(cargo metadata --format-version=1 --no-deps | jq -r '.packages[0].version')
# Generate release notes
release_notes=$(git cliff --latest --strip all)
# Publish Github release
gh release create "v$version" \
    --title "v$version" \
    --notes "$release_notes"
    ./target/release/ceramic-one

