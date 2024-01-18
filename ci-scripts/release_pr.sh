#!/bin/bash
# Script to prepare a release PR
#
# Preparing a release PR does the following:
# * Update Cargo.toml with new versions
# * Create a release branch, commit changes and push branch
# * Create PR for review
#
# Assumptions:
# * git is installed
# * git-cliff is installed
# * grep is installed
# * cargo-release is installed
# * gh is installed
# * jq is installed
# * GITHUB_TOKEN is set or gh is authenticated

# Fail script if any command fails
set -e

# Ensure we are in the git root
cd $(git rev-parse --show-toplevel)

# First determine the next release level
level=$1

# Release type
release_type=$2

# Print commits since last tag
cargo release changes

# Bump crate versions
cargo release version $level \
    --verbose \
    --execute \
    --no-confirm

# Version determined by cargo release (without the 'v' prefix)
version=$(cargo metadata --format-version=1 --no-deps | jq -r '.packages[0].version')

# Perform pre-release replacements
cargo release replace \
    --verbose \
    --execute \
    --no-confirm

# Run pre-release hooks
cargo release hook \
    --verbose \
    --execute \
    --no-confirm

# Generate release notes
release_notes=$(git cliff --unreleased --strip all --tag v$version)
# Update CHANGELOG
git cliff --tag v$version --output CHANGELOG.md

# Regenerate OpenAPI as we just updated the version metadata
./ci-scripts/gen_api_server.sh
./ci-scripts/gen_kubo_rpc_server.sh
# Update Cargo.lock with new versions
cargo update -p ceramic-kubo-rpc-server
cargo update -p ceramic-api-server

# Commit the specified packages
# `cargo release commit` currently fails to build a good commit message.
# Using git commit directly for now
# branch="release-v${version}"
git checkout feature/cd-rust-ceramic
msg="chore: pre-release version v${version}"
git commit -am "$msg"
commit_hash=$(git rev-parse HEAD)
git push --set-upstream origin cd-rust-ceramic

if["release_type" = "prerelease"]; then
        gh release create "v${version}" --target $commit_hash --title "$msg" --notes "$release_notes" --prerelease
