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
current_branch=$(git rev-parse --abbrev-ref HEAD)
pr_branch="version-v${version}"
git checkout -b "$pr_branch"
msg="chore: version v${version}"
git commit -am "$msg"
git push --set-upstream origin "$pr_branch"

# Create a PR against the branch this workflow is running on
gh pr create \
    --base "$current_branch" \
    --head "$pr_branch" \
    --label release \
    --title "$msg" \
    --body "$release_notes"
