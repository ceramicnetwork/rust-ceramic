#!/bin/bash
# Script to prepare a release PR
#
# Preparing a release PR does the following:
# * Deterimes the next version based on changes
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
# * CARGO_REGISTRY_TOKEN is set or cargo is authenticated

# Fail script if any command fails
set -e

# Ensure we are in the git root
cd $(git rev-parse --show-toplevel)

# First determine the next release level
level=$(./scripts/release_level.sh)

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

# Commit the specified packages
# `cargo release commit` currently fails to build a good commit message.
# Using git commit directly for now
branch="release-v${version}"
git checkout -b "$branch"
msg="chore: release version v${version}"
git commit -am "$msg"
git push --set-upstream origin $branch

# Create a PR
gh pr create \
    --base main \
    --head "$branch" \
    --label release \
    --title "$msg" \
    --body "$release_notes"
