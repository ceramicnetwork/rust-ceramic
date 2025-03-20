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
# * gh is installed
# * GITHUB_TOKEN is set or gh is authenticated

# Fail script if any command fails
set -e

# Bump version of all packages
for p in $(pwd)/sdk/packages/*
do
  cd $p
  version=$(npm version minor)
done

# Ensure we are in the git root
cd $(git rev-parse --show-toplevel)

# Commit the specified packages
# `cargo release commit` currently fails to build a good commit message.
# Using git commit directly for now
current_branch=$(git rev-parse --abbrev-ref HEAD)
pr_branch="sdk-version-${version}"
git checkout -b "$pr_branch"
msg="chore(sdk): version ${version}"
git commit -am "$msg"
git push --set-upstream origin "$pr_branch"

# Create a PR against the branch this workflow is running on
gh pr create \
    --base "$current_branch" \
    --head "$pr_branch" \
    --label release \
    --title "$msg" \
    --body "Release ${version}"
