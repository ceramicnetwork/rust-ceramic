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

SDK_DIR=$(dirname $0)/../sdk

# Bump version of all packages
for p in $(pwd)/sdk/packages/*
do
  cd $p
  version=$(npm version minor)
done

# Run lint fix to fix the issues the npm version command creates
cd $SDK_DIR
pnpm run lint:fix

# Ensure we are in the git root
cd $(git rev-parse --show-toplevel)

echo "Preparing PR for SDK version $version"

# Commit the specified packages
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
