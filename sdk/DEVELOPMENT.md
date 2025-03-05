# Development

> Getting started with development on ceramic-sdk

## Project setup

First clone the repo:

```
git clone https://github.com/ceramicnetwork/ceramic-sdk.git
cd ceramic-sdk
```

This monorepo uses pnpm, make sure to install it first if you don't already have it.

```
npm install -g pnpm
```

To install dependencies for all packages in this repo:

```
pnpm install
```

Then build all packages:

```
pnpm run build
```

## Run tests

You can run all tests at the top level,

```
pnpm test
```

If you only want to test a specific package just `cd` into the specific package folder and run the same command as above.

## Documenting changes

ceramic-sdk uses [changesets](https://github.com/changesets/changesets) to keep track of and release changes.

To generate a new changeset, run `pnpm changeset` in the root of the repository and follow the instructions.
The generated markdown files in the `.changeset` directory should be committed to the repository.

## Creating a release

This repo uses pnpm to make releases, each package is released and versioned individually. [Semantic versioning](https://semver.org/) is followed to version releases. We do not currently regularly make releases on schedule or make release candidates (alpha/beta). Releases are made when changes are available to release. Released packages are published to NPM.

Before creating any releases, make sure you have an npm account (you can sign up at https://www.npmjs.com/), have signed into that account on the command line with `npm adduser`, and that the account has been added to the ceramic-sdk org on npm.

### Release

Releases are currently done manually and not by any CI. CI will only publish documentation on release. Process may be further formalized in the future. To make a release:

1. First create a release branch from the lastest main branch.

```
git checkout -b release/any-name
```

2. Run `pnpm changeset version`. This will bump the versions of the packages previously specified with pnpm changeset (and any dependents of those) and update the changelog files.

3. Manually update the flight-sql-client/npm/*/package.json versions if desired. This isn't yet included with the changeset. You must download the artifacts from CI, put them in `flight-sql-client/artifacts` and `run pnpm artifacts` to move them to the platform packages similar to how the CI workfow step `Publish flight-sql-client` does this, but it isn't yet released.

4. Run `pnpm lint:fix && pnpm install`. This will update the lockfile and rebuild packages.

5. Create release commit, include each package and version to be released, for example:

```
git commit -m @ceramic-sdk/events@0.2.0, @ceramic-sdk/flight-sql-client@0.2.0, @ceramic-sdk/http-client@0.2.0, @ceramic-sdk/identifiers@0.2.0, @ceramic-sdk/model-client@0.2.0, @ceramic-sdk/model-instance-client@0.2.0, @ceramic-sdk/model-instance-protocol@0.2.0, @ceramic-sdk/model-protocol@0.2.0, @ceramic-sdk/stream-client@0.2.0, @ceramic-sdk/test-utils@0.2.0
```

6. Push and open PR, request review and make sure all github checks pass before merging.

7. Once merged, pull main branch locally, make sure it is up to date and all latest dependencies from the lock file are installed.

```
git checkout main
git pull
pnpm install --frozen-lockfile
```

8. Run `pnpm publish -r --access public` to publish all the updated and newly created packages. The publish command will build the packages and run some checks before publishing to NPM. You can use `--dry-run` to verify before publishing, as well as `--tag next` if you want a prerelease.
