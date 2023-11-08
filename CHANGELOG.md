# Changelog

All notable changes to this project will be documented in this file.

## [0.8.2] - 2023-11-08

### Bug Fixes

- Use rust-builder latest
- Remove Ceramic peer discovery
- Readd main.rs after move
- Only provide records on the DHT when they are new
- Work around github's dumb syntax for conditionals (#173)

### Features

- Add explicit log format CLI option
- Add ceramic_one_info metric
- Add exe_hash to info
- Default swarm port
- Bootstrap peers
- Add kubo rpc api metrics
- Add cli opts for connection limits
- Add ipfs metrics
- Expose basic kademlia config
- Cd (#172)

### Refactor

- Move iroh-metrics to ceramic-metrics
- Allow uses of deprecated metrics traits

## [0.8.1] - 2023-10-26

### Miscellaneous Tasks

- Add CHANGELOG.md
- Release version v0.8.1

## [0.8.0] - 2023-10-25

### Bug Fixes

- Need to install the openapi generator

### Features

- Add offline parameter to block/get

### Miscellaneous Tasks

- Add new release pr workflow based on container
- Release version v0.8.0

## [0.7.0] - 2023-10-23

### Bug Fixes

- Upgrade ssi to fix incompatibilities
- Kubo rpc alignment
- Remove unique violation error check when inserting block
- Add kubo-rpc api error logs
- Optimize sqlx pool options
- Append peer id to all listen addrs
- Change pubsub/ls to return base64url encoded topics
- Add block get timeout
- Improve API instrumentation
- Update external address handling for kad

### Features

- Allow comma-separated bootstrap addresses

### Miscellaneous Tasks

- Remove unused deps
- Cargo update
- Update libp2p to 0.52.4
- Release

## [0.6.0] - 2023-09-20

### Bug Fixes

- Release conditional on PR message
- Use multiline for conditional
- Only check for top level changes

### Miscellaneous Tasks

- Release (#130)
- Release (#131)

## [0.4.0] - 2023-09-20

### Bug Fixes

- Target set after arch/os
- Bin path needs target

### Features

- Add liveness endpoint (#127)

### Miscellaneous Tasks

- Release (#128)

## [0.3.0] - 2023-09-19

### Bug Fixes

- Fix all clippy errors
- Require docs and no warnings
- Update 'get' to resolve paths
- Add run target to makefile
- Do not add bootstrap peers
- Use aws creds to login to ecr
- Add use to test case that has many needed types
- Merge errors
- Remove cspell.json
- Work correctly with http client
- Fixes from PR review
- Add tests for recon libp2p
- Fix tests and clippy warnings
- Fmt
- Update lalrpop-util types
- Remove use of cbor, simplify exposing lalrpop in testing
- Address PR review feedback
- Regex char not allowed, change api names
- Change to underscore for naming
- Sort_key bytes bugs
- Key Bytes max_value
- PRAGMA can't use execute
- Fmt
- Cleaner conn_for_filename
- Format don't add strings
- Add sqlite to image
- Update stream type for all supported types
- Use single sql query to get first and last
- Make signer send and allow jwk to be cloned
- Wrap returned peers in "Peers" key
- Release not releases
- Add protoc to release step
- Cache full
- Grab artifacts from artifacts path
- Debug info for packaging
- Remove cycle in debug output
- Reduce build size
- Usage of args should correctly parse and apply now (#110)
- Change bitswap dial behavior
- Args are hard
- Return None instead of error if block is missing
- Release
- Move cliff correctly
- Cleanup tmp files so they don't show dirty in release
- Just use star for cleanup (#119)
- Return v1 cid when fetching blocks
- Slash (#122)
- Rename workflows and use different action
- Release all not detected
- Make versions match
- Use workspace version and only release ceramic-one
- Lock and remove shallow clone
- Install protoc
- Only release if main and version updated
- Don't release if dep changes
- Verify tag for release
- Can run manually
- Disable skip for now
- Combine tag extraction
- Proper release tag
- Try a pat
- Can only use tgz files
- Should listen to copilot

### Documentation

- Add readme and license

### Features

- Add initial implementation of Kubo RPC API
- Add support for pubsub/* HTTP endpoints
- Add pin endpoints
- Add block endpoints
- Adds id endpoint and adds dag-jose resolving support
- Adds eye command
- Add /metrics endpoint
- Add event (commit) libraries for creating events for ceramic
- Add AHash and Recon
- Add necessary fixes for keramik usage
- Implement libp2p Recon protocol
- Serialize eventid
- Add ceramic peer discovery
- Recon-bytes
- Add openapi implementation of events api
- Add offset/limit to subscribe endpoint
- Add sort-key in path
- Add eventId cid parsing
- Add synchronization of interest ranges
- Api subscribe now creates an interest
- Upgrade Recon protocol to honor interest ranges
- Sqlite durability
- Separator from "sort_key|sort_key"
- Recon kilo test fixes
- Msg-size
- Dapp functionality
- Add missing kubo endpoints for import and version
- Use debian image
- Adjustments for js-ceramic tests
- Add version endpoint to ceramic api server
- Add switch to disable/enable Recon
- Release workflow
- Msg-size
- Perform release (#121)
- Release by creating a PR to create a tag, that when merged triggers a release (#123)
- Merge-from-sqlite

### Miscellaneous Tasks

- Add basic CI workflows
- Add protoc install step
- Update ci to use merge_queue
- Add cargo caching to CI
- Explicitly update all deps
- Update makefile
- Add dependabot config
- Add conventional commit action
- Add dockerfile for running ceramic-one
- Remove stutter in directory names
- Use serde versions from workspace
- Only deny warnings during CI
- Setup docker buildx for Github Actions
- Fix buildx missing load option
- Use 3box fork of beetle
- Update deps
- Update beetle dep
- Use dtolnay rust install action
- Add check-api-server make target
- Update beetle dep to point as main
- Use sccache for better Rust caches
- Release (#125)
- Release (#126)

### Refactor

- Moves http logic into a single module
- Break http into multiple modules
- Replace iroh-p2p with ceramic-p2p
- Make Recon generic over keys
- Use RangeOpen instead of Range
- Rename sqlitestore
- Remove mutex locking of Recon
- Update if let into match for readability
- Reworks kubo-rpc as an openapi server
- Move beetle locally

### Wip

- Got actix web tests working
- Test % put are passing
- All tests passing
- Remove println

<!-- generated by git-cliff -->
