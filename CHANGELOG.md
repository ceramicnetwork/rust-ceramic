# Changelog

All notable changes to this project will be documented in this file.

## [0.36.0] - 2024-09-18

### 🚀 Features

- Make feature flags top level with experimental features requiring the experimental-features flag to be set (#521)
- Api to transform events to conclusion events (#508)
- Add flight sql server to expose conclusion feed. (#523)
- Anchor service (#484)
- Add metamodel stream id constant (#529)
- Infer stream type from raw event model and add it to conclusion event + tests (#525)
- Rpc client for eth blockchains (#520)
- Verify time event proofs from eth rpc client and calculate time (#522)

### 🐛 Bug Fixes

- Disable debian repo release workflow (#519)
- Use build.rs to rebuild sql crate on migrations change (#531)

### 🚜 Refactor

- More prep for signed event validation (#526)

## [0.35.0] - 2024-09-09

### 🚀 Features

- Aes 287 create conclusion event type (#499)
- Trait for generating cbor bytes and cids (#477)
- Groundwork for event validation and some clean up (#505)
- Anchor service (part 2) (#476)
- Remote anchor transaction manager (part 3) (#478)

### 🐛 Bug Fixes

- Updates detection of tile docs and doesn't log them by default (#514)
- Start up ordering task bug (#516)

### 🚜 Refactor

- Remove recon client/server and send empty ranges when we don't need anything (#509)
- Create explicit event and interest services with separate stores. (#513)
- Simplify daemon logic (#515)

### ⚙️ Miscellaneous Tasks

- Update console-subscriber package version (#507)
- Version v0.35.0 (#517)

## [0.34.0] - 2024-08-26

### 🚀 Features

- Eth and sol pkh cacao validation (#496)
- Jws signature validation (#498)
- Check cacao resources against stream information (#502)

### 🐛 Bug Fixes

- Use cacao timestamp strings and parse (#504)

### ⚙️ Miscellaneous Tasks

- Add logic to dispatch validation based on event type (#493)
- Modify cacao, unvalidated module, jwk to add functionality needed for event validation (#492)
- Version v0.34.0 (#506)

## [0.33.0] - 2024-08-19

### 🚀 Features

- Recon event insert batching (#470)
- Modify recon store to support event validation concepts (#473)
- Add noop ceramic-olap binary (#482)

### 🐛 Bug Fixes

- Add better TileDocument detection (#475)
- Fix bug in TimeEvent witness blocks parsing logic (#486)

### 🚜 Refactor

- Create sync car crate (#485)

### ⚙️ Miscellaneous Tasks

- Flatten EventInsertable type (#480)
- Remove Clone from EventInsertable (#481)
- Change EventInsertable to store the parsed Event object instead of its raw blocks (#487)
- Remove an iteration pass in OrderEvents::try_new (#489)
- Add TODO comment (#491)
- Remove EventMetadata type (#490)
- Version v0.33.0 (#494)

## [0.32.0] - 2024-08-07

### 🚀 Features

- Feature and experimental feature flags (#448)
- Change default dir back to shared home location (#471)

### ⚙️ Miscellaneous Tasks

- Version v0.32.0 (#472)

## [0.31.0] - 2024-08-02

### 🚀 Features

- Don't deploy js-ceramic when deploying ceramic-one (#451)
- Add query parameter to return event data on feed endpoint (#398)
- Add peer id filter to interests (#456)

### 🐛 Bug Fixes

- Fix benchmark (#410)
- Create release prs via bot (#454)
- Revert to using pat for release prs (╯°□°)╯︵ ┻━┻ (#458)

### ⚙️ Miscellaneous Tasks

- Rust 1.80 lints (#450)
- Version v0.31.0 (#464)

## [0.30.0] - 2024-07-29

### 🚀 Features

- Log bind address at startup (#446)
- Expose current ceramic network via api (#441)
- Track version history in database (#409)

### 🐛 Bug Fixes

- Disable libp2p tcp port reuse (#440)
- Use default (google) dns servers if system resolv.conf is invalid (#447)

### ⚙️ Miscellaneous Tasks

- Version v0.30.0 (#449)

## [0.29.0] - 2024-07-22

### 🚀 Features

- Support cors via cli flag (#425)
- Workflow update deb repo (#437)
- Add option for external swarm addresses (#439)

### 🐛 Bug Fixes

- Add context to p2p key dir errors (#438)

### 📚 Documentation

- Adds UPGRADE.md docs (#402)

### ⚙️ Miscellaneous Tasks

- Add CODEOWNERS file (#443)
- Version v0.29.0 (#445)

## [0.28.1] - 2024-07-15

### 🚀 Features

- Add debug to all event types (#432)

### ⚙️ Miscellaneous Tasks

- Release archive with linux binary (#422)
- Update selector for post-deployment tests (#435)
- Version v0.28.1 (#436)

## [0.28.0] - 2024-07-12

### 🚀 Features

- Add devqa bootstrap peers (#430)
- [**breaking**] Change default dir to current directory (#431)

### ⚙️ Miscellaneous Tasks

- Version v0.28.0 (#433)

## [0.27.0] - 2024-07-10

### 🚀 Features

- Add explicit checking for tile documents for specific errors (#417)
- Add log-format option to migrations command (#420)
- Change default ports used for swarm and rpc endpoint (#411)
- Make private key dir configurable (#426)
- Add new mainnet and tnet bootstrap peers (#428)

### 🐛 Bug Fixes

- Update homebrew-tap push token (#418)
- Schedule post-deployment tests 15 min after deployment (#419)
- Optimize migrations logic for less memory (#424)

### 🚜 Refactor

- Allow skipping ordering all events on start up and skip tracking during migration (#423)

### 📚 Documentation

- Add cargo testing docs (#394)

### ⚙️ Miscellaneous Tasks

- Version v0.27.0 (#429)

## [0.26.0] - 2024-07-02

### 🐛 Bug Fixes

- IOD long streams could remain undelivered (#387)
- Address edge cases in from-ipfs time events (#395)
- Make sure the store directory exists and create it if needed (#401)
- Default missing sep to model (#404)
- Update correctness test selector (#406)
- Use event service rather than store to enforce deliverability/or… (#403)
- Homebrew support (#412)
- Update homebrew workflow (#413)
- Use pat for homebrew workflow (#415)

### 🚜 Refactor

- Use an Iterator for insert_many and other clean up (#399)

### ⚙️ Miscellaneous Tasks

- Mzk/readme installation (#397)
- Homebrew support (#407)
- Version v0.26.0 (#416)

## [0.25.0] - 2024-06-24

### 🚀 Features

- Adds migration logic from IPFS (#326)

### 🐛 Bug Fixes

- Adjust API insert contract and avoid failing entire batch if any one is invalid (#388)

### 🚜 Refactor

- Modify store API and rename some things  (#390)

### ⚙️ Miscellaneous Tasks

- Replace tracing_test with test-log and don't init tracing manually (#391)
- Rename prometheus registry and metrics for better consistency (#373)
- Version v0.25.0 (#396)

## [0.24.0] - 2024-06-18

### 🐛 Bug Fixes

- Remove redundant db url cli arg (#384)
- TimeEvents from single-write anchor batches don't have a separate root block (#386)

### ⚙️ Miscellaneous Tasks

- Version v0.24.0 (#389)

## [0.23.0] - 2024-06-17

### 🚀 Features

- Cd to dev (#376)
- Add builder for time events (#378)
- Add cacao serde to unvalidated::Events (#380)

### 🚜 Refactor

- Consolidate car serde logic into event crate (#379)

### ⚙️ Miscellaneous Tasks

- Run clippy fix (#377)
- Minor clean up and derive more behavior on events (#381)
- Version v0.23.0 (#383)

## [0.22.0] - 2024-06-10

### 🚀 Features

- Get feed highwater mark to start from "now" (#370)

### ⚙️ Miscellaneous Tasks

- Fix prom cardinality for test path (#372)
- Version v0.22.0 (#374)

## [0.21.0] - 2024-06-03

### 🚀 Features

- Adjust prom metrics to capture path info and status codes  (#364)
- Disallow events with unexpected fields via the http api (#363)

### ⚙️ Miscellaneous Tasks

- Rename store traits to InterestStore and EventStore (#365)
- Deserialize CAR into Event before extracting EventID. (#362)
- Version v0.21.0 (#371)

## [0.20.0] - 2024-05-24

### 🚀 Features

- Add interests endpoint for inspecting interests of a node (#353)

### 🐛 Bug Fixes

- Add context to init event header (#360)
- Events incorrectly left undelivered (#366)
- Recon protocol stall with no shared interests (#367)

### ⚙️ Miscellaneous Tasks

- Version v0.20.0 (#368)

## [0.19.0] - 2024-05-20

### 🚀 Features

- Adjust recon bounds to be lower inclusive and always store value
- Use builder pattern to create unvalidated and validated events
- Update event types and builder APIs to prevent building events that dont align with the protocol spec (#357)
- In order delivery of ceramic events (#344)

### 🐛 Bug Fixes

- Remove first/last from recon::Store trait
- Lower correctness test network ttl
- Move network ttl to hermetic driver command
- Tests
- Recon protocol hang with large diffs (#356)

### 🚜 Refactor

- Make recon value required
- Remove value parsing from recon tests
- Optimize remote missing edge cases
- Remove keys_with_missing_values from all traits
- Add service crate (#354)

### ⚙️ Miscellaneous Tasks

- Address review feedback
- PR review comments (#346)
- Clippy
- Review and test fixes
- Unused deps
- Make event struct fields private (#359)
- Version v0.19.0 (#361)

## [0.18.0] - 2024-05-13

### ⚙️ Miscellaneous Tasks

- Version v0.18.0

## [0.17.0] - 2024-05-09

### 🚀 Features

- Update events GET API to return event CID instead of EventID
- Return event root cid instead of EventID from feed
- Update experimental API for getting a range of events to return event root CID, not EventID (#337)
- Simplify event id
- Add serde methods for Events
- Make event creation endpoint infer EventID from event CAR file data (#341)
- Remove id argument from events POST endpoint (#347)

### 🐛 Bug Fixes

- Parse unsigned init events
- Handle data events with unsigned init events (#348)

### 🚜 Refactor

- Rename payload -> init_payload

### ⚙️ Miscellaneous Tasks

- SQL queries for event blocks dont need order_key
- Update comment for Event type in API (#338)
- Make gen-api-server
- Version v0.17.0

## [0.16.0] - 2024-05-08

### 🚀 Features

- Remove deprecated http API for getting events for a single interest (#332)

### 🐛 Bug Fixes

- Changes to improve sync and ingest performance (#324)
- Fix clippy (#334)

### 🚜 Refactor

- Update deps cid, multihash, and ipld (#309)
- Update recon word list test to make it easier to debug (#342)

### ⚙️ Miscellaneous Tasks

- Add bad request response to get events API (#329)
- Run build/test/check CI on all PRs (#333)
- Version v0.16.0 (#336)

## [0.15.0] - 2024-04-29

### 🚀 Features

- Validate time events (#289)
- Add workflow for custom images
- Migrations with new table format to de-dup blocks and support out of order delivery (#292)
- Don't mark peers failed when we error during sync (#300)
- Jemalloc (#319)
- Store bench (#303)
- Disable mdns,bitswap,kad,relay,dcutr protocols (#323)

### 🐛 Bug Fixes

- Use TAG_LATEST (#294)
- Set the js-ceramic image when scheduling k8 deploy (#298)
- Gh workflow syntax
- Cd-to-infra workflow quote fix (#314)
- Tokio vs std thread is okay (#318)
- Rust-jemalloc-pprof is only supported on linux (#321)

### ⚙️ Miscellaneous Tasks

- Remove mut from store traits (#305)
- Adjust types to allow dynamic usage (#306)
- Don't hang the executor (#307)
- Recon libp2p e2e tests (#310)
- Add debug image builds (#311)
- Update debug tags when not releasing (#313)
- Fix docker build for debug mode (#315)
- Add a release build with debug symbols and tokio console (#316)
- Update open telemetry and tokio-console dependencies (#322)
- Refactor to to lay groundwork for postgres support (#320)
- Add variants for error handling (#317)
- Version v0.15.0 (#325)

## [0.14.0] - 2024-03-26

### 🚀 Features

- Standard 500 error response body (ws1-1518) (#284)
- Add POST interests route that accepts body instead of path/query parameters (#285)
- Move GET events by interest route under experimental (#286)

### 🐛 Bug Fixes

- Moved api tests to a separate file (#268)

### 📚 Documentation

- Added explicit deps to build steps (#291)

### ⚙️ Miscellaneous Tasks

- Version v0.14.0 (#297)

## [0.13.0] - 2024-03-19

### 🚀 Features

- Add rust-ceramic clay bootstrap nodes (#288)

### 🐛 Bug Fixes

- Pass job labels to designate notification channels (#281)
- Remove loud warn from Recon (#287)

### ⚙️ Miscellaneous Tasks

- Delete subscribe endpoint (#277)
- Only lint single commit or PR title (#279)
- Remove support for old event payload during creation (#272)
- Version v0.13.0 (#290)

## [0.12.0] - 2024-02-22

### 🚀 Features

- Create GET endpoint to return events for an interest range (#276)

### 🐛 Bug Fixes

- Use PAT for create-release-pr workflow (#275)
- Honor PENDING_RANGES_LIMIT for initial ranges (#271)

### ⚙️ Miscellaneous Tasks

- Version v0.12.0 (#278)

## [0.11.0] - 2024-02-12

### 🚀 Features

- Enable recon by default (#270)
- Support new id/data event payload for event creation (POST /events) (#269)

### 🐛 Bug Fixes

- Store metrics under own registry (not recon) (#266)

### ⚙️ Miscellaneous Tasks

- Version v0.11.0 (#274)

## [0.10.1] - 2024-02-08

### 🐛 Bug Fixes

- Allow double insert of key/value pairs in Recon (#264)
- Use try_from on recon keys (#263)
- Resume token should return previous (not 0) when nothing found (#265)

### ⚙️ Miscellaneous Tasks

- Version v0.10.1 (#267)

## [0.10.0] - 2024-02-06

### 🚀 Features

- Continously publish provider records (#175)
- Add publisher batch metrics (#187)
- Add config for republish_max_concurrent (#192)
- Add peering support (#194)
- Add tokio metrics (#206)
- Merge migration script with ceramic-one (#190)
- Add metrics to api and recon (#208)
- Schedule_k8s_deploy github action (#213)
- Recon-over-http (#168)
- Remove all usage of gossipsub (#209)
- Stop publishing CIDs to DHT on write (#211)
- On tag publish deploy to envs (#220)
- Add sqlite read/write pool split (#218)
- Add recon store metrics (#221)
- Add value support to Recon (#217)
- Post-deployment tests (#242)
- Updated release workflow (#241)
- Modify recon storage tables, trait and sqlite config to improve throughput (#243)
- Synchronize value for synchronized ranges (#238)
- Workflow_dispatch build job (#249)
- Add unified recon block store implementation (#245)
- Interest registration endpoint (#246)
- Added correctness test (#248)
- Support for set type documents (#259)
- Add API to fetch eventData from event String (#258)
- Add feed endpoint to propagate event data to js-ceramic (#255)

### 🐛 Bug Fixes

- Check limits first before other behaviours (#183)
- Panic with divide by zero duration math (#184)
- Fix JSON log format errors (#185)
- Update comment to pass clippy (#189)
- Run publisher in its own task (#188)
- Trickle publisher keys to swarm (#191)
- Use AIMD for publisher batch size (#195)
- Do not use bootstrap list with kademlia (#199)
- Simplify the publisher (#200)
- Always collect metrics (#202)
- Typo (#203)
- Upgrade to libp2p 0.53 (#205)
- Clippy accessing first element with first (#212)
- Update deploy workflows for k8s (#216)
- Rename workflows (#223)
- Add a BUILD tag if it's not a PR merge (#256)
- Refactor recon storage and remove sqlx dependency from recon and core crates (#254)
- Update git config for release pr workflow

### 🚜 Refactor

- Update bitswap logs to use structured logging (#193)

### ⚙️ Miscellaneous Tasks

- Use latest stable openapi-generator-cli (#222)
- Use docker root user (#251)
- Adding ci for cargo machete (#252)
- Run fast post-deployment tests for all envs (#257)
- Fix false positive in checking generated servers (#260)
- Version v0.10.0 (#261)

## [0.9.0] - 2023-11-13

### 🚀 Features

- Add control over autonat (#176)

### 🐛 Bug Fixes

- Rename iroh to ceramic-one in agent (#181)

### ⚙️ Miscellaneous Tasks

- Pass manual flag through in deployment job (#180)
- Release version v0.9.0 (#182)

## [0.8.3] - 2023-11-09

### 🐛 Bug Fixes

- Call correct api method for removing block from pin store (#178)
- Be explicit about release deployments (#177)

### ⚙️ Miscellaneous Tasks

- Release version v0.8.3 (#179)

## [0.8.2] - 2023-11-08

### 🚀 Features

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

### 🐛 Bug Fixes

- Use rust-builder latest
- Remove Ceramic peer discovery
- Readd main.rs after move
- Only provide records on the DHT when they are new
- Work around github's dumb syntax for conditionals (#173)

### 🚜 Refactor

- Move iroh-metrics to ceramic-metrics
- Allow uses of deprecated metrics traits

### ⚙️ Miscellaneous Tasks

- Release version v0.8.2 (#174)

## [0.8.1] - 2023-10-26

### ⚙️ Miscellaneous Tasks

- Add CHANGELOG.md
- Release version v0.8.1

## [0.8.0] - 2023-10-25

### 🚀 Features

- Add offline parameter to block/get

### 🐛 Bug Fixes

- Need to install the openapi generator

### ⚙️ Miscellaneous Tasks

- Add new release pr workflow based on container
- Release version v0.8.0

## [0.7.0] - 2023-10-23

### 🚀 Features

- Allow comma-separated bootstrap addresses

### 🐛 Bug Fixes

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

### ⚙️ Miscellaneous Tasks

- Remove unused deps
- Cargo update
- Update libp2p to 0.52.4
- Release

## [0.6.0] - 2023-09-20

### 🐛 Bug Fixes

- Use multiline for conditional
- Only check for top level changes

### ⚙️ Miscellaneous Tasks

- Release (#130)
- Release (#131)

## [0.4.0] - 2023-09-20

### 🚀 Features

- Add liveness endpoint (#127)

### 🐛 Bug Fixes

- Target set after arch/os
- Bin path needs target
- Release conditional on PR message

### ⚙️ Miscellaneous Tasks

- Release (#128)

## [0.3.0] - 2023-09-19

### 🚀 Features

- Add initial implementation of Kubo RPC API
- Add support for pubsub/* HTTP endpoints
- Add pin endpoints
- Add block endpoints
- Adds id endpoint and adds dag-jose resolving support
- Adds eye command
- Add /metrics endpoint
- *(event)* Add event (commit) libraries for creating events for ceramic
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

### 🐛 Bug Fixes

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

### 🚜 Refactor

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

### 📚 Documentation

- Add readme and license

### ⚙️ Miscellaneous Tasks

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

### Wip

- Got actix web tests working
- Test % put are passing
- All tests passing
- Remove println

<!-- generated by git-cliff -->
