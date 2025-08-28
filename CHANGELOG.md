# Changelog

All notable changes to this project will be documented in this file.

## [0.56.0] - 2025-08-28

### 🚀 Features

- Allow configuring daemon with extra interests ([#734](https://github.com/ceramicnetwork/rust-ceramic/issues/734))

### 🐛 Bug Fixes

- *(sdk)* Use correct controller during stream creation ([#724](https://github.com/ceramicnetwork/rust-ceramic/issues/724))
- *(sdk)* Set correct unique value for mids with set relation ([#730](https://github.com/ceramicnetwork/rust-ceramic/issues/730))
- Aggregator cache flush on shutdown ([#735](https://github.com/ceramicnetwork/rust-ceramic/issues/735))

### ⚙️ Miscellaneous Tasks

- *(sdk)* Version v0.11.0 ([#727](https://github.com/ceramicnetwork/rust-ceramic/issues/727))
- *(sdk)* Version v0.12.0 ([#731](https://github.com/ceramicnetwork/rust-ceramic/issues/731))

## [0.55.1] - 2025-06-18

### 🐛 Bug Fixes

- Fetch unvalidated time proofs for conclusion events ([#723](https://github.com/ceramicnetwork/rust-ceramic/issues/723))

### ⚙️ Miscellaneous Tasks

- Version v0.55.1 ([#726](https://github.com/ceramicnetwork/rust-ceramic/issues/726))

## [0.55.0] - 2025-05-26

### 🚀 Features

- Conflict resolution scaffolding ([#687](https://github.com/ceramicnetwork/rust-ceramic/issues/687))

### 🐛 Bug Fixes

- Update more sdk repo urls ([#703](https://github.com/ceramicnetwork/rust-ceramic/issues/703))
- Skip private sdk packages ([#705](https://github.com/ceramicnetwork/rust-ceramic/issues/705))
- *(sdk)* Use recursive pnpm publish instead of npm publish
- Random things like feed queries, query CLI, migration logging, deps ([#708](https://github.com/ceramicnetwork/rust-ceramic/issues/708))
- *(sdk)* Add missing pnpm prepare directive
- Modify set account relation validation ([#713](https://github.com/ceramicnetwork/rust-ceramic/issues/713))
- Prevent panic on duplicate tracing initialization ([#718](https://github.com/ceramicnetwork/rust-ceramic/issues/718))

### 📚 Documentation

- Improved docs and logging for `migrations from-ipfs` ([#695](https://github.com/ceramicnetwork/rust-ceramic/issues/695))

### ⚙️ Miscellaneous Tasks

- *(sdk)* Version v0.6.0 ([#704](https://github.com/ceramicnetwork/rust-ceramic/issues/704))
- *(sdk)* Version v0.7.0 ([#706](https://github.com/ceramicnetwork/rust-ceramic/issues/706))
- Sdk docs update ([#709](https://github.com/ceramicnetwork/rust-ceramic/issues/709))
- *(sdk)* Version v0.8.0 ([#711](https://github.com/ceramicnetwork/rust-ceramic/issues/711))
- *(sdk)* Version v0.9.0 ([#712](https://github.com/ceramicnetwork/rust-ceramic/issues/712))
- *(sdk)* Version v0.10.0 ([#720](https://github.com/ceramicnetwork/rust-ceramic/issues/720))
- Version v0.55.0 ([#722](https://github.com/ceramicnetwork/rust-ceramic/issues/722))

## [0.5.0] - 2025-03-22

### 🐛 Bug Fixes

- Use correct paths in sdk release script ([#699](https://github.com/ceramicnetwork/rust-ceramic/issues/699))
- Update more sdk repo urls ([#701](https://github.com/ceramicnetwork/rust-ceramic/issues/701))

### ⚙️ Miscellaneous Tasks

- *(sdk)* Version v0.4.0 ([#700](https://github.com/ceramicnetwork/rust-ceramic/issues/700))
- *(sdk)* Version v0.5.0 ([#702](https://github.com/ceramicnetwork/rust-ceramic/issues/702))

## [0.54.2] - 2025-03-21

### 🐛 Bug Fixes

- Update the sdk repo url ([#696](https://github.com/ceramicnetwork/rust-ceramic/issues/696))

### ⚙️ Miscellaneous Tasks

- *(sdk)* Version v0.3.0 ([#694](https://github.com/ceramicnetwork/rust-ceramic/issues/694))
- Version v0.54.2 ([#697](https://github.com/ceramicnetwork/rust-ceramic/issues/697))

## [0.54.1] - 2025-03-21

### 🐛 Bug Fixes

- Make apply patch not fail the pipeline ([#690](https://github.com/ceramicnetwork/rust-ceramic/issues/690))

### 📚 Documentation

- Add pipeline docs ([#691](https://github.com/ceramicnetwork/rust-ceramic/issues/691))

### ⚙️ Miscellaneous Tasks

- Add CI workflows to publish sdk ([#689](https://github.com/ceramicnetwork/rust-ceramic/issues/689))
- Fix sdk_release_pr script ([#693](https://github.com/ceramicnetwork/rust-ceramic/issues/693))
- Version v0.54.1 ([#692](https://github.com/ceramicnetwork/rust-ceramic/issues/692))

## [0.54.0] - 2025-03-19

### 🚀 Features

- Add some validation logic to migration from-ipfs ([#685](https://github.com/ceramicnetwork/rust-ceramic/issues/685))

### 🐛 Bug Fixes

- Make sdk use correct id in data event payloads ([#673](https://github.com/ceramicnetwork/rust-ceramic/issues/673))
- Use new default gnosis rpc endpoint ([#683](https://github.com/ceramicnetwork/rust-ceramic/issues/683))
- Remove distinct step as its prohibitively expensive ([#682](https://github.com/ceramicnetwork/rust-ceramic/issues/682))

### ⚙️ Miscellaneous Tasks

- Do not deploy to durable envs ([#670](https://github.com/ceramicnetwork/rust-ceramic/issues/670))
- Update CODEOWNERS ([#672](https://github.com/ceramicnetwork/rust-ceramic/issues/672))
- Autogen sdk docs, ci check they are generated ([#674](https://github.com/ceramicnetwork/rust-ceramic/issues/674))
- Use different gnosis rpc url ([#684](https://github.com/ceramicnetwork/rust-ceramic/issues/684))
- Version v0.54.0 ([#688](https://github.com/ceramicnetwork/rust-ceramic/issues/688))

## [0.53.0] - 2025-03-10

### 🚀 Features

- Make ceramic-sdk part of the repo, promote flight sql and aggregator ([#669](https://github.com/ceramicnetwork/rust-ceramic/issues/669))

### ⚙️ Miscellaneous Tasks

- Migrates ceramic-tests to this repo ([#668](https://github.com/ceramicnetwork/rust-ceramic/issues/668))
- Version v0.53.0 ([#671](https://github.com/ceramicnetwork/rust-ceramic/issues/671))

## [0.52.0] - 2025-02-24

### 🚀 Features

- Schema validation ([#663](https://github.com/ceramicnetwork/rust-ceramic/issues/663))

### ⚙️ Miscellaneous Tasks

- Version v0.52.0 ([#667](https://github.com/ceramicnetwork/rust-ceramic/issues/667))

## [0.51.0] - 2025-02-18

### 🚀 Features

- Adds stream_id_string udf ([#653](https://github.com/ceramicnetwork/rust-ceramic/issues/653))
- Add dimension_extract udf ([#654](https://github.com/ceramicnetwork/rust-ceramic/issues/654))
- Adds output format options to query command ([#659](https://github.com/ceramicnetwork/rust-ceramic/issues/659))
- [**breaking**] Partition event states by event cid ([#658](https://github.com/ceramicnetwork/rust-ceramic/issues/658))
- Add/update UDFs for the ceramic pipeline ([#662](https://github.com/ceramicnetwork/rust-ceramic/issues/662))

### 🐛 Bug Fixes

- Use invoke_batch on UDFs as invoke is deprecated ([#652](https://github.com/ceramicnetwork/rust-ceramic/issues/652))
- Adds batching logic to conclusion-feed-table ([#656](https://github.com/ceramicnetwork/rust-ceramic/issues/656))

### 🚜 Refactor

- More efficient list udf impls ([#657](https://github.com/ceramicnetwork/rust-ceramic/issues/657))

### ⚙️ Miscellaneous Tasks

- Version v0.49.0 ([#660](https://github.com/ceramicnetwork/rust-ceramic/issues/660))
- Use artifacts v4 as v3 is deprecated ([#661](https://github.com/ceramicnetwork/rust-ceramic/issues/661))
- Version v0.50.0 ([#664](https://github.com/ceramicnetwork/rust-ceramic/issues/664))
- Fix name conflict in linux artifacts ([#665](https://github.com/ceramicnetwork/rust-ceramic/issues/665))
- Version v0.51.0 ([#666](https://github.com/ceramicnetwork/rust-ceramic/issues/666))

## [0.48.0] - 2025-01-28

### 🚀 Features

- Adds actor framework ([#644](https://github.com/ceramicnetwork/rust-ceramic/issues/644))
- Stream flight sql queries ([#645](https://github.com/ceramicnetwork/rust-ceramic/issues/645))
- Add pipeline metrics ([#646](https://github.com/ceramicnetwork/rust-ceramic/issues/646))

### 🐛 Bug Fixes

- Update built dep ([#641](https://github.com/ceramicnetwork/rust-ceramic/issues/641))
- Fix off by one, projection and not found bugs ([#647](https://github.com/ceramicnetwork/rust-ceramic/issues/647))
- Conclusion poll loop no longer stops ([#648](https://github.com/ceramicnetwork/rust-ceramic/issues/648))

### 🚜 Refactor

- Add simpler shutdown handling ([#643](https://github.com/ceramicnetwork/rust-ceramic/issues/643))
- Rename _stream tables to _feed ([#649](https://github.com/ceramicnetwork/rust-ceramic/issues/649))

### ⚙️ Miscellaneous Tasks

- Make clippy happy about lifetimes ([#642](https://github.com/ceramicnetwork/rust-ceramic/issues/642))
- Fix event-svc bench ([#639](https://github.com/ceramicnetwork/rust-ceramic/issues/639))
- Version v0.48.0 ([#650](https://github.com/ceramicnetwork/rust-ceramic/issues/650))

## [0.47.3] - 2025-01-13

### 🐛 Bug Fixes

- Use new default testnet eth rpc url ([#638](https://github.com/ceramicnetwork/rust-ceramic/issues/638))

### ⚙️ Miscellaneous Tasks

- Add tests for eip55 cacaos ([#636](https://github.com/ceramicnetwork/rust-ceramic/issues/636))
- Version v0.47.3 ([#640](https://github.com/ceramicnetwork/rust-ceramic/issues/640))

## [0.47.2] - 2024-12-20

### 🐛 Bug Fixes

- Shutdown ordering tasks with sigint ([#629](https://github.com/ceramicnetwork/rust-ceramic/issues/629))

### ⚙️ Miscellaneous Tasks

- Use explicit os versions ([#634](https://github.com/ceramicnetwork/rust-ceramic/issues/634))
- Version v0.47.1 ([#635](https://github.com/ceramicnetwork/rust-ceramic/issues/635))
- Fix issue in package script
- Version v0.47.2 ([#637](https://github.com/ceramicnetwork/rust-ceramic/issues/637))

## [0.47.0] - 2024-12-18

### 🚀 Features

- Stop synchronizing interests ([#632](https://github.com/ceramicnetwork/rust-ceramic/issues/632))

### 🐛 Bug Fixes

- Use debug for peer dial failures ([#631](https://github.com/ceramicnetwork/rust-ceramic/issues/631))

### ⚙️ Miscellaneous Tasks

- Version v0.47.0 ([#633](https://github.com/ceramicnetwork/rust-ceramic/issues/633))

## [0.46.0] - 2024-12-09

### 🚀 Features

- Add peer entry and peer key structs ([#615](https://github.com/ceramicnetwork/rust-ceramic/issues/615))
- Add support for local file object store ([#623](https://github.com/ceramicnetwork/rust-ceramic/issues/623))
- Add support for prepared statements ([#600](https://github.com/ceramicnetwork/rust-ceramic/issues/600))
- Add peer recon ring ([#616](https://github.com/ceramicnetwork/rust-ceramic/issues/616))
- Use many tasks to order streams and discover undelivered events at startup ([#620](https://github.com/ceramicnetwork/rust-ceramic/issues/620))
- Adds transitive sharing of peer information ([#618](https://github.com/ceramicnetwork/rust-ceramic/issues/618))
- Adds /peers endpoint ([#628](https://github.com/ceramicnetwork/rust-ceramic/issues/628))

### 🐛 Bug Fixes

- Sqlite optimize during connect could timeout and allow specifying db options ([#622](https://github.com/ceramicnetwork/rust-ceramic/issues/622))

### 🚜 Refactor

- Remove offset/limit where too generic ([#624](https://github.com/ceramicnetwork/rust-ceramic/issues/624))
- Remove unused version code ([#627](https://github.com/ceramicnetwork/rust-ceramic/issues/627))

### ⚙️ Miscellaneous Tasks

- Log current highwater mark when processing events ([#619](https://github.com/ceramicnetwork/rust-ceramic/issues/619))
- Run sqlite optimize at startup and interval ([#621](https://github.com/ceramicnetwork/rust-ceramic/issues/621))
- Use arrow release 53.3 ([#625](https://github.com/ceramicnetwork/rust-ceramic/issues/625))
- Version v0.46.0 ([#630](https://github.com/ceramicnetwork/rust-ceramic/issues/630))

## [0.45.0] - 2024-11-25

### 🚀 Features

- Add api to get stream state ([#586](https://github.com/ceramicnetwork/rust-ceramic/issues/586))

### 🐛 Bug Fixes

- Expose various db errors when storing events ([#601](https://github.com/ceramicnetwork/rust-ceramic/issues/601))
- Handle double write of same data ([#613](https://github.com/ceramicnetwork/rust-ceramic/issues/613))

### ⚙️ Miscellaneous Tasks

- Automatically select release level ([#612](https://github.com/ceramicnetwork/rust-ceramic/issues/612))
- Version v0.45.0 ([#617](https://github.com/ceramicnetwork/rust-ceramic/issues/617))

## [0.44.0] - 2024-11-18

### 🚀 Features

- Backoff recon synchronization to peers with bad data ([#597](https://github.com/ceramicnetwork/rust-ceramic/issues/597))

### 🚜 Refactor

- Consistent pipeline table names and schemas ([#587](https://github.com/ceramicnetwork/rust-ceramic/issues/587))

### ⚙️ Miscellaneous Tasks

- Version v0.44.0 ([#598](https://github.com/ceramicnetwork/rust-ceramic/issues/598))

## [0.43.0] - 2024-11-14

### 🚀 Features

- Use cache table to make queries faster and writes to object store ([#576](https://github.com/ceramicnetwork/rust-ceramic/issues/576))

### ⚙️ Miscellaneous Tasks

- Update ci env to dev-qa cluster ([#584](https://github.com/ceramicnetwork/rust-ceramic/issues/584))
- Version v0.43.0 ([#585](https://github.com/ceramicnetwork/rust-ceramic/issues/585))

## [0.42.0] - 2024-11-07

### 🚀 Features

- Expose all tables via flight sql ([#571](https://github.com/ceramicnetwork/rust-ceramic/issues/571))

### ⚙️ Miscellaneous Tasks

- Use larger github runner for image builds ([#577](https://github.com/ceramicnetwork/rust-ceramic/issues/577))
- Version v0.42.0 ([#579](https://github.com/ceramicnetwork/rust-ceramic/issues/579))

## [0.41.1] - 2024-11-06

### 🐛 Bug Fixes

- Remove superflous $ in cacao model auth check ([#573](https://github.com/ceramicnetwork/rust-ceramic/issues/573))
- Make error message match the js-ceramic version ([#574](https://github.com/ceramicnetwork/rust-ceramic/issues/574))

### ⚙️ Miscellaneous Tasks

- Version v0.41.1 ([#575](https://github.com/ceramicnetwork/rust-ceramic/issues/575))

## [0.41.0] - 2024-11-04

### 🚀 Features

- Turn on event validation by default ([#562](https://github.com/ceramicnetwork/rust-ceramic/issues/562))

### 🐛 Bug Fixes

- Correct path to latest tarball ([#566](https://github.com/ceramicnetwork/rust-ceramic/issues/566))
- Size 1 anchor trees ([#565](https://github.com/ceramicnetwork/rust-ceramic/issues/565))

### 🚜 Refactor

- Modify ethereum rpc trait for hoku support ([#550](https://github.com/ceramicnetwork/rust-ceramic/issues/550))
- Make ChainInclusion trait fully generic and change input to the raw AnchorProof ([#570](https://github.com/ceramicnetwork/rust-ceramic/issues/570))

### ⚙️ Miscellaneous Tasks

- Version v0.41.0 ([#572](https://github.com/ceramicnetwork/rust-ceramic/issues/572))

## [0.40.0] - 2024-10-17

### 🚀 Features

- Protect FlightSQL server with sql options. ([#544](https://github.com/ceramicnetwork/rust-ceramic/issues/544))
- Aes 291 continuous queries in olap aggregator ([#538](https://github.com/ceramicnetwork/rust-ceramic/issues/538))
- Migrate from file list ([#501](https://github.com/ceramicnetwork/rust-ceramic/issues/501))
- Use data container wrapper for mutable metadata ([#559](https://github.com/ceramicnetwork/rust-ceramic/issues/559))
- Add query command to ceramic-one ([#545](https://github.com/ceramicnetwork/rust-ceramic/issues/545))

### 🐛 Bug Fixes

- Only write out error counts on errors ([#560](https://github.com/ceramicnetwork/rust-ceramic/issues/560))
- Use correct index for the conclusion feed. ([#561](https://github.com/ceramicnetwork/rust-ceramic/issues/561))
- Use tokio::time::Interval for scheduling anchor batches ([#563](https://github.com/ceramicnetwork/rust-ceramic/issues/563))
- Subscribe to server shutdown signal in insert task ([#553](https://github.com/ceramicnetwork/rust-ceramic/issues/553))

### ⚙️ Miscellaneous Tasks

- Record the per item duration of an insert_many request ([#551](https://github.com/ceramicnetwork/rust-ceramic/issues/551))
- Version v0.40.0 ([#564](https://github.com/ceramicnetwork/rust-ceramic/issues/564))

## [0.39.0] - 2024-10-07

### 🐛 Bug Fixes

- Allow local and inmemory networks to start up without an eth rpc url ([#555](https://github.com/ceramicnetwork/rust-ceramic/issues/555))

### ⚙️ Miscellaneous Tasks

- Always log options at startup ([#554](https://github.com/ceramicnetwork/rust-ceramic/issues/554))
- Version v0.39.0 ([#557](https://github.com/ceramicnetwork/rust-ceramic/issues/557))

## [0.38.0] - 2024-10-07

### 🚀 Features

- Validate signed events ([#503](https://github.com/ceramicnetwork/rust-ceramic/issues/503))
- Self-anchoring (part 5) ([#488](https://github.com/ceramicnetwork/rust-ceramic/issues/488))
- Support non sharded IPFS block paths ([#541](https://github.com/ceramicnetwork/rust-ceramic/issues/541))
- Store doc_state table in object storage ([#540](https://github.com/ceramicnetwork/rust-ceramic/issues/540))
- Add logic to migration to count errors by model ([#542](https://github.com/ceramicnetwork/rust-ceramic/issues/542))
- Validate time event chain inclusion proofs ([#539](https://github.com/ceramicnetwork/rust-ceramic/issues/539))
- Process batches on the interval ([#548](https://github.com/ceramicnetwork/rust-ceramic/issues/548))
- Ethereum RPC provider configuration ([#547](https://github.com/ceramicnetwork/rust-ceramic/issues/547))

### 🐛 Bug Fixes

- Make self-anchoring config experimental ([#546](https://github.com/ceramicnetwork/rust-ceramic/issues/546))

### 🚜 Refactor

- Return reference to inserted item from event store and reduce unnecessary ordering work  ([#530](https://github.com/ceramicnetwork/rust-ceramic/issues/530))
- Teach event validator some more and store a copy on the service ([#536](https://github.com/ceramicnetwork/rust-ceramic/issues/536))
- Use alloy as ethereum RPC provider ([#543](https://github.com/ceramicnetwork/rust-ceramic/issues/543))

### ⚙️ Miscellaneous Tasks

- Update ca-certificates so eth rpc connections ([#549](https://github.com/ceramicnetwork/rust-ceramic/issues/549))
- Version v0.38.0 ([#552](https://github.com/ceramicnetwork/rust-ceramic/issues/552))

## [0.37.0] - 2024-09-23

### 🚀 Features

- Add webauthn signature validation ([#511](https://github.com/ceramicnetwork/rust-ceramic/issues/511))
- Support dimensions in conclusion feed ([#535](https://github.com/ceramicnetwork/rust-ceramic/issues/535))

### 🐛 Bug Fixes

- Spawn std task instead of tokio to avoid blocking runtime ([#532](https://github.com/ceramicnetwork/rust-ceramic/issues/532))

### 🚜 Refactor

- Small logic change for readability ([#534](https://github.com/ceramicnetwork/rust-ceramic/issues/534))

### ⚙️ Miscellaneous Tasks

- Version v0.37.0 ([#537](https://github.com/ceramicnetwork/rust-ceramic/issues/537))

## [0.36.0] - 2024-09-18

### 🚀 Features

- Make feature flags top level with experimental features requiring the experimental-features flag to be set ([#521](https://github.com/ceramicnetwork/rust-ceramic/issues/521))
- Api to transform events to conclusion events ([#508](https://github.com/ceramicnetwork/rust-ceramic/issues/508))
- Add flight sql server to expose conclusion feed. ([#523](https://github.com/ceramicnetwork/rust-ceramic/issues/523))
- Anchor service ([#484](https://github.com/ceramicnetwork/rust-ceramic/issues/484))
- Add metamodel stream id constant ([#529](https://github.com/ceramicnetwork/rust-ceramic/issues/529))
- Infer stream type from raw event model and add it to conclusion event + tests ([#525](https://github.com/ceramicnetwork/rust-ceramic/issues/525))
- Rpc client for eth blockchains ([#520](https://github.com/ceramicnetwork/rust-ceramic/issues/520))
- Verify time event proofs from eth rpc client and calculate time ([#522](https://github.com/ceramicnetwork/rust-ceramic/issues/522))
- Add olap aggregation function ([#527](https://github.com/ceramicnetwork/rust-ceramic/issues/527))

### 🐛 Bug Fixes

- Disable debian repo release workflow ([#519](https://github.com/ceramicnetwork/rust-ceramic/issues/519))
- Use build.rs to rebuild sql crate on migrations change ([#531](https://github.com/ceramicnetwork/rust-ceramic/issues/531))

### 🚜 Refactor

- More prep for signed event validation ([#526](https://github.com/ceramicnetwork/rust-ceramic/issues/526))

### ⚙️ Miscellaneous Tasks

- Version v0.36.0 ([#533](https://github.com/ceramicnetwork/rust-ceramic/issues/533))

## [0.35.0] - 2024-09-09

### 🚀 Features

- Aes 287 create conclusion event type ([#499](https://github.com/ceramicnetwork/rust-ceramic/issues/499))
- Trait for generating cbor bytes and cids ([#477](https://github.com/ceramicnetwork/rust-ceramic/issues/477))
- Groundwork for event validation and some clean up ([#505](https://github.com/ceramicnetwork/rust-ceramic/issues/505))
- Anchor service (part 2) ([#476](https://github.com/ceramicnetwork/rust-ceramic/issues/476))
- Remote anchor transaction manager (part 3) ([#478](https://github.com/ceramicnetwork/rust-ceramic/issues/478))

### 🐛 Bug Fixes

- Updates detection of tile docs and doesn't log them by default ([#514](https://github.com/ceramicnetwork/rust-ceramic/issues/514))
- Start up ordering task bug ([#516](https://github.com/ceramicnetwork/rust-ceramic/issues/516))

### 🚜 Refactor

- Remove recon client/server and send empty ranges when we don't need anything ([#509](https://github.com/ceramicnetwork/rust-ceramic/issues/509))
- Create explicit event and interest services with separate stores. ([#513](https://github.com/ceramicnetwork/rust-ceramic/issues/513))
- Simplify daemon logic ([#515](https://github.com/ceramicnetwork/rust-ceramic/issues/515))

### ⚙️ Miscellaneous Tasks

- Update console-subscriber package version ([#507](https://github.com/ceramicnetwork/rust-ceramic/issues/507))
- Version v0.35.0 ([#517](https://github.com/ceramicnetwork/rust-ceramic/issues/517))

## [0.34.0] - 2024-08-26

### 🚀 Features

- Eth and sol pkh cacao validation ([#496](https://github.com/ceramicnetwork/rust-ceramic/issues/496))
- Jws signature validation ([#498](https://github.com/ceramicnetwork/rust-ceramic/issues/498))
- Check cacao resources against stream information ([#502](https://github.com/ceramicnetwork/rust-ceramic/issues/502))

### 🐛 Bug Fixes

- Use cacao timestamp strings and parse ([#504](https://github.com/ceramicnetwork/rust-ceramic/issues/504))

### ⚙️ Miscellaneous Tasks

- Add logic to dispatch validation based on event type ([#493](https://github.com/ceramicnetwork/rust-ceramic/issues/493))
- Modify cacao, unvalidated module, jwk to add functionality needed for event validation ([#492](https://github.com/ceramicnetwork/rust-ceramic/issues/492))
- Version v0.34.0 ([#506](https://github.com/ceramicnetwork/rust-ceramic/issues/506))

## [0.33.0] - 2024-08-19

### 🚀 Features

- Recon event insert batching ([#470](https://github.com/ceramicnetwork/rust-ceramic/issues/470))
- Modify recon store to support event validation concepts ([#473](https://github.com/ceramicnetwork/rust-ceramic/issues/473))
- Add noop ceramic-olap binary ([#482](https://github.com/ceramicnetwork/rust-ceramic/issues/482))

### 🐛 Bug Fixes

- Add better TileDocument detection ([#475](https://github.com/ceramicnetwork/rust-ceramic/issues/475))
- Fix bug in TimeEvent witness blocks parsing logic ([#486](https://github.com/ceramicnetwork/rust-ceramic/issues/486))

### 🚜 Refactor

- Create sync car crate ([#485](https://github.com/ceramicnetwork/rust-ceramic/issues/485))

### ⚙️ Miscellaneous Tasks

- Flatten EventInsertable type ([#480](https://github.com/ceramicnetwork/rust-ceramic/issues/480))
- Remove Clone from EventInsertable ([#481](https://github.com/ceramicnetwork/rust-ceramic/issues/481))
- Change EventInsertable to store the parsed Event object instead of its raw blocks ([#487](https://github.com/ceramicnetwork/rust-ceramic/issues/487))
- Remove an iteration pass in OrderEvents::try_new ([#489](https://github.com/ceramicnetwork/rust-ceramic/issues/489))
- Add TODO comment ([#491](https://github.com/ceramicnetwork/rust-ceramic/issues/491))
- Remove EventMetadata type ([#490](https://github.com/ceramicnetwork/rust-ceramic/issues/490))
- Version v0.33.0 ([#494](https://github.com/ceramicnetwork/rust-ceramic/issues/494))

## [0.32.0] - 2024-08-07

### 🚀 Features

- Feature and experimental feature flags ([#448](https://github.com/ceramicnetwork/rust-ceramic/issues/448))
- Change default dir back to shared home location ([#471](https://github.com/ceramicnetwork/rust-ceramic/issues/471))

### ⚙️ Miscellaneous Tasks

- Version v0.32.0 ([#472](https://github.com/ceramicnetwork/rust-ceramic/issues/472))

## [0.31.0] - 2024-08-02

### 🚀 Features

- Don't deploy js-ceramic when deploying ceramic-one ([#451](https://github.com/ceramicnetwork/rust-ceramic/issues/451))
- Add query parameter to return event data on feed endpoint ([#398](https://github.com/ceramicnetwork/rust-ceramic/issues/398))
- Add peer id filter to interests ([#456](https://github.com/ceramicnetwork/rust-ceramic/issues/456))

### 🐛 Bug Fixes

- Fix benchmark ([#410](https://github.com/ceramicnetwork/rust-ceramic/issues/410))
- Create release prs via bot ([#454](https://github.com/ceramicnetwork/rust-ceramic/issues/454))
- Revert to using pat for release prs (╯°□°)╯︵ ┻━┻ ([#458](https://github.com/ceramicnetwork/rust-ceramic/issues/458))

### ⚙️ Miscellaneous Tasks

- Rust 1.80 lints ([#450](https://github.com/ceramicnetwork/rust-ceramic/issues/450))
- Version v0.31.0 ([#464](https://github.com/ceramicnetwork/rust-ceramic/issues/464))

## [0.30.0] - 2024-07-29

### 🚀 Features

- Log bind address at startup ([#446](https://github.com/ceramicnetwork/rust-ceramic/issues/446))
- Expose current ceramic network via api ([#441](https://github.com/ceramicnetwork/rust-ceramic/issues/441))
- Track version history in database ([#409](https://github.com/ceramicnetwork/rust-ceramic/issues/409))

### 🐛 Bug Fixes

- Disable libp2p tcp port reuse ([#440](https://github.com/ceramicnetwork/rust-ceramic/issues/440))
- Use default (google) dns servers if system resolv.conf is invalid ([#447](https://github.com/ceramicnetwork/rust-ceramic/issues/447))

### ⚙️ Miscellaneous Tasks

- Version v0.30.0 ([#449](https://github.com/ceramicnetwork/rust-ceramic/issues/449))

## [0.29.0] - 2024-07-22

### 🚀 Features

- Support cors via cli flag ([#425](https://github.com/ceramicnetwork/rust-ceramic/issues/425))
- Workflow update deb repo ([#437](https://github.com/ceramicnetwork/rust-ceramic/issues/437))
- Add option for external swarm addresses ([#439](https://github.com/ceramicnetwork/rust-ceramic/issues/439))

### 🐛 Bug Fixes

- Add context to p2p key dir errors ([#438](https://github.com/ceramicnetwork/rust-ceramic/issues/438))

### 📚 Documentation

- Adds UPGRADE.md docs ([#402](https://github.com/ceramicnetwork/rust-ceramic/issues/402))

### ⚙️ Miscellaneous Tasks

- Add CODEOWNERS file ([#443](https://github.com/ceramicnetwork/rust-ceramic/issues/443))
- Version v0.29.0 ([#445](https://github.com/ceramicnetwork/rust-ceramic/issues/445))

## [0.28.1] - 2024-07-15

### 🚀 Features

- Add debug to all event types ([#432](https://github.com/ceramicnetwork/rust-ceramic/issues/432))

### ⚙️ Miscellaneous Tasks

- Release archive with linux binary ([#422](https://github.com/ceramicnetwork/rust-ceramic/issues/422))
- Update selector for post-deployment tests ([#435](https://github.com/ceramicnetwork/rust-ceramic/issues/435))
- Version v0.28.1 ([#436](https://github.com/ceramicnetwork/rust-ceramic/issues/436))

## [0.28.0] - 2024-07-12

### 🚀 Features

- Add devqa bootstrap peers ([#430](https://github.com/ceramicnetwork/rust-ceramic/issues/430))
- [**breaking**] Change default dir to current directory ([#431](https://github.com/ceramicnetwork/rust-ceramic/issues/431))

### ⚙️ Miscellaneous Tasks

- Version v0.28.0 ([#433](https://github.com/ceramicnetwork/rust-ceramic/issues/433))

## [0.27.0] - 2024-07-10

### 🚀 Features

- Add explicit checking for tile documents for specific errors ([#417](https://github.com/ceramicnetwork/rust-ceramic/issues/417))
- Add log-format option to migrations command ([#420](https://github.com/ceramicnetwork/rust-ceramic/issues/420))
- Change default ports used for swarm and rpc endpoint ([#411](https://github.com/ceramicnetwork/rust-ceramic/issues/411))
- Make private key dir configurable ([#426](https://github.com/ceramicnetwork/rust-ceramic/issues/426))
- Add new mainnet and tnet bootstrap peers ([#428](https://github.com/ceramicnetwork/rust-ceramic/issues/428))

### 🐛 Bug Fixes

- Update homebrew-tap push token ([#418](https://github.com/ceramicnetwork/rust-ceramic/issues/418))
- Schedule post-deployment tests 15 min after deployment ([#419](https://github.com/ceramicnetwork/rust-ceramic/issues/419))
- Optimize migrations logic for less memory ([#424](https://github.com/ceramicnetwork/rust-ceramic/issues/424))

### 🚜 Refactor

- Allow skipping ordering all events on start up and skip tracking during migration ([#423](https://github.com/ceramicnetwork/rust-ceramic/issues/423))

### 📚 Documentation

- Add cargo testing docs ([#394](https://github.com/ceramicnetwork/rust-ceramic/issues/394))

### ⚙️ Miscellaneous Tasks

- Version v0.27.0 ([#429](https://github.com/ceramicnetwork/rust-ceramic/issues/429))

## [0.26.0] - 2024-07-02

### 🐛 Bug Fixes

- IOD long streams could remain undelivered ([#387](https://github.com/ceramicnetwork/rust-ceramic/issues/387))
- Address edge cases in from-ipfs time events ([#395](https://github.com/ceramicnetwork/rust-ceramic/issues/395))
- Make sure the store directory exists and create it if needed ([#401](https://github.com/ceramicnetwork/rust-ceramic/issues/401))
- Default missing sep to model ([#404](https://github.com/ceramicnetwork/rust-ceramic/issues/404))
- Update correctness test selector ([#406](https://github.com/ceramicnetwork/rust-ceramic/issues/406))
- Use event service rather than store to enforce deliverability/or… ([#403](https://github.com/ceramicnetwork/rust-ceramic/issues/403))
- Homebrew support ([#412](https://github.com/ceramicnetwork/rust-ceramic/issues/412))
- Update homebrew workflow ([#413](https://github.com/ceramicnetwork/rust-ceramic/issues/413))
- Use pat for homebrew workflow ([#415](https://github.com/ceramicnetwork/rust-ceramic/issues/415))

### 🚜 Refactor

- Use an Iterator for insert_many and other clean up ([#399](https://github.com/ceramicnetwork/rust-ceramic/issues/399))

### ⚙️ Miscellaneous Tasks

- Mzk/readme installation ([#397](https://github.com/ceramicnetwork/rust-ceramic/issues/397))
- Homebrew support ([#407](https://github.com/ceramicnetwork/rust-ceramic/issues/407))
- Version v0.26.0 ([#416](https://github.com/ceramicnetwork/rust-ceramic/issues/416))

## [0.25.0] - 2024-06-24

### 🚀 Features

- Adds migration logic from IPFS ([#326](https://github.com/ceramicnetwork/rust-ceramic/issues/326))

### 🐛 Bug Fixes

- Adjust API insert contract and avoid failing entire batch if any one is invalid ([#388](https://github.com/ceramicnetwork/rust-ceramic/issues/388))

### 🚜 Refactor

- Modify store API and rename some things  ([#390](https://github.com/ceramicnetwork/rust-ceramic/issues/390))

### ⚙️ Miscellaneous Tasks

- Replace tracing_test with test-log and don't init tracing manually ([#391](https://github.com/ceramicnetwork/rust-ceramic/issues/391))
- Rename prometheus registry and metrics for better consistency ([#373](https://github.com/ceramicnetwork/rust-ceramic/issues/373))
- Version v0.25.0 ([#396](https://github.com/ceramicnetwork/rust-ceramic/issues/396))

## [0.24.0] - 2024-06-18

### 🐛 Bug Fixes

- Remove redundant db url cli arg ([#384](https://github.com/ceramicnetwork/rust-ceramic/issues/384))
- TimeEvents from single-write anchor batches don't have a separate root block ([#386](https://github.com/ceramicnetwork/rust-ceramic/issues/386))

### ⚙️ Miscellaneous Tasks

- Version v0.24.0 ([#389](https://github.com/ceramicnetwork/rust-ceramic/issues/389))

## [0.23.0] - 2024-06-17

### 🚀 Features

- Cd to dev ([#376](https://github.com/ceramicnetwork/rust-ceramic/issues/376))
- Add builder for time events ([#378](https://github.com/ceramicnetwork/rust-ceramic/issues/378))
- Add cacao serde to unvalidated::Events ([#380](https://github.com/ceramicnetwork/rust-ceramic/issues/380))

### 🚜 Refactor

- Consolidate car serde logic into event crate ([#379](https://github.com/ceramicnetwork/rust-ceramic/issues/379))

### ⚙️ Miscellaneous Tasks

- Run clippy fix ([#377](https://github.com/ceramicnetwork/rust-ceramic/issues/377))
- Minor clean up and derive more behavior on events ([#381](https://github.com/ceramicnetwork/rust-ceramic/issues/381))
- Version v0.23.0 ([#383](https://github.com/ceramicnetwork/rust-ceramic/issues/383))

## [0.22.0] - 2024-06-10

### 🚀 Features

- Get feed highwater mark to start from "now" ([#370](https://github.com/ceramicnetwork/rust-ceramic/issues/370))

### ⚙️ Miscellaneous Tasks

- Fix prom cardinality for test path ([#372](https://github.com/ceramicnetwork/rust-ceramic/issues/372))
- Version v0.22.0 ([#374](https://github.com/ceramicnetwork/rust-ceramic/issues/374))

## [0.21.0] - 2024-06-03

### 🚀 Features

- Adjust prom metrics to capture path info and status codes  ([#364](https://github.com/ceramicnetwork/rust-ceramic/issues/364))
- Disallow events with unexpected fields via the http api ([#363](https://github.com/ceramicnetwork/rust-ceramic/issues/363))

### ⚙️ Miscellaneous Tasks

- Rename store traits to InterestStore and EventStore ([#365](https://github.com/ceramicnetwork/rust-ceramic/issues/365))
- Deserialize CAR into Event before extracting EventID. ([#362](https://github.com/ceramicnetwork/rust-ceramic/issues/362))
- Version v0.21.0 ([#371](https://github.com/ceramicnetwork/rust-ceramic/issues/371))

## [0.20.0] - 2024-05-24

### 🚀 Features

- Add interests endpoint for inspecting interests of a node ([#353](https://github.com/ceramicnetwork/rust-ceramic/issues/353))

### 🐛 Bug Fixes

- Add context to init event header ([#360](https://github.com/ceramicnetwork/rust-ceramic/issues/360))
- Events incorrectly left undelivered ([#366](https://github.com/ceramicnetwork/rust-ceramic/issues/366))
- Recon protocol stall with no shared interests ([#367](https://github.com/ceramicnetwork/rust-ceramic/issues/367))

### ⚙️ Miscellaneous Tasks

- Version v0.20.0 ([#368](https://github.com/ceramicnetwork/rust-ceramic/issues/368))

## [0.19.0] - 2024-05-20

### 🚀 Features

- Adjust recon bounds to be lower inclusive and always store value
- Use builder pattern to create unvalidated and validated events
- Update event types and builder APIs to prevent building events that dont align with the protocol spec ([#357](https://github.com/ceramicnetwork/rust-ceramic/issues/357))
- In order delivery of ceramic events ([#344](https://github.com/ceramicnetwork/rust-ceramic/issues/344))

### 🐛 Bug Fixes

- Remove first/last from recon::Store trait
- Lower correctness test network ttl
- Move network ttl to hermetic driver command
- Tests
- Recon protocol hang with large diffs ([#356](https://github.com/ceramicnetwork/rust-ceramic/issues/356))

### 🚜 Refactor

- Make recon value required
- Remove value parsing from recon tests
- Optimize remote missing edge cases
- Remove keys_with_missing_values from all traits
- Add service crate ([#354](https://github.com/ceramicnetwork/rust-ceramic/issues/354))

### ⚙️ Miscellaneous Tasks

- Address review feedback
- PR review comments ([#346](https://github.com/ceramicnetwork/rust-ceramic/issues/346))
- Clippy
- Review and test fixes
- Unused deps
- Make event struct fields private ([#359](https://github.com/ceramicnetwork/rust-ceramic/issues/359))
- Version v0.19.0 ([#361](https://github.com/ceramicnetwork/rust-ceramic/issues/361))

## [0.18.0] - 2024-05-13

### ⚙️ Miscellaneous Tasks

- Version v0.18.0

## [0.17.0] - 2024-05-09

### 🚀 Features

- Update events GET API to return event CID instead of EventID
- Return event root cid instead of EventID from feed
- Update experimental API for getting a range of events to return event root CID, not EventID ([#337](https://github.com/ceramicnetwork/rust-ceramic/issues/337))
- Simplify event id
- Add serde methods for Events
- Make event creation endpoint infer EventID from event CAR file data ([#341](https://github.com/ceramicnetwork/rust-ceramic/issues/341))
- Remove id argument from events POST endpoint ([#347](https://github.com/ceramicnetwork/rust-ceramic/issues/347))

### 🐛 Bug Fixes

- Parse unsigned init events
- Handle data events with unsigned init events ([#348](https://github.com/ceramicnetwork/rust-ceramic/issues/348))

### 🚜 Refactor

- Rename payload -> init_payload

### ⚙️ Miscellaneous Tasks

- SQL queries for event blocks dont need order_key
- Update comment for Event type in API ([#338](https://github.com/ceramicnetwork/rust-ceramic/issues/338))
- Make gen-api-server
- Version v0.17.0

## [0.16.0] - 2024-05-08

### 🚀 Features

- Remove deprecated http API for getting events for a single interest ([#332](https://github.com/ceramicnetwork/rust-ceramic/issues/332))

### 🐛 Bug Fixes

- Changes to improve sync and ingest performance ([#324](https://github.com/ceramicnetwork/rust-ceramic/issues/324))
- Fix clippy ([#334](https://github.com/ceramicnetwork/rust-ceramic/issues/334))

### 🚜 Refactor

- Update deps cid, multihash, and ipld ([#309](https://github.com/ceramicnetwork/rust-ceramic/issues/309))
- Update recon word list test to make it easier to debug ([#342](https://github.com/ceramicnetwork/rust-ceramic/issues/342))

### ⚙️ Miscellaneous Tasks

- Add bad request response to get events API ([#329](https://github.com/ceramicnetwork/rust-ceramic/issues/329))
- Run build/test/check CI on all PRs ([#333](https://github.com/ceramicnetwork/rust-ceramic/issues/333))
- Version v0.16.0 ([#336](https://github.com/ceramicnetwork/rust-ceramic/issues/336))

## [0.15.0] - 2024-04-29

### 🚀 Features

- Validate time events ([#289](https://github.com/ceramicnetwork/rust-ceramic/issues/289))
- Add workflow for custom images
- Migrations with new table format to de-dup blocks and support out of order delivery ([#292](https://github.com/ceramicnetwork/rust-ceramic/issues/292))
- Don't mark peers failed when we error during sync ([#300](https://github.com/ceramicnetwork/rust-ceramic/issues/300))
- Jemalloc ([#319](https://github.com/ceramicnetwork/rust-ceramic/issues/319))
- Store bench ([#303](https://github.com/ceramicnetwork/rust-ceramic/issues/303))
- Disable mdns,bitswap,kad,relay,dcutr protocols ([#323](https://github.com/ceramicnetwork/rust-ceramic/issues/323))

### 🐛 Bug Fixes

- Use TAG_LATEST ([#294](https://github.com/ceramicnetwork/rust-ceramic/issues/294))
- Set the js-ceramic image when scheduling k8 deploy ([#298](https://github.com/ceramicnetwork/rust-ceramic/issues/298))
- Gh workflow syntax
- Cd-to-infra workflow quote fix ([#314](https://github.com/ceramicnetwork/rust-ceramic/issues/314))
- Tokio vs std thread is okay ([#318](https://github.com/ceramicnetwork/rust-ceramic/issues/318))
- Rust-jemalloc-pprof is only supported on linux ([#321](https://github.com/ceramicnetwork/rust-ceramic/issues/321))

### ⚙️ Miscellaneous Tasks

- Remove mut from store traits ([#305](https://github.com/ceramicnetwork/rust-ceramic/issues/305))
- Adjust types to allow dynamic usage ([#306](https://github.com/ceramicnetwork/rust-ceramic/issues/306))
- Don't hang the executor ([#307](https://github.com/ceramicnetwork/rust-ceramic/issues/307))
- Recon libp2p e2e tests ([#310](https://github.com/ceramicnetwork/rust-ceramic/issues/310))
- Add debug image builds ([#311](https://github.com/ceramicnetwork/rust-ceramic/issues/311))
- Update debug tags when not releasing ([#313](https://github.com/ceramicnetwork/rust-ceramic/issues/313))
- Fix docker build for debug mode ([#315](https://github.com/ceramicnetwork/rust-ceramic/issues/315))
- Add a release build with debug symbols and tokio console ([#316](https://github.com/ceramicnetwork/rust-ceramic/issues/316))
- Update open telemetry and tokio-console dependencies ([#322](https://github.com/ceramicnetwork/rust-ceramic/issues/322))
- Refactor to to lay groundwork for postgres support ([#320](https://github.com/ceramicnetwork/rust-ceramic/issues/320))
- Add variants for error handling ([#317](https://github.com/ceramicnetwork/rust-ceramic/issues/317))
- Version v0.15.0 ([#325](https://github.com/ceramicnetwork/rust-ceramic/issues/325))

## [0.14.0] - 2024-03-26

### 🚀 Features

- Standard 500 error response body (ws1-1518) ([#284](https://github.com/ceramicnetwork/rust-ceramic/issues/284))
- Add POST interests route that accepts body instead of path/query parameters ([#285](https://github.com/ceramicnetwork/rust-ceramic/issues/285))
- Move GET events by interest route under experimental ([#286](https://github.com/ceramicnetwork/rust-ceramic/issues/286))

### 🐛 Bug Fixes

- Moved api tests to a separate file ([#268](https://github.com/ceramicnetwork/rust-ceramic/issues/268))

### 📚 Documentation

- Added explicit deps to build steps ([#291](https://github.com/ceramicnetwork/rust-ceramic/issues/291))

### ⚙️ Miscellaneous Tasks

- Version v0.14.0 ([#297](https://github.com/ceramicnetwork/rust-ceramic/issues/297))

## [0.13.0] - 2024-03-19

### 🚀 Features

- Add rust-ceramic clay bootstrap nodes ([#288](https://github.com/ceramicnetwork/rust-ceramic/issues/288))

### 🐛 Bug Fixes

- Pass job labels to designate notification channels ([#281](https://github.com/ceramicnetwork/rust-ceramic/issues/281))
- Remove loud warn from Recon ([#287](https://github.com/ceramicnetwork/rust-ceramic/issues/287))

### ⚙️ Miscellaneous Tasks

- Delete subscribe endpoint ([#277](https://github.com/ceramicnetwork/rust-ceramic/issues/277))
- Only lint single commit or PR title ([#279](https://github.com/ceramicnetwork/rust-ceramic/issues/279))
- Remove support for old event payload during creation ([#272](https://github.com/ceramicnetwork/rust-ceramic/issues/272))
- Version v0.13.0 ([#290](https://github.com/ceramicnetwork/rust-ceramic/issues/290))

## [0.12.0] - 2024-02-22

### 🚀 Features

- Create GET endpoint to return events for an interest range ([#276](https://github.com/ceramicnetwork/rust-ceramic/issues/276))

### 🐛 Bug Fixes

- Use PAT for create-release-pr workflow ([#275](https://github.com/ceramicnetwork/rust-ceramic/issues/275))
- Honor PENDING_RANGES_LIMIT for initial ranges ([#271](https://github.com/ceramicnetwork/rust-ceramic/issues/271))

### ⚙️ Miscellaneous Tasks

- Version v0.12.0 ([#278](https://github.com/ceramicnetwork/rust-ceramic/issues/278))

## [0.11.0] - 2024-02-12

### 🚀 Features

- Enable recon by default ([#270](https://github.com/ceramicnetwork/rust-ceramic/issues/270))
- Support new id/data event payload for event creation (POST /events) ([#269](https://github.com/ceramicnetwork/rust-ceramic/issues/269))

### 🐛 Bug Fixes

- Store metrics under own registry (not recon) ([#266](https://github.com/ceramicnetwork/rust-ceramic/issues/266))

### ⚙️ Miscellaneous Tasks

- Version v0.11.0 ([#274](https://github.com/ceramicnetwork/rust-ceramic/issues/274))

## [0.10.1] - 2024-02-08

### 🐛 Bug Fixes

- Allow double insert of key/value pairs in Recon ([#264](https://github.com/ceramicnetwork/rust-ceramic/issues/264))
- Use try_from on recon keys ([#263](https://github.com/ceramicnetwork/rust-ceramic/issues/263))
- Resume token should return previous (not 0) when nothing found ([#265](https://github.com/ceramicnetwork/rust-ceramic/issues/265))

### ⚙️ Miscellaneous Tasks

- Version v0.10.1 ([#267](https://github.com/ceramicnetwork/rust-ceramic/issues/267))

## [0.10.0] - 2024-02-06

### 🚀 Features

- Continously publish provider records ([#175](https://github.com/ceramicnetwork/rust-ceramic/issues/175))
- Add publisher batch metrics ([#187](https://github.com/ceramicnetwork/rust-ceramic/issues/187))
- Add config for republish_max_concurrent ([#192](https://github.com/ceramicnetwork/rust-ceramic/issues/192))
- Add peering support ([#194](https://github.com/ceramicnetwork/rust-ceramic/issues/194))
- Add tokio metrics ([#206](https://github.com/ceramicnetwork/rust-ceramic/issues/206))
- Merge migration script with ceramic-one ([#190](https://github.com/ceramicnetwork/rust-ceramic/issues/190))
- Add metrics to api and recon ([#208](https://github.com/ceramicnetwork/rust-ceramic/issues/208))
- Schedule_k8s_deploy github action ([#213](https://github.com/ceramicnetwork/rust-ceramic/issues/213))
- Recon-over-http ([#168](https://github.com/ceramicnetwork/rust-ceramic/issues/168))
- Remove all usage of gossipsub ([#209](https://github.com/ceramicnetwork/rust-ceramic/issues/209))
- Stop publishing CIDs to DHT on write ([#211](https://github.com/ceramicnetwork/rust-ceramic/issues/211))
- On tag publish deploy to envs ([#220](https://github.com/ceramicnetwork/rust-ceramic/issues/220))
- Add sqlite read/write pool split ([#218](https://github.com/ceramicnetwork/rust-ceramic/issues/218))
- Add recon store metrics ([#221](https://github.com/ceramicnetwork/rust-ceramic/issues/221))
- Add value support to Recon ([#217](https://github.com/ceramicnetwork/rust-ceramic/issues/217))
- Post-deployment tests ([#242](https://github.com/ceramicnetwork/rust-ceramic/issues/242))
- Updated release workflow ([#241](https://github.com/ceramicnetwork/rust-ceramic/issues/241))
- Modify recon storage tables, trait and sqlite config to improve throughput ([#243](https://github.com/ceramicnetwork/rust-ceramic/issues/243))
- Synchronize value for synchronized ranges ([#238](https://github.com/ceramicnetwork/rust-ceramic/issues/238))
- Workflow_dispatch build job ([#249](https://github.com/ceramicnetwork/rust-ceramic/issues/249))
- Add unified recon block store implementation ([#245](https://github.com/ceramicnetwork/rust-ceramic/issues/245))
- Interest registration endpoint ([#246](https://github.com/ceramicnetwork/rust-ceramic/issues/246))
- Added correctness test ([#248](https://github.com/ceramicnetwork/rust-ceramic/issues/248))
- Support for set type documents ([#259](https://github.com/ceramicnetwork/rust-ceramic/issues/259))
- Add API to fetch eventData from event String ([#258](https://github.com/ceramicnetwork/rust-ceramic/issues/258))
- Add feed endpoint to propagate event data to js-ceramic ([#255](https://github.com/ceramicnetwork/rust-ceramic/issues/255))

### 🐛 Bug Fixes

- Check limits first before other behaviours ([#183](https://github.com/ceramicnetwork/rust-ceramic/issues/183))
- Panic with divide by zero duration math ([#184](https://github.com/ceramicnetwork/rust-ceramic/issues/184))
- Fix JSON log format errors ([#185](https://github.com/ceramicnetwork/rust-ceramic/issues/185))
- Update comment to pass clippy ([#189](https://github.com/ceramicnetwork/rust-ceramic/issues/189))
- Run publisher in its own task ([#188](https://github.com/ceramicnetwork/rust-ceramic/issues/188))
- Trickle publisher keys to swarm ([#191](https://github.com/ceramicnetwork/rust-ceramic/issues/191))
- Use AIMD for publisher batch size ([#195](https://github.com/ceramicnetwork/rust-ceramic/issues/195))
- Do not use bootstrap list with kademlia ([#199](https://github.com/ceramicnetwork/rust-ceramic/issues/199))
- Simplify the publisher ([#200](https://github.com/ceramicnetwork/rust-ceramic/issues/200))
- Always collect metrics ([#202](https://github.com/ceramicnetwork/rust-ceramic/issues/202))
- Typo ([#203](https://github.com/ceramicnetwork/rust-ceramic/issues/203))
- Upgrade to libp2p 0.53 ([#205](https://github.com/ceramicnetwork/rust-ceramic/issues/205))
- Clippy accessing first element with first ([#212](https://github.com/ceramicnetwork/rust-ceramic/issues/212))
- Update deploy workflows for k8s ([#216](https://github.com/ceramicnetwork/rust-ceramic/issues/216))
- Rename workflows ([#223](https://github.com/ceramicnetwork/rust-ceramic/issues/223))
- Add a BUILD tag if it's not a PR merge ([#256](https://github.com/ceramicnetwork/rust-ceramic/issues/256))
- Refactor recon storage and remove sqlx dependency from recon and core crates ([#254](https://github.com/ceramicnetwork/rust-ceramic/issues/254))
- Update git config for release pr workflow

### 🚜 Refactor

- Update bitswap logs to use structured logging ([#193](https://github.com/ceramicnetwork/rust-ceramic/issues/193))

### ⚙️ Miscellaneous Tasks

- Use latest stable openapi-generator-cli ([#222](https://github.com/ceramicnetwork/rust-ceramic/issues/222))
- Use docker root user ([#251](https://github.com/ceramicnetwork/rust-ceramic/issues/251))
- Adding ci for cargo machete ([#252](https://github.com/ceramicnetwork/rust-ceramic/issues/252))
- Run fast post-deployment tests for all envs ([#257](https://github.com/ceramicnetwork/rust-ceramic/issues/257))
- Fix false positive in checking generated servers ([#260](https://github.com/ceramicnetwork/rust-ceramic/issues/260))
- Version v0.10.0 ([#261](https://github.com/ceramicnetwork/rust-ceramic/issues/261))

## [0.9.0] - 2023-11-13

### 🚀 Features

- Add control over autonat ([#176](https://github.com/ceramicnetwork/rust-ceramic/issues/176))

### 🐛 Bug Fixes

- Rename iroh to ceramic-one in agent ([#181](https://github.com/ceramicnetwork/rust-ceramic/issues/181))

### ⚙️ Miscellaneous Tasks

- Pass manual flag through in deployment job ([#180](https://github.com/ceramicnetwork/rust-ceramic/issues/180))
- Release version v0.9.0 ([#182](https://github.com/ceramicnetwork/rust-ceramic/issues/182))

## [0.8.3] - 2023-11-09

### 🐛 Bug Fixes

- Call correct api method for removing block from pin store ([#178](https://github.com/ceramicnetwork/rust-ceramic/issues/178))
- Be explicit about release deployments ([#177](https://github.com/ceramicnetwork/rust-ceramic/issues/177))

### ⚙️ Miscellaneous Tasks

- Release version v0.8.3 ([#179](https://github.com/ceramicnetwork/rust-ceramic/issues/179))

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
- Cd ([#172](https://github.com/ceramicnetwork/rust-ceramic/issues/172))

### 🐛 Bug Fixes

- Use rust-builder latest
- Remove Ceramic peer discovery
- Readd main.rs after move
- Only provide records on the DHT when they are new
- Work around github's dumb syntax for conditionals ([#173](https://github.com/ceramicnetwork/rust-ceramic/issues/173))

### 🚜 Refactor

- Move iroh-metrics to ceramic-metrics
- Allow uses of deprecated metrics traits

### ⚙️ Miscellaneous Tasks

- Release version v0.8.2 ([#174](https://github.com/ceramicnetwork/rust-ceramic/issues/174))

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

- Release ([#130](https://github.com/ceramicnetwork/rust-ceramic/issues/130))
- Release ([#131](https://github.com/ceramicnetwork/rust-ceramic/issues/131))

## [0.4.0] - 2023-09-20

### 🚀 Features

- Add liveness endpoint ([#127](https://github.com/ceramicnetwork/rust-ceramic/issues/127))

### 🐛 Bug Fixes

- Target set after arch/os
- Bin path needs target
- Release conditional on PR message

### ⚙️ Miscellaneous Tasks

- Release ([#128](https://github.com/ceramicnetwork/rust-ceramic/issues/128))

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
- Perform release ([#121](https://github.com/ceramicnetwork/rust-ceramic/issues/121))
- Release by creating a PR to create a tag, that when merged triggers a release ([#123](https://github.com/ceramicnetwork/rust-ceramic/issues/123))
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
- Usage of args should correctly parse and apply now ([#110](https://github.com/ceramicnetwork/rust-ceramic/issues/110))
- Change bitswap dial behavior
- Args are hard
- Return None instead of error if block is missing
- Release
- Move cliff correctly
- Cleanup tmp files so they don't show dirty in release
- Just use star for cleanup ([#119](https://github.com/ceramicnetwork/rust-ceramic/issues/119))
- Return v1 cid when fetching blocks
- Slash ([#122](https://github.com/ceramicnetwork/rust-ceramic/issues/122))
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

### 💼 Other

- Got actix web tests working
- Test % put are passing
- All tests passing
- Remove println

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
- Release ([#125](https://github.com/ceramicnetwork/rust-ceramic/issues/125))
- Release ([#126](https://github.com/ceramicnetwork/rust-ceramic/issues/126))

<!-- generated by git-cliff -->
