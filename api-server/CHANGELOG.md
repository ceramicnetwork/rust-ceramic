# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0](https://github.com/3box/rust-ceramic/releases/tag/ceramic-api-server-v0.1.0) - 2023-09-18

### Added
- Release workflow
- add version endpoint to ceramic api server
- add sort-key in path
- add offset/limit to subscribe endpoint
- add openapi implementation of events api

### Fixed
- release
- change to underscore for naming
- regex char not allowed, change api names

### Other
- reworks kubo-rpc as an openapi server
- add check-api-server make target
