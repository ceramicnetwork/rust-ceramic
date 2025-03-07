# Ceramic SDK

TypeScript client and utilities for [Ceramic One](https://github.com/ceramicnetwork/rust-ceramic) interactions

## Packages

| Name                                                          | Description                                  | Version                                                                               |
| ------------------------------------------------------------- | -------------------------------------------- | ------------------------------------------------------------------------------------- |
| [events](./packages/events)                                   | Events encoding, signing and other utilities | ![npm version](https://img.shields.io/npm/v/@ceramic-sdk/events.svg)                  |
| [flight-sql-client](./packages/flight-sql-client)             | Flight SQL client for ceramic one using WASM | ![npm version](https://img.shields.io/npm/v/@ceramic-sdk/flight-sql-client.svg)       |
| [http-client](./packages/http-client)                         | HTTP client for Ceramic One                  | ![npm version](https://img.shields.io/npm/v/@ceramic-sdk/http-client.svg)             |
| [identifiers](./packages/identifiers)                         | Ceramic streams and commits identifiers      | ![npm version](https://img.shields.io/npm/v/@ceramic-sdk/identifiers.svg)             |
| [model-protocol](./packages/model-protocol)                   | Model streams protocol                       | ![npm version](https://img.shields.io/npm/v/@ceramic-sdk/model-protocol.svg)          |
| [model-client](./packages/model-client)                       | Model streams client                         | ![npm version](https://img.shields.io/npm/v/@ceramic-sdk/model-client.svg)            |
| [model-instance-protocol](./packages/model-instance-protocol) | ModelInstanceDocument streams protocol       | ![npm version](https://img.shields.io/npm/v/@ceramic-sdk/model-instance-protocol.svg) |
| [model-instance-client](./packages/model-instance-client)     | ModelInstanceDocument streams client         | ![npm version](https://img.shields.io/npm/v/@ceramic-sdk/model-instance-client.svg)   |
| [stream-client](./packages/stream-client)                     | Generic streams client                       | ![npm version](https://img.shields.io/npm/v/@ceramic-sdk/stream-client.svg)           |

Other packages present in the `packages` folder are for internal use and may not be published to the npm registry.

## Development

Getting started:

```sh
pnpm i
pnpm build
pnpm test # run all tests (unit and integration, requires docker to be running)
pnpm test:ci # run only unit tests
```

The flight-sql-client is written in Rust and has the ability to log to the console. However the jest framework will often overwrite the logs output by the Rust code.
To avoid this use:

```sh
CI=true RUST_LOG=debug pnpm test
```

The `RUST_LOG` env var can be set to change the Rust log level.

## CI

In order to specify targets for WASM builds on CI, the build script is split into `pnpm build:rust` which allows passing `--target TARGET_TRIPLE` and `pnpm build:js`.

## License

Dual licensed under MIT and Apache 2
