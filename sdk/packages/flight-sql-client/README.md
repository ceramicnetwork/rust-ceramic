# `@ceramic-sdk/flight-sql-client`

A client library for interacting with [Arrow Flight SQL] enabled databases from Node.js.

This library provides a thin wrapper around the flight-sql client implementation in
the [arrow-flight] crate. Node bindings are created with the help of [napi-rs]. Originally forked from lakehouse-rs [npm](https://www.npmjs.com/package/@lakehouse-rs/flight-sql-client) and [github](https://github.com/roeap/flight-sql-client-node).

## Usage

Install library

```sh
yarn add @ceramic-sdk/flight-sql-client
# or
npm install @ceramic-sdk/flight-sql-client
# or
pnpm add @ceramic-sdk/flight-sql-client
```

Create a new client instance

```ts
import { ClientOptions, createFlightSqlClient } from '@ceramic-sdk/flight-sql-client';
import { tableFromIPC } from 'apache-arrow';

const options: ClientOptions = {
  username: undefined,
  password: undefined,
  tls: false,
  host: '127.0.0.1',
  port: 5102,
  headers: [],
};

const client = await createFlightSqlClient(options);
```

Execute a query against the service

```ts
const buffer = await client.query('SELECT * FROM my_tyble');
const table = tableFromIPC(buffer);
```

Or inspect some server metadata

```ts
const buffer = await client.getTables({ includeSchema: true });
const table = tableFromIPC(buffer);
```

## Development

Requirements:

- Rust
- node.js >= 18
- Pnpm

Install dependencies via

```sh
pnpm i
```

Build native module

```sh
pnpm build
```

Run tests

```sh
pnpm test
```

## Release

TODO

[Arrow Flight SQL]: https://arrow.apache.org/docs/format/FlightSql.html
[arrow-flight]: https://crates.io/crates/arrow-flight
[napi-rs]: https://napi.rs/
