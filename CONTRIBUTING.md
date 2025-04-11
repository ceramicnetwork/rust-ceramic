## Contributing

We are happy to accept small and large contributions, feel free to make a suggestion or submit a pull request.

Use the provided `Makefile` for basic actions to ensure your changes are ready for CI.

    $ make build
    $ make check-clippy
    $ make check-fmt
    $ make test

Using the makefile is not necessary during your development cycle, feel free to use the relevant cargo commands directly, in which case you'll likely need openssl's dev files, the protocol buffer compiler and a c dev environment; on a clean slate Ubuntu:

    $ apt install make protobuf-compiler clang libssl-dev

However running `make` before publishing a PR will provide a good signal if you PR will pass CI.

### Migrations

If you need to add to the sqlite database schema, you will need to add a migration using the sqlx CLI. 

```sh
cargo install sqlx-cli
# use the name of the migration and the source directory
sqlx migrate add -r "chain_proof" --source ./migrations/sqlite
```

After the up and down files are generated, write the apply/revert SQL in the up/down files. This will be applied automatically at startup.

### Testing Specific Changes

The above `make` targets test changes as a whole.
In order to test specific changes use the Rust `cargo` tooling directly.

To run all tests via `cargo` use:

    cargo test

To run tests for a single crate in the workspace use:

    cargo test -p ceramic-event

To debug code and tests enable logging of traces:

 * By default no tracing logs are output
 * Use env var `RUST_LOG`, i.e. `RUST_LOG=debug`, to enable logging in tests. Note only failing tests will print their logs, as this is the default `cargo test` behavior.
 * Use `cargo test -- --show-output` to print logs from passing tests. This is because by default cargo test suppresses all logs from passing tests.
 * Use `cargo test -- --nocapture` to print logs from passing or failing tests as they are printed without any buffering.

See the [env_logger](https://docs.rs/env_logger/latest/env_logger/index.html) docs for more details on how `RUST_LOG` can be used.
See the [tracing](https://docs.rs/tracing/latest/tracing/#shorthand-macros) docs for more details on adding new trace events into code or tests.

### Integration Tests

The `tests` directory contains a suite of end to end tests. See its README for more details.

### Generating Servers

There are two OpenAPI based servers that are generated.
The `ceramic-api-server` and `ceramic-kubo-rpc-server` crates are generated using OpenAPI.
You will need to install augtools to run the checks:

      # Install augtools e.g. brew install augeas or apt-get install augeas-tools
      make gen-api-server
      make gen-kubo-rpc-server

## Packaging
To package rust-ceramic, you will need the following dependencies

* [jq](https://jqlang.github.io/jq/)
* [FPM](https://fpm.readthedocs.io/en/v1.15.1/) - `gem install fpm`
 * Dependent on your system, you might also need [ruby](https://www.ruby-lang.org/en/)

You can then run the [package script](./ci-scripts/package.sh) to generate a package for your operating system.

## Releasing
To release rust-ceramic, you will need the following dependencies

* [git cliff](https://git-cliff.org/docs/installation/crates-io)
* [cargo-release](https://github.com/crate-ci/cargo-release)
* [gh client](https://cli.github.com/)

When releasing, please release at the appropriate level

* `patch` -> binary compatible, no new functionality
* `minor` (default) -> binary incompatible or binary compatible with new functionality
* `major` -> breaking functionality

You will also need to login with the `gh` client to your github account, and that account needs permission to perform
a release of the rust-ceramic repository.

You can then run the [release script](./ci-scripts/release.sh) to create a release of rust-ceramic.

