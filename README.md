# Rust Ceramic

Implementation of the Ceramic protocol in Rust.

Current status is that the `ceramic-one` binary only mimics the Kubo RPC API and relies on https://github.com/ceramicnetwork/js-ceramic for the remaining logic.

## Usage

Run in single binary using the `ceramic-one` crate:

    $ cargo run -p ceramic-one -- daemon

The process honors RUST_LOG env variable for controlling its logging output.
For example, to enable debug logging for code from this repo but error logging for all other code use:

    $ RUST_LOG=ERROR,ceramic_kubo_rpc=DEBUG,ceramic_one=DEBUG cargo run -p ceramic-one -- daemon

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

## Contributing

We are happy to accept small and large contributions, feel free to make a suggestion or submit a pull request.

Use the provided `Makefile` for basic actions to ensure your changes are ready for CI.

    $ make build
    $ make check-clippy
    $ make check-fmt
    $ make test

Using the makefile is not necessary during your development cycle, feel free to use the relevant cargo commands directly.
However running `make` before publishing a PR will provide a good signal if you PR will pass CI.

### Generating Servers

There are two OpenAPI based servers that are generated.
The `ceramic-api-server` and `ceramic-kubo-rpc-server` crates are generated using OpenAPI.
Install `@openapitools/openapi-generator-cli` and make to generate the crates:

      npm install @openapitools/openapi-generator-cli@2.6.0 -g
      make gen-api-server
      make gen-kubo-rpc-server

### Migration
This repo also contains the kubo to ceramic-one migration script.

This script will read ipfs repo files matching ~/.ipfs/blocks/*/*.data
and insert them into the ceramic-one database ~/.ceramic-one/db.sqlite3

you can run it with cargo

    $ cargo run --bin migration -- -h
    Usage: migration [OPTIONS]

    Options:
    -i, --input-ipfs-path <INPUT_IPFS_PATH>
            The path to the ipfs_repo [default: ~/.ipfs/]
    -o, --output-ceramic-path <OUTPUT_CERAMIC_PATH>
            The path to the ceramic_db [default: ~/.ceramic-one/db.sqlite3]
    -v, --verbose...
            More output per occurrence
    -q, --quiet...
            Less output per occurrence
    -h, --help
            Print help
    -V, --version
            Print version

or build it, move migration to you ceramic box, run it there.

    $ cargo build --frozen --release --bin migration
    $ cp target/release/migration ./migration
    $ ./migration -h
    Usage: migration [OPTIONS]

    Options:
    -i, --input-ipfs-path <INPUT_IPFS_PATH>
            The path to the ipfs_repo [default: ~/.ipfs/]
    -o, --output-ceramic-path <OUTPUT_CERAMIC_PATH>
            The path to the ceramic_db [default: ~/.ceramic-one/db.sqlite3]
    -v, --verbose...
            More output per occurrence
    -q, --quiet...
            Less output per occurrence
    -h, --help
            Print help
    -V, --version
            Print version


## License

Fully open source and dual-licensed under MIT and Apache 2.
