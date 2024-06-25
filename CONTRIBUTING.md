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
