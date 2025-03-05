# Ceramic Integration Tests

This repo contains an end-to-end test suite for the Ceramic Network.

## Design

This repo provides three entities:

* A test suite of end-to-end tests.
* A hermetic test driver, which runs the test suite against isolated infrastructure.
* A durable test driver, which runs the test suite against live public network infrastructure.

The test suite is compiled into a docker image and expects to be configured with the endpoints of the infrastructure to
test against.
The test drivers run the test image providing the correct configuration for their intended infrastructure.

Hermetic infrastructure refers to infrastructure this is isolated from the rest of the world. In other words it controls
all of its dependencies.
Testing against hermetic infrastructure allows for easier debugging as all dependencies are controlled and can be
isolated for potential bugs.

Durable infrastructure refers to infrastructure running connected to the rest of the world. For example other anonymous
Ceramic nodes may connect to this infrastructure, and it relies on a public Ethereum blockchain.
Testing against durable infrastructure allows for better coverage of real world edge cases.

## Flavors

There are several test flavors:

* Correctness tests
* Migration tests
* Performance tests

More will be added as needed.

The correctness tests, test a specific property of a network (i.e. writes can be read).
These tests do not assume any network topology.
Correctness tests live in this repo in the `/tests/suite` directory.

The migration tests run tests specific to migrating from older versions to newer versions of Ceramic.
These tests assume that when the version of the Ceramic process changes the migration is complete and the test can continue to validate the migration.
Migrations tests live in this repo in the `/tests/suite/src/__tests__/migration` directory.

The performance tests, test at scale.
These tests do not assume any network topology.
Performance tests live outside of this repo.

## Prerequisites

These test assume you have already set up Keramik.  If you are not also making changes to Keramik itself,
you will likely want to configure Keramik to always use the 'latest' imageTag.  See comments in
k8s/operator/kustomization.yaml for instructions on how to do that.

## Environment Variables

The test suite is configured via environment variables.

| Env Var        | Description                                             |
| -------        | -----------                                             |
| COMPOSEDB_URLS | Comma separated list of URLs to ComposeDB API endpoints |
| CERAMIC_URLS   | Comma separated list of URLs to Ceramic API endpoints   |

Note, these names use the new API boundaries being currently constructed.
So confusingly the `js-ceramic` process exposes the `ComposeDB API` while the `rust-ceramic` process exposes the
`Ceramic API`.

Eventually we expect the `js-ceramic` repo/process to be renamed to `js-composedb` and the `js-composedb` repo being
renamed to `js-compsedb-client` or other similar names.


## Running tests locally

Tests are run in CI via the build docker image. However, locally it's useful to be able to run the tests without doing
an image build.
There are some helpful scripts in this repo to make testing locally possible:
```bash
    cd tests
    # Create the network
    kubectl apply -f networks/basic-rust.yaml

    # Wait for network to be ready then,
    # port forward local ports into each ceramic node.
    # We `source` the script so it can export the env vars.
    source ./port-forward.sh

    # This will run "fast" tests locally using the newly exported env vars.
    cd suite
    pnpm install # do NOT `pnpm run build`. if you do, remove the .build directory
    pnpm run test --testPathPattern fast
```
>NOTE: The port-forward script will leave `kubectl` process running in the background to forward the ports.
The script kills those processes and creates new ones. However, if you need you can kill them directly.
The script should be run anytime a pod in the network restarts.

You can also run these tests against durable infrastructure (e.g. Testnet):
```bash
    cd tests/suite
    DURABLE_ENV=tnet DOTENV_CONFIG_PATH=./env/.env."$DURABLE_ENV" pnpm run test --testPathPattern fast
```
### Visualizing Topology

When debugging a test failure it can be helpful to visualize the current network topology.
Use this script:

    node topology.mjs

The script will create a `topology.svg` (view it in a browser) of how each of the nodes are connected to one another.

>NOTE: The topology.mjs script expects the same environment variables to exist in order to communicate with the nodes to
> determine their topology.
Source the `port-forward.sh` script in order to expose the environment locally.

## Using make

Make provides targets for each test driver and the intermediate steps to build all dependencies.

The make file exposes variables that can be set in the environment to control how it performs each task.

### Example workflow

Using this workflow you can build the test suite image locally, load the image into kind and then invoke the driver to
run the tests in the network.

    cd tests
    # Choose a meaningful but local name for the image
    export TEST_SUITE_IMAGE_NAME=3box/ceramic-tests-suite
    # Build the test suite image locally, do not publish
    make build-suite
    # Load the built image into kind
    kind load docker-image 3box/ceramic-tests-suite:dev-run
    # Build and run the hermetic test driver using the test suite image
    make hermetic-tests

Using make is not required, feel free to peak inside the Makefile in order to call the test driver directly for whatever
workflow you may need.

### Debugging failures

If you want to get the logs from any process started as part of running tests (the ceramic node, the CAS, the test
output itself), you can use:
`kubectl logs -n <namespace> <pod name>`

You can see a list of running kubernetes pods and their namespace and pod name by running `k9s`.

### Cleaning up

To tear down all the pods created by running the tests, you can delete the network that they were set up in.

    > kubectl get networks # Look up the network name
    NAME                          AGE
    basic-rust-dev-run   33m

    > kubectl delete network basic-rust-dev-run # delete the network and all pods within it.

## Contributing

We are happy to accept small and large contributions, feel free to make a suggestion or submit a pull request.

Use the provided `Makefile` for basic actions to ensure your changes are ready for CI.

    $ cd tests
    $ make build
    $ make check-clippy
    $ make check-fmt

Using the makefile is not necessary during your development cycle, feel free to use the relevant cargo commands
directly. However, running `make` before publishing a PR will provide a good signal if your PR will pass CI.

## License

Fully open source and dual-licensed under MIT and Apache 2.
