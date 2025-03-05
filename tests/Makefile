# Makefile provides an API for CI related tasks
# Using the makefile is not required however CI
# uses the specific targets within the file.
# Therefore may be useful in ensuring a change
# is ready to pass CI checks.

# Set Rust build profile, one of release or dev.
BUILD_PROFILE ?= release
# Unique identifier for builds and tags.
BUILD_TAG ?= dev-run
# Path to network to test against.
TEST_NETWORK ?= ./networks/basic-rust.yaml
# Path to migration network to test against.
TEST_PRE_MIGRATION_NETWORK ?= ./migration-networks/basic-go-rust-pre.yaml
# Path to migration network to test against.
TEST_POST_MIGRATION_NETWORK ?= ./migration-networks/basic-go-rust-post.yaml
# Path to performance simulation.
TEST_SIMULATION ?= ./simulations/basic-simulation.yaml
# Name for the test suite image, without any tag
TEST_SUITE_IMAGE_NAME ?= public.ecr.aws/r5b3e0r5/3box/ceramic-tests-suite
# Name for the hermetic-driver image, without any tag
HERMETIC_DRIVER_IMAGE_NAME ?= public.ecr.aws/r5b3e0r5/3box/hermetic-driver
# Full path of the test suite image with a tag
TEST_SUITE_IMAGE ?= ${TEST_SUITE_IMAGE_NAME}:${BUILD_TAG}

CARGO = RUSTFLAGS="-D warnings" cargo
CARGO_RUN = ${CARGO} run --locked --profile ${BUILD_PROFILE}
CARGO_BUILD = ${CARGO} build --locked --profile ${BUILD_PROFILE}

# Command to run the hermetic driver
# Defaults to using cargo run
HERMETIC_CMD ?= ${CARGO_RUN} --bin hermetic-driver --

# Suffix to use for hermetic tests
HERMETIC_SUFFIX ?= ${BUILD_TAG}

# TTL for cleaning up network after tests (defaults to 8 hours)
HERMETIC_TTL ?= 3600

# Environment to run durable tests against
DURABLE_ENV ?= dev

# Branch from which to use the durable test workflow
DURABLE_TEST_BRANCH ?= main

# Test selector
TEST_SELECTOR ?= correctness

PNPM = pnpm

.PHONY: all
all: check-fmt check-clippy test

.PHONY: build
build: driver suite

.PHONY: test
test:
	${CARGO} test --locked --all-features -- --nocapture

.PHONY: driver
driver:
	${CARGO_BUILD} -p hermetic-driver

.PHONY: suite
suite:
	cd suite && ${PNPM} install && ${PNPM} run build

.PHONY: check-fmt
check-fmt:
	${CARGO} fmt --all -- --check

.PHONY: check-clippy
check-clippy:
	# Check with default features
	${CARGO} clippy --workspace
	# Check with all features
	${CARGO} clippy --workspace --all-features

.PHONY: check-suite
check-suite:
	./ci-scripts/check_deps.sh

.PHONY: build-suite
build-suite:
	BUILD_PROFILE=${BUILD_PROFILE} IMAGE_NAME=${TEST_SUITE_IMAGE_NAME} NO_PUSH=true ./ci-scripts/publish_suite.sh ${BUILD_TAG}

.PHONY: publish-suite
publish-suite:
	BUILD_PROFILE=${BUILD_PROFILE} IMAGE_NAME=${TEST_SUITE_IMAGE_NAME} ./ci-scripts/publish_suite.sh ${BUILD_TAG}

.PHONY: publish-hermetic-driver
publish-hermetic-driver:
	IMAGE_NAME=${TEST_SUITE_IMAGE_NAME} ./ci-scripts/publish_hermetic_driver.sh ${BUILD_TAG}

# TODO Remove this target when the flavors are refactored away
.PHONY: publish-tests-property
publish-tests-property:
	BUILD_PROFILE=${BUILD_PROFILE} ./ci-scripts/publish_property.sh ${BUILD_TAG}

.PHONY: hermetic-tests
hermetic-tests:
	${HERMETIC_CMD} test \
		--network "${TEST_NETWORK}" \
		--flavor correctness \
		--suffix "${HERMETIC_SUFFIX}" \
		--network-ttl ${HERMETIC_TTL} \
		--test-image "${TEST_SUITE_IMAGE}" \
		--test-selector "${TEST_SELECTOR}"

.PHONY: migration-tests
migration-tests:
	${HERMETIC_CMD} test \
		--network "${TEST_PRE_MIGRATION_NETWORK}" \
		--flavor migration \
		--suffix "${HERMETIC_SUFFIX}" \
		--network-ttl ${HERMETIC_TTL} \
		--test-image "${TEST_SUITE_IMAGE}" \
		--test-selector "migration" \
		--migration-wait-secs 400 \
		--migration-network "${TEST_POST_MIGRATION_NETWORK}"

.PHONY: performance-tests
performance-tests:
	${HERMETIC_CMD} test \
		--network "${TEST_NETWORK}" \
		--flavor perf \
		--suffix "${HERMETIC_SUFFIX}" \
		--network-ttl ${HERMETIC_TTL} \
		--simulation ${TEST_SIMULATION}

.PHONY: durable-tests
durable-tests:
	BUILD_TAG="${BUILD_TAG}" IMAGE_NAME="${TEST_SUITE_IMAGE_NAME}" ./durable/durable-driver.sh "${DURABLE_ENV}" "${TEST_SELECTOR}"

.PHONY: schedule-durable-tests
schedule-durable-tests:
	BUILD_TAG="${BUILD_TAG}" TEST_BRANCH="${DURABLE_TEST_BRANCH}" ./ci-scripts/schedule_durable_tests.sh "${DURABLE_ENV}" "${TEST_SELECTOR}"
