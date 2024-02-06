# Makefile provides an API for CI related tasks
# Using the makefile is not required however CI
# uses the specific targets within the file.
# Therefore may be useful in ensuring a change
# is ready to pass CI checks.

RUSTFLAGS = -D warnings --cfg tokio_unstable
CARGO = RUSTFLAGS='${RUSTFLAGS}' cargo

RELEASE_LEVEL ?= minor

# ECS environment to deploy image to
DEPLOY_ENV ?= dev

# Deploy target to use for CD manager job
DEPLOY_TARGET ?= latest

# Docker image tag to deploy
DEPLOY_TAG ?= latest

# Whether or not this is a manual deployment
MANUAL_DEPLOY ?= false

# Test selector
TEST_SELECTOR ?= .

.PHONY: all
all: build check-fmt check-clippy check-deps test

.PHONY: build
build:
	# Build with default features
	$(CARGO) build --locked --release
	# Build with all features
	$(CARGO) build --locked --release --all-features

# Generates api-server crate from ceramic.yaml OpenAPI spec
.PHONY: gen-api-server
gen-api-server: api/ceramic.yaml
	./ci-scripts/gen_api_server.sh

# Checks api-server crate is up-to-date
.PHONY: check-api-server
check-api-server:
	./ci-scripts/check_generated_server.sh api-server ./ci-scripts/gen_api_server.sh

# Generates kubo-rpc-server crate from ceramic.yaml OpenAPI spec
.PHONY: gen-kubo-rpc-server
gen-kubo-rpc-server:
	./ci-scripts/gen_kubo_rpc_server.sh

# Checks kubo-rpc-server crate is up-to-date
.PHONY: check-kubo-rpc-server
check-kubo-rpc-server:
	./ci-scripts/check_generated_server.sh kubo-rpc-server ./ci-scripts/gen_kubo_rpc_server.sh

.PHONY: release
release:
	$(CARGO) build -p ceramic-one --locked --release

# Prepare a release PR.
.PHONY: release-pr
release-pr:
	./ci-scripts/release_pr.sh ${RELEASE_LEVEL}

.PHONY: debug
debug:
	$(CARGO) build -p ceramic-one --locked

.PHONY: test
test:
	# Test with default features
	$(CARGO) test --locked --release
	# Test with all features
	$(CARGO) test --locked --release --all-features

.PHONY: check-fmt
check-fmt:
	$(CARGO) fmt --all -- --check

.PHONY: check-clippy
check-clippy:
	# Check with default features
	$(CARGO) clippy --workspace --locked --release -- -D warnings --no-deps
	# Check with all features
	$(CARGO) clippy --workspace --locked --release --all-features -- -D warnings --no-deps

.PHONY: check-deps
check-deps:
	$(CARGO) machete

.PHONY: run
run:
	RUST_LOG=ERROR,ceramic_kubo_rpc=DEBUG,ceramic_one=DEBUG $(CARGO) run --all-features --locked --release --bin ceramic-one -- daemon -b 127.0.0.1:5001

.PHONY: publish-docker
publish-docker:
	./ci-scripts/publish.sh

.PHONY: schedule-ecs-deployment
schedule-ecs-deployment:
	./ci-scripts/schedule_ecs_deploy.sh "${DEPLOY_ENV}" "${DEPLOY_TARGET}" "${DEPLOY_TAG}" "${MANUAL_DEPLOY}"

.PHONY: schedule-k8s-deployment
schedule-k8s-deployment:
	./ci-scripts/schedule_k8s_deploy.sh "${DEPLOY_ENV}" "${DEPLOY_TAG}"

.PHONY: schedule-tests
schedule-tests:
	./ci-scripts/schedule_tests.sh "${DEPLOY_ENV}" "${TEST_SELECTOR}"
