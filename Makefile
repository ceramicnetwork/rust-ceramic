# Makefile provides an API for CI related tasks
# Using the makefile is not required however CI
# uses the specific targets within the file.
# Therefore may be useful in ensuring a change
# is ready to pass CI checks.

.PHONY: all
all: build check-fmt check-clippy test

.PHONY: build
build:
	# Build with default features
	cargo build --locked
	# Build with all features
	cargo build --locked --all-features

# Generates api-server crate from ceramic.yaml OpenAPI spec
.PHONY: gen-api-server
gen-api-server: api/ceramic.yaml
	./ci-scripts/gen_api_server.sh

# Checks api-server crate is up-to-date
.PHONY: check-api-server
check-api-server:
	./ci-scripts/check_api_server.sh

# Generates kubo-rpc-server crate from ceramic.yaml OpenAPI spec
.PHONY: gen-kubo-rpc-server
gen-kubo-rpc-server:
	./ci-scripts/gen_kubo_rpc_server.sh

# Checks kubo-rpc-server crate is up-to-date
.PHONY: check-kubo-rpc-server
check-kubo-rpc-server:
	./ci-scripts/check_kubo_rpc_server.sh

.PHONY: release
release:
	RUSTFLAGS="-D warnings" cargo build -p ceramic-one --locked --release

.PHONY: debug
debug:
	cargo build -p ceramic-one --locked

.PHONY: test
test:
	# Test with default features
	cargo test --locked
	# Test with all features
	cargo test --locked --all-features

.PHONY: check-fmt
check-fmt:
	cargo fmt --all -- --check

.PHONY: check-clippy
check-clippy:
	# Check with default features
	cargo clippy --workspace --all-targets -- -D warnings
	# Check with all features
	cargo clippy --workspace --all-targets --all-features -- -D warnings

.PHONY: run
run:
	RUST_LOG=ERROR,ceramic_kubo_rpc=DEBUG,ceramic_one=DEBUG cargo run --all-features --locked --bin ceramic-one -- daemon -b 127.0.0.1:5001

.PHONY: publish-docker
publish-docker:
	./ci-scripts/publish.sh

