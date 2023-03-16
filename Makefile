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
	cargo run --all-features --locked --bin ceramic-one -- daemon -b 127.0.0.1:5001

# Prepare a release PR.
.PHONY: release-pr
release-pr:
	./scripts/release_pr.sh

# Publish any unpublished releases.
# A release PR must first be merged before this target
# will have any effect.
.PHONY: release
release:
	./scripts/release.sh

