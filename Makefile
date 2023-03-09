# Makefile provides an API for CI related tasks
# Using the makefile is not required however CI
# uses the specific targets within the file.
# Therefore may be useful in ensuring a change
# is ready to pass CI checks.

.PHONY: all
all: build check-fmt check-clippy test

.PHONY: build
build:
	cargo build --all-features --locked

.PHONY: test
test:
	cargo test --all-features --locked

.PHONY: check-fmt
check-fmt:
	cargo fmt --all -- --check

.PHONY: check-clippy
check-clippy:
	cargo clippy --workspace --all-targets -- -D warnings
