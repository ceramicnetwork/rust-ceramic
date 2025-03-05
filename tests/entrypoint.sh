#!/bin/bash

set -e

# Run all test binaries, fail on first failure.
for tb in /test-binaries/*
do
    echo "Running tests in: $tb"
    $tb
done
