# Performance tests

## Running

Each subdirectory contains a network.yaml and simulation.yaml defining a keramik test that is designed to be run through the Github Action [run-performance](./.github/workflows/run-performance.yaml).

The test name is the directory name.

To run a test, go to the ["Run Performance" Github Action](https://github.com/3box/ceramic-tests/actions/workflows/run-performance.yaml) and use the directory name for the "Folder name containing the performance test yaml" input.

## Adding a new test

To add a new test, create a new directory with a network.yaml and simulation.yaml.

Ensure that the following setting are used to capture metrics.

```
spec:
  monitoring:
    namespaced: true
    podMonitor:
      enabled: true
```
