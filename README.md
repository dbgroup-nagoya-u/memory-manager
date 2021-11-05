# Memory Manager

[![Ubuntu-20.04](https://github.com/dbgroup-nagoya-u/memory-manager/actions/workflows/unit_tests.yaml/badge.svg)](https://github.com/dbgroup-nagoya-u/memory-manager/actions/workflows/unit_tests.yaml)

## Build

**Note**: this is a header only library. You can use this without pre-build.

### Prerequisites

```bash
sudo apt update && sudo apt install -y build-essential cmake
```

### Build Options

#### Tuning Parameters

- `MEMORY_MANAGER_GARBAGE_BUFFER_SIZE`: the size of an initially created buffer for garbage instances (default `1024`).

#### Parameters for Unit Testing

- `MEMORY_MANAGER_BUILD_TESTS`: build unit tests for this repository if `ON` (default `OFF`).
- `MEMORY_MANAGER_TEST_THREAD_NUM`: the number of threads to run unit tests (default `8`).

### Build and Run Unit Tests

```bash
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release -DMEMORY_MANAGER_BUILD_TESTS=ON ..
make -j
ctest -C Release
```
