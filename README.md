# Memory Manager

![Ubuntu-20.04](https://github.com/dbgroup-nagoya-u/memory-manager/workflows/Ubuntu-20.04/badge.svg?branch=main)

## Build

**Note**: this is a header only library. You can use this without pre-build.

### Prerequisites

```bash
sudo apt update && sudo apt install -y build-essential cmake
```

### Build Options for Tuning

- `MEMORY_MANAGER_GARBAGE_BUFFER_SIZE`: the size of an initially created buffer for garbage instances (default `1024`).

### Build Options for Unit Testing

- `MEMORY_MANAGER_BUILD_TESTS`: build unit tests for this repository if `ON` (default `OFF`).
- `MEMORY_MANAGER_TEST_THREAD_NUM`: the number of threads to run unit tests (default `8`).

### Build and Run Unit Tests

```bash
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release -DMEMORY_MANAGER_BUILD_TESTS=ON ..
make -j
ctest -C Release
```
