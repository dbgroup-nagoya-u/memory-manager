# Memory Manager

![Unit Tests](https://github.com/dbgroup-nagoya-u/memory-manager/workflows/Unit%20Tests/badge.svg?branch=main)

## Build

**Note**: this is a header only library. You can use this without pre-build.

### Prerequisites

```bash
sudo apt update && sudo apt install -y build-essential cmake
```

### Build Options

- `MEMORY_MANAGER_GARBAGE_BUFFER_SIZE`: the size of an initially created buffer for garbage instances (default `1024`).
- `MEMORY_MANAGER_USE_MIMALLOC`: use [mimalloc](https://github.com/microsoft/mimalloc) as a memory allocator/deleter if `on` (default `off`).
    - If you use this option, you need to install mimalloc beforehand and enable `cmake` find it by using the [find_package](https://cmake.org/cmake/help/latest/command/find_package.html) command.

### Build Options for Unit Testing

- `MEMORY_MANAGER_BUILD_TESTS`: build unit tests for this repository if `on` (default `off`).
- `MEMORY_MANAGER_TEST_THREAD_NUM`: the number of threads to run unit tests (default `8`).

### Build and Run Unit Tests

```bash
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release -DMEMORY_MANAGER_BUILD_TESTS=on ..
make -j
ctest -C Release
```
