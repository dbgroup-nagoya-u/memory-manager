# Simple Garbage Collectors

![Unit Tests](https://github.com/dbgroup-nagoya-u/simple-garbage-collectors/workflows/Unit%20Tests/badge.svg?branch=main)

## Build

Note: this library is a header only. You can use this as just a submodule.

### Prerequisites

```bash
sudo apt update && sudo apt install -y build-essential cmake
```

### Build Options

- `SIMPLE_GC_BUFFER_SIZE`: the size of epoch/garbage ring buffers (default: `4096`).
- `SIMPLE_GC_PARTITION_NUM`: the number of partitions of epoch/garbage ring buffers (default: `8`). This setting affects performance in multi-threads environment.
- `SIMPLE_GC_INITIAL_GARBAGE_LIST_CAPACITY`: the default capacity of garbage lists (default: `256`).
- `SIMPLE_GC_BUILD_TESTS`: build unit tests for GC if `on`.

### Build and Run Unit Tests

```bash
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release -DSIMPLE_GC_BUILD_TESTS=on ..
make -j
ctest -C Release
```
