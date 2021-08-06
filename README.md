# Memory Manager

![Ubuntu-20.04](https://github.com/dbgroup-nagoya-u/memory-manager/workflows/Ubuntu-20.04/badge.svg?branch=main)
![mimalloc](https://github.com/dbgroup-nagoya-u/memory-manager/workflows/mimalloc/badge.svg?branch=main)
![jemalloc](https://github.com/dbgroup-nagoya-u/memory-manager/workflows/jemalloc/badge.svg?branch=main)

## Build

**Note**: this is a header only library. You can use this without pre-build.

### Prerequisites

```bash
sudo apt update && sudo apt install -y build-essential cmake
```

### Build Options

- `MEMORY_MANAGER_GARBAGE_BUFFER_SIZE`: the size of an initially created buffer for garbage instances (default `1024`).

#### Using Efficient Allocators

We prepare the following options to use efficient memory allocators internally. Note that you do not need to use these options if your application overrides functions/operators for memory allocation entirely.

- `MEMORY_MANAGER_USE_MIMALLOC`: use [mimalloc](https://github.com/microsoft/mimalloc) as a memory allocator/deleter if `ON` (default `OFF`).
    - If you use this option, you need to install mimalloc beforehand and enable `cmake` find it by using the [find_package](https://cmake.org/cmake/help/latest/command/find_package.html) command.
- `MEMORY_MANAGER_USE_JEMALLOC`: use [jemalloc](https://github.com/jemalloc/jemalloc) as a memory allocator/deleter if `ON` (default `OFF`).
    - If you use this option, you need to build/install jemalloc beforehand with the following configuration.

    ```bash
    ./configure --prefix=/usr/local --with-version=VERSION --with-jemalloc-prefix=je_ --with-install-suffix=_without_override --disable-cxx
    ```

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
