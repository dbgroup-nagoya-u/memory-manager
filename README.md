# Memory Manager

[![Ubuntu-20.04](https://github.com/dbgroup-nagoya-u/memory-manager/actions/workflows/unit_tests.yaml/badge.svg)](https://github.com/dbgroup-nagoya-u/memory-manager/actions/workflows/unit_tests.yaml)

This repository is an open source implementation of epoch-based garbage collection for reseach use.

- [Build](#build)
    - [Prerequisites](#prerequisites)
    - [Build Options](#build-options)
        - [Tuning Parameters](#tuning-parameters)
        - [Parameters for Unit Testing](#parameters-for-unit-testing)
    - [Build and Run Unit Tests](#build-and-run-unit-tests)
- [Usage](#usage)
    - [Linking by CMake](#linking-by-cmake)
    - [Collect and Release Garbage Pages](#collect-and-release-garbage-pages)
    - [Destruct and Release](#destruct-and-release)
    - [Reuse Garbage Collected Pages](#reuse-garbage-collected-pages)


## Build

**Note**: this is a header only library. You can use this without pre-build.

### Prerequisites

```bash
sudo apt update && sudo apt install -y build-essential cmake
```

### Build Options

#### Tuning Parameters

- `MEMORY_MANAGER_GARBAGE_BUFFER_SIZE`: the size of buffers for retaining garbages (default `1024`).
- `MEMORY_MANAGER_EXPECTED_THREAD_NUM`: the expected number of worker threads (default: `128`).

#### Parameters for Unit Testing

- `MEMORY_MANAGER_BUILD_TESTS`: build unit tests for this repository if `ON` (default `OFF`).
- `DBGROUP_TEST_THREAD_NUM`: the number of threads to run unit tests (default `8`).

### Build and Run Unit Tests

```bash
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release -DMEMORY_MANAGER_BUILD_TESTS=ON ..
make -j
ctest -C Release
```

## Usage

### Linking by CMake

1. Download the files in any way you prefer (e.g., `git submodule`).

    ```bash
    cd <your_project_workspace>
    mkdir external
    git submodule add https://github.com/dbgroup-nagoya-u/memory-manager.git external/memory-manager
    ```

1. Add this library to your build in `CMakeLists.txt`.

    ```cmake
    add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/external/memory-manager")

    add_executable(
      <target_bin_name>
      [<source> ...]
    )
    target_link_libraries(<target_bin_name> PRIVATE
      dbgroup::memory-manager
    )
    ```

### Collect and Release Garbage Pages

If you wish to only release garbages, you can use our garbage collector as follows.

```cpp
// C++ standard libraries
#include <chrono>
#include <thread>
#include <vector>

// our libraries
#include "memory/epoch_based_gc.hpp"

auto
main(  //
    const int argc,
    const char *argv[])  //
    -> int
{
  constexpr size_t kGCInterval = 1E3;  // increment a epoch value every 1ms
  constexpr size_t kThreadNum = 1;     // use one thread to release garbages

  // create and run a garbage collector
  ::dbgroup::memory::EpochBasedGC gc{kGCInterval, kThreadNum};
  gc.StartGC();

  // prepare a sample worker procedure
  auto worker = [&]() {
    for (size_t loop = 0; loop < 100; ++loop) {
      // this thread has not enter a current epoch yet
      {
        // we use the scoped pattern to prevent garbages from releasing.
        const auto &guard = gc.CreateEpochGuard();

        // you can access this page safely until all the threads leave the current epoch
        auto *page = new size_t{loop};
        gc.AddGarbage(page);
      }
      // this thread has left the epoch

      std::this_thread::sleep_for(std::chrono::microseconds{100});  // dummy sleep
    }
  };

  // create threads to add garbages
  std::vector<std::thread> threads{};
  for (size_t i = 0; i < 8; ++i) {
    threads.emplace_back(worker);
  }

  // wait for the threads
  for (auto &&t : threads) {
    t.join();
  }

  // our GC has released all the garbage pages before its destruction
  return 0;
}
```

### Destruct and Release

### Reuse Garbage Collected Pages
