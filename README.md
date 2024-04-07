# Memory Manager

[![Ubuntu 22.04](https://github.com/dbgroup-nagoya-u/memory-manager/actions/workflows/ubuntu_22.yaml/badge.svg)](https://github.com/dbgroup-nagoya-u/memory-manager/actions/workflows/ubuntu_22.yaml) [![Ubuntu 20.04](https://github.com/dbgroup-nagoya-u/memory-manager/actions/workflows/ubuntu_20.yaml/badge.svg)](https://github.com/dbgroup-nagoya-u/memory-manager/actions/workflows/ubuntu_20.yaml) [![macOS](https://github.com/dbgroup-nagoya-u/memory-manager/actions/workflows/mac.yaml/badge.svg)](https://github.com/dbgroup-nagoya-u/memory-manager/actions/workflows/mac.yaml)

This repository is an open source implementation of epoch-based garbage collection for reseach use.

- [Build](#build)
    - [Prerequisites](#prerequisites)
    - [Build Options](#build-options)
    - [Build and Run Unit Tests](#build-and-run-unit-tests)
- [Usage](#usage)
    - [Linking by CMake](#linking-by-cmake)
    - [Collect and Release Garbage Pages](#collect-and-release-garbage-pages)
    - [Destruct Garbage before Releasing](#destruct-garbage-before-releasing)
    - [Reuse Garbage-Collected Pages](#reuse-garbage-collected-pages)
    - [Release Aligned Pages](#release-aligned-pages)
- [Acknowledgments](#acknowledgments)

## Build

### Prerequisites

```bash
sudo apt update && sudo apt install -y build-essential cmake
```

### Build Options

#### Tuning Parameters

- `DBGROUP_MAX_THREAD_NUM`: the maximum number of worker threads (please refer to [cpp-utility](https://github.com/dbgroup-nagoya-u/cpp-utility)).

#### Parameters for Unit Testing

- `MEMORY_MANAGER_BUILD_TESTS`: build unit tests for this repository if `ON` (default `OFF`).
- `DBGROUP_TEST_THREAD_NUM`: the number of threads to run unit tests (default `2`).

### Build and Run Unit Tests

```bash
mkdir build && cd build
cmake .. \
  -DCMAKE_BUILD_TYPE=Release \
  -DMEMORY_MANAGER_BUILD_TESTS=ON
cmake --build . --parallel --config Release
ctest -C Release
```

## Usage

### Linking by CMake

Add this library to your build in `CMakeLists.txt`.

```cmake
FetchContent_Declare(
  memory-manager
  GIT_REPOSITORY "https://github.com/dbgroup-nagoya-u/memory-manager.git"
  GIT_TAG "<commit_tag_you_want_to_use>"
)
FetchContent_MakeAvailable(memory-manager)

add_executable(
  <target_bin_name>
  [<source> ...]
)
target_link_libraries(<target_bin_name> PRIVATE
  dbgroup::memory_manager
)
```

### Collect and Release Garbage Pages

If you want only to release garbage pointers, you can use our garbage collector as follows.

```cpp
// C++ standard libraries
#include <chrono>
#include <cstddef>
#include <thread>
#include <vector>

// our libraries
#include "memory/epoch_based_gc.hpp"

auto
main(  //
    [[maybe_unused]] const int argc,
    [[maybe_unused]] const char *argv[])  //
    -> int
{
  // create and run a garbage collector
  ::dbgroup::memory::EpochBasedGC gc{};
  gc.StartGC();

  // prepare a sample worker procedure
  auto worker = [&]() {
    for (size_t loop = 0; loop < 100; ++loop) {
      // this thread has not enter a current epoch yet
      {
        // we use the scoped pattern to prevent garbage from releasing.
        const auto &guard = gc.CreateEpochGuard();

        // you can access this page safely until all the threads leave the current epoch
        auto *page = ::dbgroup::memory::Allocate<size_t>();
        auto *garbage = new (page) size_t{loop};
        gc.AddGarbage(garbage);
      }
      // this thread has left the epoch

      std::this_thread::sleep_for(std::chrono::milliseconds{1});  // dummy sleep
    }
  };

  // create threads to add garbage
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

### Destruct Garbage before Releasing

You can call a specific destructor before releasing garbage.

```cpp
// C++ standard libraries
#include <chrono>
#include <cstddef>
#include <iostream>
#include <memory>
#include <thread>
#include <vector>

// our libraries
#include "memory/epoch_based_gc.hpp"

// prepare the information of target garbage
struct SharedPtrTarget : public ::dbgroup::memory::DefaultTarget {
  // set the type of garbage to perform destructor
  using T = std::shared_ptr<size_t>;
};

auto
main(  //
    [[maybe_unused]] const int argc,
    [[maybe_unused]] const char *argv[])  //
    -> int
{
  // prepare weak_ptr for checking garbage' life-time
  std::vector<std::weak_ptr<size_t>> weak_pointers{};
  std::mutex lock{};

  {
    // create a garbage collector with a specific target
    ::dbgroup::memory::EpochBasedGC<SharedPtrTarget> gc{};
    gc.StartGC();

    // prepare a sample worker procedure
    auto worker = [&]() {
      for (size_t loop = 0; loop < 100; ++loop) {
        {
          const auto &guard = gc.CreateEpochGuard();

          // create a shared pointer as gabage pages
          auto *page = ::dbgroup::memory::Allocate<std::shared_ptr<size_t>>();
          auto *garbage = new (page) std::shared_ptr<size_t>{new size_t{loop}};
          {
            // track the life-time of this garbage
            const auto &lock_guard = std::lock_guard{lock};
            weak_pointers.emplace_back(*garbage);
          }

          // specify the type of target garbage
          gc.AddGarbage<SharedPtrTarget>(garbage);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
      }
    };

    std::vector<std::thread> threads{};
    for (size_t i = 0; i < 8; ++i) {
      threads.emplace_back(worker);
    }
    for (auto &&t : threads) {
      t.join();
    }

    // our GC has released all the garbage pages before its destruction
  }

  // check all the garbage has been destructed and released
  for (const auto &weak_p : weak_pointers) {
    if (!weak_p.expired()) {
      throw std::runtime_error{"Failed: there is the unreleased garbage."};
    }
  }
  std::cout << "Succeeded: all the garbage has been released.\n";

  return 0;
}
```

### Reuse Garbage-Collected Pages

You can reuse garbage-collected pages. Our GC maintains garbage lists in each thread's local storage, so reusing pages can avoid contention due to memory allocation.

```cpp
// C++ standard libraries
#include <atomic>
#include <chrono>
#include <cstddef>
#include <iostream>
#include <thread>
#include <vector>

// our libraries
#include "memory/epoch_based_gc.hpp"

// prepare the information of target garbage
struct ReusableTarget : public ::dbgroup::memory::DefaultTarget {
  // reuse garbage-collected pages
  static constexpr bool kReusePages = true;
};

auto
main(  //
    [[maybe_unused]] const int argc,
    [[maybe_unused]] const char *argv[])  //
    -> int
{
  std::atomic_size_t count{0};

  // create a garbage collector with a reusable target
  ::dbgroup::memory::EpochBasedGC<ReusableTarget> gc{};
  gc.StartGC();

  // prepare a sample worker procedure
  auto worker = [&]() {
    for (size_t loop = 0; loop < 100; ++loop) {
      {
        const auto &guard = gc.CreateEpochGuard();

        // get a page if exist
        auto *page = gc.GetPageIfPossible<ReusableTarget>();
        if (page != nullptr) {
          ++count;
        } else {
          page = ::dbgroup::memory::Allocate<size_t>();
        }

        auto *garbage = new (page) size_t{loop};
        gc.AddGarbage<ReusableTarget>(garbage);
      }
      std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
  };

  std::vector<std::thread> threads{};
  for (size_t i = 0; i < 8; ++i) {
    threads.emplace_back(worker);
  }
  for (auto &&t : threads) {
    t.join();
  }

  std::cout << count << " pages are reused.\n";

  return 0;
}
```

### Release Aligned Pages

If a target class has a specific alignment, you must also specify this in your target garbage class.

```cpp
// release pointers with alignment for CPU cache lines
struct alignas(64) AlignedTarget : public ::dbgroup::memory::DefaultTarget {
  // you can set the other parameters here
};
```

## Acknowledgments

This work is based on results from project JPNP16007 commissioned by the New Energy and Industrial Technology Development Organization (NEDO), and it was supported partially by KAKENHI (JP16H01722, JP20K19804, JP21H03555, and JP22H03594).
