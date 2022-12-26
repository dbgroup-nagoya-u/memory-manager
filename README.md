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
    - [Destruct Garbage before Releasing](#destruct-garbage-before-releasing)
    - [Reuse Garbage-Collected Pages](#reuse-garbage-collected-pages)
    - [Perform GC on Persistent Memory](#perform-gc-on-persistent-memory)


## Build

**Note**: this is a header only library. You can use this without pre-build.

### Prerequisites

```bash
sudo apt update && sudo apt install -y build-essential cmake
```

If you use this library for pages on persistent memory, install [libpmemobj++ (>= ver. 1.12)](https://pmem.io/pmdk/). You can install related packages by using a package manager on Ubuntu 22.04 LTS.

```bash
sudo apt update && sudo apt install -y libpmemobj-cpp-dev
```

### Build Options

#### Tuning Parameters

- `MEMORY_MANAGER_GARBAGE_BUFFER_SIZE`: the size of buffers for retaining garbage (default `1024`).
- `MEMORY_MANAGER_EXPECTED_THREAD_NUM`: the expected number of worker threads (default: `128`).
- `MEMORY_MANAGER_USE_PERSISTENT_MEMORY`: perform garbage collection for pages on persistent memory (default: `OFF`).

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
      dbgroup::memory_manager
    )
    ```

### Collect and Release Garbage Pages

If you wish to only release garbage, you can use our garbage collector as follows.

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
  constexpr size_t kGCInterval = 1E3;  // increment an epoch value every 1ms
  constexpr size_t kThreadNum = 1;     // use one thread to release garbage

  // create and run a garbage collector
  ::dbgroup::memory::EpochBasedGC gc{kGCInterval, kThreadNum};
  gc.StartGC();

  // prepare a sample worker procedure
  auto worker = [&]() {
    for (size_t loop = 0; loop < 100; ++loop) {
      // this thread has not enter a current epoch yet
      {
        // we use the scoped pattern to prevent garbage from releasing.
        const auto &guard = gc.CreateEpochGuard();

        // you can access this page safely until all the threads leave the current epoch
        auto *page = new size_t{loop};
        gc.AddGarbage(page);
      }
      // this thread has left the epoch

      std::this_thread::sleep_for(std::chrono::microseconds{100});  // dummy sleep
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
#include <iostream>
#include <memory>
#include <thread>
#include <tuple>
#include <vector>

// our libraries
#include "memory/epoch_based_gc.hpp"

// prepare the information of target garbage
struct SharedPtrTarget {
  // set the type of garbage to perform destructor
  using T = std::shared_ptr<size_t>;

  // do not reuse pages in this example
  static constexpr bool kReusePages = false;

  // use the standard delete function to release garbage
  static const inline std::function<void(void *)> deleter = [](void *ptr) {
    ::operator delete(ptr);
  };
};

auto
main(  //
    const int argc,
    const char *argv[])  //
    -> int
{
  constexpr size_t kGCInterval = 1E3;
  constexpr size_t kThreadNum = 1;

  // prepare weak_ptr for checking garbage' life-time
  std::vector<std::weak_ptr<size_t>> weak_pointers{};
  std::mutex lock{};

  {
    // create a garbage collector with a specific target
    ::dbgroup::memory::EpochBasedGC<SharedPtrTarget> gc{kGCInterval, kThreadNum};
    gc.StartGC();

    // prepare a sample worker procedure
    auto worker = [&]() {
      for (size_t loop = 0; loop < 100; ++loop) {
        {
          const auto &guard = gc.CreateEpochGuard();

          // create a shared pointer as gabage pages
          auto *page = new std::shared_ptr<size_t>{new size_t{loop}};
          {
            // track the life-time of this garbage
            const auto &lock_guard = std::lock_guard{lock};
            weak_pointers.emplace_back(*page);
          }

          // specify the type of target garbage
          gc.AddGarbage<SharedPtrTarget>(page);
        }

        std::this_thread::sleep_for(std::chrono::microseconds{100});
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
      std::cout << "Failed: there is the unreleased garbage." << std::endl;
      exit(EXIT_FAILURE);
    }
  }
  std::cout << "Succeeded: all the garbage has been released." << std::endl;

  return 0;
}
```

### Reuse Garbage-Collected Pages

You can reuse garbage-collected pages. Our GC maintains garbage lists in thread local storage of each thread, so reusing pages can avoid the contention due to memory allocation.

```cpp
// C++ standard libraries
#include <chrono>
#include <iostream>
#include <mutex>
#include <thread>
#include <tuple>
#include <vector>

// our libraries
#include "memory/epoch_based_gc.hpp"

// prepare the information of target garbage
struct ReusableTarget {
  // do not call destructor
  using T = void;

  // reuse garbage-collected pages
  static constexpr bool kReusePages = true;

  // use the standard delete function to release garbage
  static const inline std::function<void(void *)> deleter = [](void *ptr) {
    ::operator delete(ptr);
  };
};

auto
main(  //
    const int argc,
    const char *argv[])  //
    -> int
{
  constexpr size_t kGCInterval = 1E3;
  constexpr size_t kThreadNum = 1;
  std::mutex lock{};

  // create a garbage collector with a reusable target
  ::dbgroup::memory::EpochBasedGC<ReusableTarget> gc{kGCInterval, kThreadNum};
  gc.StartGC();

  // prepare a sample worker procedure
  auto worker = [&]() {
    for (size_t loop = 0; loop < 100; ++loop) {
      {
        const auto &guard = gc.CreateEpochGuard();

        // get a page if exist
        auto *page = gc.GetPageIfPossible<ReusableTarget>();
        if (page != nullptr) {
          const auto &lock_guard = std::lock_guard{lock};
          std::cout << "Page Reused." << std::endl;
        } else {
          page = new size_t{};
        }

        auto *garbage = new (page) size_t{loop};
        gc.AddGarbage<ReusableTarget>(page);
      }

      std::this_thread::sleep_for(std::chrono::microseconds{100});  // dummy sleep
    }
  };

  std::vector<std::thread> threads{};
  for (size_t i = 0; i < 8; ++i) {
    threads.emplace_back(worker);
  }
  for (auto &&t : threads) {
    t.join();
  }

  return 0;
}
```

### Perform GC on Persistent Memory

You can use our GC with peristent memory. Although the usage is roughly the same as for volatile memory, note that some APIs are slightly different.

```cpp
// C++ standard libraries
#include <chrono>
#include <iostream>
#include <mutex>
#include <thread>
#include <tuple>
#include <vector>

// our libraries
#include "memory/epoch_based_gc.hpp"

// prepare the information of target garbage
struct PMEMTarget {
  // do not call destructor
  using T = void;

  // target pages are on persistent memory
  static constexpr bool kOnPMEM = true;

  // reuse garbage-collected pages
  static constexpr bool kReusePages = true;

  // cannot specify a deleter function
  // static const inline std::function<void(void *)> deleter = [](void *ptr) {
  //   ::operator delete(ptr);
  // };
};

// a root region of your pool must have a head of garbage lists for recovery
struct PMEMRoot {
  using GarbageNode_t = ::dbgroup::memory::GarbageNodeOnPMEM<PMEMTarget>;

  ::pmem::obj::persistent_ptr<GarbageNode_t> head{nullptr};
};

auto
main(  //
    const int argc,
    const char *argv[])  //
    -> int
{
  constexpr size_t kGCInterval = 1E3;
  constexpr size_t kThreadNum = 1;
  std::mutex lock{};

  // prepare a pool on persistent memory before creating a GC instance
  constexpr size_t kSize = PMEMOBJ_MIN_POOL * 32;
  constexpr int kModeRW = S_IWUSR | S_IRUSR;
  auto &&pool = ::pmem::obj::pool<PMEMRoot>::create("/pmem_tmp/test", "test", kSize, kModeRW);

  // create a garbage collector
  ::dbgroup::memory::EpochBasedGC<PMEMTarget> gc{kGCInterval, kThreadNum};
  gc.SetHeadAddrOnPMEM<PMEMTarget>(&(pool.root()->head));  // set the corresponding head
  gc.StartGC();

  // prepare a sample worker procedure
  auto worker = [&]() {
    for (size_t loop = 0; loop < 100; ++loop) {
      {
        const auto &guard = gc.CreateEpochGuard();

        // get a page if exist
        ::pmem::obj::persistent_ptr<size_t> garbage{nullptr};

        // this function get a reusable page atomically, so `garbage` variable should
        // be allocated on persistent memory in practical to prevent memory leak.
        gc.GetPageIfPossible<PMEMTarget>(garbage.raw_ptr(), pool);
        if (garbage == nullptr) {
          // allocate a page dynamically
          try {
            ::pmem::obj::flat_transaction::run(
                pool, [&] { garbage = ::pmem::obj::make_persistent<size_t>(loop); });
          } catch (const std::exception &e) {
            std::cerr << e.what() << std::endl;
            std::terminate();
          }
        } else {
          // reuse the allocated page
          new (garbage.get()) size_t{loop};
          const auto &lock_guard = std::lock_guard{lock};
          std::cout << "Page Reused." << std::endl;
        }

        // add garbage on persistent memory
        gc.AddGarbage<PMEMTarget>(garbage.raw_ptr(), pool);
      }

      std::this_thread::sleep_for(std::chrono::microseconds{100});  // dummy sleep
    }
  };

  std::vector<std::thread> threads{};
  for (size_t i = 0; i < 8; ++i) {
    threads.emplace_back(worker);
  }
  for (auto &&t : threads) {
    t.join();
  }

  return 0;
}
```
