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
    - [Release Aligned Pages](#release-aligned-pages)
    - [Perform GC on Persistent Memory](#perform-gc-on-persistent-memory)
- [Acknowledgments](#acknowledgments)


## Build

**Note**: this is a header-only library. You can use this without pre-build.

### Prerequisites

```bash
sudo apt update && sudo apt install -y build-essential cmake
```

If you use this library for pages on persistent memory, install [libpmemobj](https://pmem.io/pmdk/).

```bash
sudo apt update && sudo apt install -y libpmemobj-dev
```

### Build Options

#### Tuning Parameters

- `MEMORY_MANAGER_USE_PERSISTENT_MEMORY`: perform garbage collection for pages on persistent memory (default: `OFF`).
- `DBGROUP_MAX_THREAD_NUM`: the maximum number of worker threads (please refer to [cpp-utility](https://github.com/dbgroup-nagoya-u/cpp-utility)).

#### Parameters for Unit Testing

- `MEMORY_MANAGER_BUILD_TESTS`: build unit tests for this repository if `ON` (default `OFF`).
- `DBGROUP_TEST_THREAD_NUM`: the number of threads to run unit tests (default `8`).
- `DBGROUP_TEST_TMP_PMEM_PATH`: the path to a durable storage (default: `/tmp`). We expect this option to be used with persistent memory.

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
struct SharedPtrTarget : public DefaultTarget {
  // set the type of garbage to perform destructor
  using T = std::shared_ptr<size_t>;
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
struct ReusableTarget : public DefaultTarget {
  // reuse garbage-collected pages
  static constexpr bool kReusePages = true;
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

### Release Aligned Pages

If a target class has a specific alignment, you must also specify this in your target garbage class.

```cpp
// release pointers with alignment for CPU cache lines
struct alignas(64) AlignedTarget : public DefaultTarget {
  // you can set the other parameters here
};
```

### Perform GC on Persistent Memory

You can use our GC with peristent memory. Although the usage is roughly the same as for volatile memory, note that some APIs are slightly different.

Note that you can call a garbage destructor only in normal GC processes. In a recovery process after failures, our GC releases garbage without destruction.

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
  // target pages are on persistent memory
  static constexpr bool kOnPMEM = true;

  // reuse garbage-collected pages
  static constexpr bool kReusePages = true;
};

using GC_t = ::dbgroup::memory::EpochBasedGC<PMEMTarget>;

auto
main(  //
    const int argc,
    const char *argv[])  //
    -> int
{
  constexpr size_t kGCInterval = 1E3;
  constexpr size_t kThreadNum = 1;
  constexpr size_t kPMDKTypeID = 0;
  std::mutex lock{};

  // prepare a pool on persistent memory for your objects
  constexpr size_t kSizeForPool = PMEMOBJ_MIN_POOL * 32;  // 256MiB
  constexpr int kModeRW = S_IWUSR | S_IRUSR;
  auto *pop = pmemobj_create("/pmem_tmp/test", "test", kSizeForPool, kModeRW);

  // create a garbage collector
  const auto *path_to_gc = "/pmem_tmp/gc";
  constexpr size_t kGCSize = PMEMOBJ_MIN_POOL;  // 8 MiB can manage about 0.5M garbage instances
  GC_t gc{path_to_gc, kGCSize, kGCInterval, kThreadNum};
  gc.StartGC();

  // prepare a sample worker procedure
  auto worker = [&]() {
    for (size_t loop = 0; loop < 100; ++loop) {
      {
        const auto &guard = gc.CreateEpochGuard();

        /* You can use temporary fields (up to thirteen) for recovery purposes. Because
         * the temporary fields are retained after power failures, you can use them by
         * calling `GetUnreleasedFields` to perform your recovery procedures. */
        auto *tmp_oid = gc.GetTmpField<PMEMTarget>(0);

        // check pages can be reused
        gc.GetPageIfPossible<PMEMTarget>(tmp_oid);
        if (!OID_IS_NULL(*tmp_oid)) {
          // reused
          const auto &lock_guard = std::lock_guard{lock};
          std::cout << "Page Reused." << std::endl;
        } else {
          // allocate a page dynamically
          if (pmemobj_zalloc(pop, tmp_oid, sizeof(size_t), kPMDKTypeID) != 0) {
            std::cerr << pmemobj_errormsg() << std::endl;
            break;
          }
        }

        // initialize garbage and add it to GC
        new (pmemobj_direct(*tmp_oid)) size_t{loop};
        gc.AddGarbage<PMEMTarget>(tmp_oid);
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

  // check there is no allocated pages in the pool
  gc.StopGC();
  if (OID_IS_NULL(pmemobj_first(pop))) {
    std::cout << "All the garbage is released by GC." << std::endl;
  }

  pmemobj_close(pop);
  return 0;
}
```

## Acknowledgments

This work is based on results from project JPNP16007 commissioned by the New Energy and Industrial Technology Development Organization (NEDO), and it was supported partially by KAKENHI (JP16H01722, JP20K19804, JP21H03555, and JP22H03594).
