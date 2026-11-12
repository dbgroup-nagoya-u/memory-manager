# Memory Manager

[![Ubuntu 24.04](https://github.com/dbgroup-nagoya-u/memory-manager/actions/workflows/ubuntu_24.yaml/badge.svg?branch=main)](https://github.com/dbgroup-nagoya-u/memory-manager/actions/workflows/ubuntu_24.yaml?query=branch%3Amain) [![Ubuntu 22.04](https://github.com/dbgroup-nagoya-u/memory-manager/actions/workflows/ubuntu_22.yaml/badge.svg?branch=main)](https://github.com/dbgroup-nagoya-u/memory-manager/actions/workflows/ubuntu_22.yaml?query=branch%3Amain) [![macOS](https://github.com/dbgroup-nagoya-u/memory-manager/actions/workflows/mac.yaml/badge.svg?branch=main)](https://github.com/dbgroup-nagoya-u/memory-manager/actions/workflows/mac.yaml?query=branch%3Amain)

This repository is an open source implementation of epoch-based garbage collection for research use.

- [Build](#build)
    - [Prerequisites](#prerequisites)
    - [Build Options](#build-options)
    - [Build and Run Unit Tests](#build-and-run-unit-tests)
- [Usage](#usage)
    - [Linking by CMake](#linking-by-cmake)
    - [Collect and Release Garbage Pages](#collect-and-release-garbage-pages)
    - [Configure Garbage Collectors](#configure-garbage-collectors)
    - [Destruct Garbage before Releasing](#destruct-garbage-before-releasing)
    - [Reuse Garbage-Collected Pages](#reuse-garbage-collected-pages)
    - [Reuse Common-Size Pages](#reuse-common-size-pages)
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
- `MEMORY_MANAGER_TEST_SANITIZER`: sanitizers for unit tests and their dependencies (default `none`).
    - `thread`: use ThreadSanitizer.
    - `address`: use AddressSanitizer and UndefinedBehaviorSanitizer.
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

GCC's ThreadSanitizer may abort with "unexpected memory mapping" under address space layout randomization. In this case, use Clang 18 or later, or run unit tests as follows.

```bash
setarch $(uname -m) -R ctest -C Release
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
#include "dbgroup/memory/epoch_based_gc.hpp"

auto
main(  //
    [[maybe_unused]] const int argc,
    [[maybe_unused]] const char *argv[])  //
    -> int
{
  // create and run a garbage collector
  ::dbgroup::memory::EpochBasedGC gc{};

  // prepare a sample worker procedure
  auto worker = [&]() {
    for (size_t loop = 0; loop < 100; ++loop) {
      // this thread has not entered the current epoch yet
      {
        // we use the scoped pattern to prevent garbage from releasing.
        const auto guard = gc.CreateEpochGuard();

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

### Configure Garbage Collectors

You can configure a garbage collector by using `Builder`, which receives the same template arguments as `EpochBasedGC`.

```cpp
auto gc = ::dbgroup::memory::Builder<>{}  //
              .SetGCInterval(10)          // run GC every 10 ms (default: 100 ms)
              .SetGCThreadNum(2)          // use two cleaner threads (default: 1)
              .SetReusablePageNum(64)     // retain 64 reusable pages for each thread (default: 32)
              .Build();                   // std::unique_ptr<EpochBasedGC<>>
```

Our GC prepares garbage lists for `::dbgroup::thread::IDManager::GetMaxThreadNum()` threads at its construction. If you change the maximum number of threads by `::dbgroup::thread::IDManager::SetMaxThreadNum()`, call it before constructing garbage collectors; otherwise, threads with larger IDs fail to add garbage with `std::out_of_range`.

### Destruct Garbage before Releasing

You can call a specific destructor before releasing garbage.

```cpp
// C++ standard libraries
#include <chrono>
#include <cstddef>
#include <iostream>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <vector>

// our libraries
#include "dbgroup/memory/epoch_based_gc.hpp"

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

    // prepare a sample worker procedure
    auto worker = [&]() {
      for (size_t loop = 0; loop < 100; ++loop) {
        {
          const auto guard = gc.CreateEpochGuard();

          // create a shared pointer as garbage pages
          auto *page = ::dbgroup::memory::Allocate<std::shared_ptr<size_t>>();
          auto *garbage = new (page) std::shared_ptr<size_t>{new size_t{loop}};
          {
            // track the life-time of this garbage
            const std::lock_guard lock_guard{lock};
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

A target class reusing pages must define `kZeroFillSize`, which is the size of each page filled with zeros before reuse. If it is zero, reused pages are not filled with zeros. Note that `DefaultTarget` defines `kZeroFillSize` as zero, so derived classes inherit it.

```cpp
// C++ standard libraries
#include <atomic>
#include <chrono>
#include <cstddef>
#include <iostream>
#include <thread>
#include <vector>

// our libraries
#include "dbgroup/memory/epoch_based_gc.hpp"

// prepare the information of target garbage
struct ReusableTarget : public ::dbgroup::memory::DefaultTarget {
  // reuse garbage-collected pages
  static constexpr bool kReusePages = true;

  // fill reused pages with zeros
  static constexpr size_t kZeroFillSize = sizeof(size_t);
};

auto
main(  //
    [[maybe_unused]] const int argc,
    [[maybe_unused]] const char *argv[])  //
    -> int
{
  std::atomic_size_t count{0};

  // create a garbage collector with a reusable target and a short GC interval
  auto gc = ::dbgroup::memory::Builder<ReusableTarget>{}.SetGCInterval(10).Build();

  // prepare a sample worker procedure
  auto worker = [&]() {
    for (size_t loop = 0; loop < 100; ++loop) {
      {
        const auto guard = gc->CreateEpochGuard();

        // get a page if exist
        auto *page = gc->GetPageIfPossible<ReusableTarget>();
        if (page) {
          ++count;
        } else {
          page = ::dbgroup::memory::Allocate<size_t>();
        }

        auto *garbage = new (page) size_t{loop};
        gc->AddGarbage<ReusableTarget>(garbage);
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

### Reuse Common-Size Pages

Our GC also manages pages of common sizes (512B, 1KiB, 2KiB, ..., and 64KiB) without declaring target classes. Specify the size of each page when you add garbage or get a reusable page. Note that reused pages are filled with zeros, and you must allocate pages with `Page512`, `Page1Ki`, ..., or `Page64Ki` to align them properly.

```cpp
::dbgroup::memory::EpochBasedGC gc{};
{
  const auto guard = gc.CreateEpochGuard();

  // reuse a 4KiB page if exist; otherwise, allocate a new one
  void *page = gc.GetPageIfPossible(4096);
  if (!page) {
    page = ::dbgroup::memory::Allocate<::dbgroup::memory::Page4Ki>();
  }

  // ... use the page ...

  // add the page as garbage with its size
  gc.AddGarbage(page, 4096);
}
```

If you give an unsupported size, `AddGarbage` throws `std::runtime_error`, and `GetPageIfPossible` returns `nullptr`.

### Release Aligned Pages

Our GC releases garbage with the alignment of its target class (i.e., `alignof(Target)`). If a garbage class has a specific alignment, you must also specify it in your target class and allocate garbage with the same alignment.

```cpp
// a garbage class aligned with CPU cache lines
struct alignas(64) AlignedClass {
  size_t data{};
};

// release pointers with alignment for CPU cache lines
struct alignas(64) AlignedTarget : public ::dbgroup::memory::DefaultTarget {
  // you can set the other parameters here
};

::dbgroup::memory::EpochBasedGC<AlignedTarget> gc{};
{
  const auto guard = gc.CreateEpochGuard();

  // `Allocate` uses the alignment of a given class
  auto *garbage = new (::dbgroup::memory::Allocate<AlignedClass>()) AlignedClass{};
  gc.AddGarbage<AlignedTarget>(garbage);
}
```

## Acknowledgments

This work is based on results from project JPNP16007 commissioned by the New Energy and Industrial Technology Development Organization (NEDO), and it was supported partially by KAKENHI (JP16H01722, JP20K19804, JP21H03555, and JP22H03594).

We used Claude, a generative AI model developed by Anthropic, to assist with development tasks such as debugging.
