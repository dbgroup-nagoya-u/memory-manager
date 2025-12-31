/*
 * Copyright 2021 Database Group, Nagoya University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// the corresponding header
#include "dbgroup/memory/epoch_based_gc.hpp"

// C++ standard libraries
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <future>
#include <memory>
#include <mutex>
#include <random>
#include <thread>
#include <utility>
#include <vector>

// external libraries
#include "gtest/gtest.h"

// library sources
#include "dbgroup/memory/utility.hpp"

namespace dbgroup::memory::test
{
/*############################################################################*
 * Global type aliases
 *############################################################################*/

using Target = uint64_t;

/*############################################################################*
 * Global classes
 *############################################################################*/

struct SharedPtrTarget : public DefaultTarget {
  using T = std::shared_ptr<Target>;
  static constexpr bool kReusePages = true;
};

class EpochBasedGCFixture : public ::testing::Test
{
 protected:
  /*##########################################################################*
   * Type aliases
   *##########################################################################*/

  using EpochBasedGC_t = EpochBasedGC<SharedPtrTarget>;
  using GCBuilder = Builder<SharedPtrTarget>;
  using GarbageRef = std::vector<std::weak_ptr<Target>>;

  /*##########################################################################*
   * Internal constants
   *##########################################################################*/

  static constexpr size_t kThreadNum = kLogicalCoreNum;
  static constexpr size_t kGCInterval = 1;
  static constexpr size_t kGarbageNumLarge = 1E6;

  /*##########################################################################*
   * Test setup/teardown
   *##########################################################################*/

  void
  SetUp() override
  {
    static_assert(EpochBasedGC_t::HasTarget<SharedPtrTarget>());
    static_assert(!EpochBasedGC_t::HasTarget<void>());
  }

  void
  TearDown() override
  {
    gc_.reset();
  }

  /*##########################################################################*
   * Internal utility functions
   *##########################################################################*/

  void
  AddGarbage(  //
      std::promise<GarbageRef> p)
  {
    GarbageRef target_weak_ptrs;
    for (size_t loop = 0; loop < kGarbageNumLarge; ++loop) {
      auto *target = new Target{loop};
      auto *page = gc_->GetPageIfPossible<SharedPtrTarget>();
      auto *target_shared = (page == nullptr) ? new std::shared_ptr<Target>{target}
                                              : new (page) std::shared_ptr<Target>{target};

      target_weak_ptrs.emplace_back(*target_shared);
      gc_->AddGarbage<SharedPtrTarget>(target_shared);
    }

    p.set_value(std::move(target_weak_ptrs));
  }

  auto
  TestGC(                       //
      const size_t thread_num)  //
      -> GarbageRef
  {
    std::vector<std::future<GarbageRef>> futures;
    for (size_t i = 0; i < thread_num; ++i) {
      std::promise<GarbageRef> p;
      futures.emplace_back(p.get_future());
      std::thread{&EpochBasedGCFixture::AddGarbage, this, std::move(p)}.detach();
    }

    GarbageRef target_weak_ptrs;
    for (auto &&future : futures) {
      auto weak_ptrs = future.get();
      target_weak_ptrs.insert(target_weak_ptrs.end(), weak_ptrs.begin(), weak_ptrs.end());
    }

    return target_weak_ptrs;
  }

  auto
  TestReuse(                    //
      const size_t thread_num)  //
      -> GarbageRef
  {
    // an array for embedding reserved pages
    std::array<std::atomic<std::shared_ptr<Target> *>, kThreadNum> arr{};
    for (auto &&page : arr) {
      page.store(nullptr, std::memory_order_release);
    }

    // a lambda function for embedding pages in each thread
    auto f = [&](std::promise<GarbageRef> p) {
      // prepare a random-value engine
      std::mt19937_64 rand_engine(std::random_device{}());
      std::uniform_int_distribution<size_t> id_dist{0, arr.size() - 1};

      GarbageRef target_weak_ptrs;
      for (size_t loop = 0; loop < kGarbageNumLarge; ++loop) {
        const auto guard = gc_->CreateEpochGuard();

        // prepare a page for embedding
        auto *target = new Target{loop};
        auto *page = gc_->GetPageIfPossible<SharedPtrTarget>();
        auto *target_shared =  //
            (page == nullptr) ? new std::shared_ptr<Target>{target}
                              : new (page) std::shared_ptr<Target>{target};

        // embed the page
        const auto pos = id_dist(rand_engine);
        auto *expected = arr.at(pos).load(std::memory_order_acquire);
        while (!arr.at(pos).compare_exchange_weak(expected, target_shared,  //
                                                  std::memory_order_acq_rel)) {
          // continue until embedding succeeds
        }

        target_weak_ptrs.emplace_back(*target_shared);
        if (expected != nullptr) gc_->AddGarbage<SharedPtrTarget>(expected);
      }

      p.set_value(std::move(target_weak_ptrs));
    };

    // run a reuse test with multi-threads
    std::vector<std::future<GarbageRef>> futures;
    for (size_t i = 0; i < thread_num; ++i) {
      std::promise<GarbageRef> p;
      futures.emplace_back(p.get_future());
      std::thread{f, std::move(p)}.detach();
    }

    // gather weak pointers of GC targets
    GarbageRef target_weak_ptrs;
    for (auto &&future : futures) {
      auto weak_ptrs = future.get();
      target_weak_ptrs.insert(target_weak_ptrs.end(), weak_ptrs.begin(), weak_ptrs.end());
    }

    // delete remaining instances
    for (auto &&page : arr) {
      auto *shared_p = page.load(std::memory_order_acquire);
      delete shared_p;
    }

    return target_weak_ptrs;
  }

  /*##########################################################################*
   * Functions for verification
   *##########################################################################*/

  void
  VerifyDestructor(  //
      const size_t thread_num)
  {
    gc_ = GCBuilder{}.SetGCInterval(kGCInterval).SetGCThreadNum(thread_num).Build();

    // register garbage to GC
    auto target_weak_ptrs = TestGC(thread_num);

    // GC deletes all targets during its deconstruction
    gc_.reset(nullptr);

    // check there is no reference to target pointers
    for (auto &&target_weak : target_weak_ptrs) {
      EXPECT_TRUE(target_weak.expired());
    }
  }

  void
  VerifyCreateEpochGuard(  //
      const size_t thread_num)
  {
    gc_ = GCBuilder{}.SetGCInterval(kGCInterval).SetGCThreadNum(thread_num).Build();

    // a lambda function to keep an epoch guard
    auto create_guard = [&](std::promise<Target> p) {
      const auto guard = gc_->CreateEpochGuard();
      p.set_value(0);  // set promise to notice
      const auto lock = std::unique_lock<std::mutex>{mtx_};
    };

    // create an epoch guard on anther thread
    std::thread guarder;

    {
      // create a lock to keep an epoch guard
      const auto thread_lock = std::unique_lock<std::mutex>{mtx_};

      // create an epoch guard
      std::promise<Target> p;
      auto f = p.get_future();
      guarder = std::thread{create_guard, std::move(p)};
      f.get();

      // register garbage to GC
      auto target_weak_ptrs = TestGC(thread_num);

      // check target pointers remain
      for (auto &&target_weak : target_weak_ptrs) {
        EXPECT_FALSE(target_weak.expired());
      }
    }

    guarder.join();
  }

  void
  VerifyReusePageIfPossible(  //
      const size_t thread_num)
  {
    gc_ = GCBuilder{}.SetGCInterval(kGCInterval).SetGCThreadNum(thread_num).Build();

    // register garbage to GC
    auto target_weak_ptrs = TestReuse(thread_num);
    gc_->StopGC();

    // check there is no reference to target pointers
    for (auto &&target_weak : target_weak_ptrs) {
      EXPECT_TRUE(target_weak.expired());
    }
  }

  static void
  VerifyDefaultTarget()
  {
    auto &&gc = GCBuilder{}.SetGCInterval(kGCInterval).SetGCThreadNum(kThreadNum).Build();

    for (size_t loop = 0; loop < kGarbageNumLarge; ++loop) {
      auto *target = new Target{loop};
      gc->AddGarbage(target);
    }
  }

  /*##########################################################################*
   * Internal member variables
   *##########################################################################*/

  std::unique_ptr<EpochBasedGC_t> gc_{};

  std::mutex mtx_{};
};

/*############################################################################*
 * Unit test definitions
 *############################################################################*/

TEST_F(  //
    EpochBasedGCFixture,
    DestructorWithSingleThreadReleaseAllGarbage)
{
  VerifyDestructor(1);
}

TEST_F(  //
    EpochBasedGCFixture,
    DestructorWithManyThreadsReleaseAllGarbage)
{
  VerifyDestructor(kThreadNum);
}

TEST_F(  //
    EpochBasedGCFixture,
    CreateEpochGuardWithSingleThreadProtectGarbage)
{
  VerifyCreateEpochGuard(1);
}

TEST_F(  //
    EpochBasedGCFixture,
    CreateEpochGuardWithManyThreadsProtectGarbage)
{
  VerifyCreateEpochGuard(kThreadNum);
}

TEST_F(  //
    EpochBasedGCFixture,
    ReusePageWithSingleThreadConsistentlyReusePages)
{
  VerifyReusePageIfPossible(1);
}

TEST_F(  //
    EpochBasedGCFixture,
    ReusePageWithManyThreadsConsistentlyReusePages)
{
  VerifyReusePageIfPossible(kThreadNum);
}

TEST_F(  //
    EpochBasedGCFixture,
    EmptyDeclarationActAsGCOnlyMode)
{
  VerifyDefaultTarget();
}

TEST_F(  //
    EpochBasedGCFixture,
    GCManageCommonPageSizes)
{
  auto &&gc = GCBuilder{}.SetGCInterval(kGCInterval).Build();
  for (size_t page_size = k512; page_size <= k64Ki; page_size <<= 1UL) {
    auto *page = ::operator new(page_size, GetAlignValOnVirtualPages(page_size));
    gc->AddGarbage(page, page_size);
    do {
      page = gc->GetPageIfPossible(page_size);
    } while (!page);
  }
  EXPECT_EQ(gc->GetPageIfPossible(0), nullptr);
  EXPECT_THROW(gc->AddGarbage(nullptr, 0), std::runtime_error);
}

}  // namespace dbgroup::memory::test
