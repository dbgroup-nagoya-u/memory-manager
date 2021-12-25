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

#include "memory/epoch_based_gc.hpp"

#include <future>
#include <memory>
#include <mutex>
#include <random>
#include <thread>
#include <vector>

#include "common.hpp"
#include "gtest/gtest.h"

namespace dbgroup::memory::test
{
class EpochBasedGCFixture : public ::testing::Test
{
 protected:
  /*####################################################################################
   * Type aliases
   *##################################################################################*/
  using Target = uint64_t;
  using EpochBasedGC_t = EpochBasedGC<std::shared_ptr<Target>>;
  using GarbageRef = std::vector<std::weak_ptr<Target>>;

  /*####################################################################################
   * Internal constants
   *##################################################################################*/

  static constexpr size_t kGCInterval = 100000;
  static constexpr size_t kGarbageNumLarge = 1E6;
  static constexpr size_t kGarbageNumSmall = 10;

  /*####################################################################################
   * Test setup/teardown
   *##################################################################################*/

  void
  SetUp() override
  {
    gc_ = std::make_unique<EpochBasedGC_t>(kGCInterval, kThreadNum, true);
  }

  void
  TearDown() override
  {
  }

  /*####################################################################################
   * Internal utility functions
   *##################################################################################*/

  void
  AddGarbages(  //
      std::promise<GarbageRef> p,
      const size_t garbage_num)
  {
    GarbageRef target_weak_ptrs;
    for (size_t loop = 0; loop < garbage_num; ++loop) {
      auto *target = new Target{loop};
      auto *page = gc_->GetPageIfPossible<std::shared_ptr<Target>>();
      std::shared_ptr<Target> *target_shared =  //
          (page == nullptr) ? new std::shared_ptr<Target>{target}
                            : new (page) std::shared_ptr<Target>{target};

      target_weak_ptrs.emplace_back(*target_shared);
      gc_->AddGarbage(target_shared);
    }

    p.set_value(std::move(target_weak_ptrs));
  }

  void
  KeepEpochGuard(std::promise<Target> p)
  {
    const auto guard = gc_->CreateEpochGuard();
    p.set_value(0);  // set promise to notice
    const auto lock = std::unique_lock<std::mutex>{mtx_};
  }

  GarbageRef
  TestGC(  //
      const size_t thread_num,
      const size_t garbage_num)
  {
    std::vector<std::future<GarbageRef>> futures;
    for (size_t i = 0; i < thread_num; ++i) {
      std::promise<GarbageRef> p;
      futures.emplace_back(p.get_future());
      std::thread{&EpochBasedGCFixture::AddGarbages, this, std::move(p), garbage_num}.detach();
    }

    GarbageRef target_weak_ptrs;
    for (auto &&future : futures) {
      auto weak_ptrs = future.get();
      target_weak_ptrs.insert(target_weak_ptrs.end(), weak_ptrs.begin(), weak_ptrs.end());
    }

    return target_weak_ptrs;
  }

  GarbageRef
  TestReuse(const size_t garbage_num)
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
      for (size_t loop = 0; loop < garbage_num; ++loop) {
        const auto guard = gc_->CreateEpochGuard();

        // prepare a page for embedding
        auto *target = new Target{loop};
        auto *page = gc_->GetPageIfPossible<std::shared_ptr<Target>>();
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
        if (expected != nullptr) gc_->AddGarbage(expected);
      }

      p.set_value(std::move(target_weak_ptrs));
    };

    // run a reuse test with multi-threads
    std::vector<std::future<GarbageRef>> futures;
    for (size_t i = 0; i < kThreadNum; ++i) {
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

  /*####################################################################################
   * Functions for verification
   *##################################################################################*/

  void
  VerifyDestructor(const size_t thread_num)
  {
    // register garbages to GC
    auto target_weak_ptrs = TestGC(thread_num, kGarbageNumLarge);

    // GC deletes all targets during its deconstruction
    gc_.reset(nullptr);

    // check there is no referece to target pointers
    for (auto &&target_weak : target_weak_ptrs) {
      EXPECT_TRUE(target_weak.expired());
    }
  }

  void
  VerifyStopGC(const size_t thread_num)
  {
    // register garbages to GC
    auto target_weak_ptrs = TestGC(thread_num, kGarbageNumLarge);

    // GC deletes all targets
    gc_->StopGC();

    // check there is no referece to target pointers
    for (auto &&target_weak : target_weak_ptrs) {
      EXPECT_TRUE(target_weak.expired());
    }
  }

  void
  VerifyCreateEpochGuard(const size_t thread_num)
  {
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

      // register garbages to GC
      auto target_weak_ptrs = TestGC(thread_num, kGarbageNumLarge);

      // check target pointers remain
      for (auto &&target_weak : target_weak_ptrs) {
        EXPECT_FALSE(target_weak.expired());
      }
    }

    guarder.join();
  }

  void
  VerifyReusePageIfPossible()
  {
    // register garbages to GC
    auto target_weak_ptrs = TestReuse(kGarbageNumLarge);
    gc_->StopGC();

    // check there is no referece to target pointers
    for (auto &&target_weak : target_weak_ptrs) {
      EXPECT_TRUE(target_weak.expired());
    }
  }

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  std::unique_ptr<EpochBasedGC_t> gc_{};

  std::mutex mtx_{};
};

/*######################################################################################
 * Unit test definitions
 *####################################################################################*/

TEST_F(EpochBasedGCFixture, DestructorWithSingleThreadReleaseAllGarbages)
{  //
  VerifyDestructor(1);
}

TEST_F(EpochBasedGCFixture, DestructorWithMultiThreadsReleaseAllGarbages)
{  //
  VerifyDestructor(kThreadNum);
}

TEST_F(EpochBasedGCFixture, StopGCWithSingleThreadReleaseAllGarbages)
{  //
  VerifyStopGC(1);
}

TEST_F(EpochBasedGCFixture, StopGCWithMultiThreadsReleaseAllGarbages)
{  //
  VerifyStopGC(kThreadNum);
}

TEST_F(EpochBasedGCFixture, CreateEpochGuardWithSingleThreadProtectGarbages)
{
  VerifyCreateEpochGuard(1);
}

TEST_F(EpochBasedGCFixture, CreateEpochGuardWithMultiThreadsProtectGarbages)
{
  VerifyCreateEpochGuard(kThreadNum);
}

TEST_F(EpochBasedGCFixture, ReusePageIfPossibleWithMultiThreadsReleasePageOnlyOnce)
{  //
  VerifyReusePageIfPossible();
}

}  // namespace dbgroup::memory::test
