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
#include <thread>
#include <vector>

#include "common.hpp"
#include "gtest/gtest.h"

namespace dbgroup::memory::test
{
class EpochBasedGCFixture : public ::testing::Test
{
 protected:
  /*################################################################################################
   * Type aliases
   *##############################################################################################*/
  using Target = uint64_t;
  using EpochBasedGC_t = EpochBasedGC<std::shared_ptr<Target>>;
  using GarbageRef = std::vector<std::weak_ptr<Target>>;

  /*################################################################################################
   * Internal constants
   *##############################################################################################*/

  static constexpr size_t kGCInterval = 1000;
  static constexpr size_t kGarbageNumLarge = 1E5;
  static constexpr size_t kGarbageNumSmall = 10;

  /*################################################################################################
   * Test setup/teardown
   *##############################################################################################*/

  void
  SetUp() override
  {
    gc = std::make_unique<EpochBasedGC_t>(kGCInterval, kThreadNum, true);
  }

  void
  TearDown() override
  {
  }

  /*################################################################################################
   * Internal utility functions
   *##############################################################################################*/

  void
  AddGarbages(  //
      std::promise<GarbageRef> p,
      const size_t garbage_num)
  {
    GarbageRef target_weak_ptrs;
    for (size_t loop = 0; loop < garbage_num; ++loop) {
      auto target = new Target{loop};
      void *page = gc->GetPageIfPossible();
      std::shared_ptr<Target> *target_shared =  //
          (page == nullptr) ? new std::shared_ptr<Target>{target}
                            : new (page) std::shared_ptr<Target>{target};

      target_weak_ptrs.emplace_back(*target_shared);
      gc->AddGarbage(target_shared);
    }

    p.set_value(std::move(target_weak_ptrs));
  }

  void
  KeepEpochGuard(std::promise<Target> p)
  {
    const auto guard = gc->CreateEpochGuard();
    p.set_value(0);  // set promise to notice
    const auto lock = std::unique_lock<std::mutex>{mtx};
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

  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  std::unique_ptr<EpochBasedGC_t> gc;

  std::mutex mtx;
};

/*--------------------------------------------------------------------------------------------------
 * Public utility tests
 *------------------------------------------------------------------------------------------------*/

TEST_F(EpochBasedGCFixture, Destruct_SingleThread_GarbagesCorrectlyFreed)
{
  // register garbages to GC
  auto target_weak_ptrs = TestGC(1, kGarbageNumLarge);

  // GC deletes all targets during its deconstruction
  gc.reset(nullptr);

  // check there is no referece to target pointers
  for (auto &&target_weak : target_weak_ptrs) {
    EXPECT_TRUE(target_weak.expired());
  }
}

TEST_F(EpochBasedGCFixture, Destruct_MultiThreads_GarbagesCorrectlyFreed)
{
  // register garbages to GC
  auto target_weak_ptrs = TestGC(kThreadNum, kGarbageNumLarge);

  // GC deletes all targets during its deconstruction
  gc.reset(nullptr);

  // check there is no referece to target pointers
  for (auto &&target_weak : target_weak_ptrs) {
    EXPECT_TRUE(target_weak.expired());
  }
}

TEST_F(EpochBasedGCFixture, StartGC_SingleThreadWithoutEpochGuard_GarbagesCorrectlyFreed)
{
  // register garbages to GC
  auto target_weak_ptrs = TestGC(1, kGarbageNumLarge);
  while (gc->GetRegisteredGarbageSize() > 0) {
    // wait all garbages are freed
  }

  // check there is no referece to target pointers
  for (auto &&target_weak : target_weak_ptrs) {
    EXPECT_TRUE(target_weak.expired());
  }
}

TEST_F(EpochBasedGCFixture, StartGC_MultiThreadsWithoutEpochGuard_GarbagesCorrectlyFreed)
{
  // register garbages to GC
  auto target_weak_ptrs = TestGC(kThreadNum, kGarbageNumLarge);
  while (gc->GetRegisteredGarbageSize() > 0) {
    // wait all garbages are freed
  }

  // check there is no referece to target pointers
  for (auto &&target_weak : target_weak_ptrs) {
    EXPECT_TRUE(target_weak.expired());
  }
}

TEST_F(EpochBasedGCFixture, StartGC_SingleThreadWithEpochGuard_PreventGarbagesFromDeleeting)
{
  // a lambda function to keep an epoch guard
  auto guard = [&](std::promise<Target> p) {
    const auto guard = gc->CreateEpochGuard();
    p.set_value(0);  // set promise to notice
    const auto lock = std::unique_lock<std::mutex>{mtx};
  };

  // create an epoch guard on anther thread
  std::thread guarder;

  {
    // create a lock to keep an epoch guard
    const auto thread_lock = std::unique_lock<std::mutex>{mtx};

    // create an epoch guard
    std::promise<Target> p;
    auto f = p.get_future();
    guarder = std::thread{guard, std::move(p)};
    f.get();

    // register garbages to GC
    auto target_weak_ptrs = TestGC(1, kGarbageNumLarge);

    // check target pointers remain
    for (auto &&target_weak : target_weak_ptrs) {
      EXPECT_FALSE(target_weak.expired());
    }
  }

  guarder.join();
}

TEST_F(EpochBasedGCFixture, StartGC_MultiThreadsWithEpochGuard_PreventGarbagesFromDeleeting)
{
  // a lambda function to keep an epoch guard
  auto guard = [&](std::promise<Target> p) {
    const auto guard = gc->CreateEpochGuard();
    p.set_value(0);  // set promise to notice
    const auto lock = std::unique_lock<std::mutex>{mtx};
  };

  // create an epoch guard on anther thread
  std::thread guarder;

  {
    // create a lock to keep an epoch guard
    const auto thread_lock = std::unique_lock<std::mutex>{mtx};

    // create an epoch guard
    std::promise<Target> p;
    auto f = p.get_future();
    guarder = std::thread{guard, std::move(p)};
    f.get();

    // register garbages to GC
    auto target_weak_ptrs = TestGC(kThreadNum, kGarbageNumLarge);

    // check target pointers remain
    for (auto &&target_weak : target_weak_ptrs) {
      EXPECT_FALSE(target_weak.expired());
    }
  }

  guarder.join();
}

}  // namespace dbgroup::memory::test
