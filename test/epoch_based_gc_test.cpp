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
  using Target = size_t;

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
  }

  void
  TearDown() override
  {
  }

  /*################################################################################################
   * Internal utility functions
   *##############################################################################################*/
 public:
  void
  AddGarbages(  //
      std::promise<std::vector<std::weak_ptr<Target>>> p,
      EpochBasedGC<std::shared_ptr<Target>> *gc,
      const size_t garbage_num)
  {
    std::vector<std::weak_ptr<Target>> target_weak_ptrs;
    for (size_t loop = 0; loop < garbage_num; ++loop) {
      std::shared_ptr<Target> *target_shared;
      {
        const auto guard = gc->CreateEpochGuard();

        auto target = new Target{loop};
        void *page = gc->GetPageIfPossible();
        if (page == nullptr) {
          target_shared = new std::shared_ptr<Target>{target};
        } else {
          target_shared = new (page) std::shared_ptr<Target>{target};
        }

        target_weak_ptrs.emplace_back(*target_shared);
      }
      gc->AddGarbage(target_shared);
    }
    p.set_value(std::move(target_weak_ptrs));
  }

  void
  KeepEpochGuard(  //
      std::promise<Target> p,
      EpochBasedGC<std::shared_ptr<Target>> *gc)
  {
    const auto guard = gc->CreateEpochGuard();
    p.set_value(0);  // set promise to notice
    const auto lock = std::unique_lock<std::mutex>{mtx};
  }

  std::vector<std::weak_ptr<Target>>
  TestGC(  //
      EpochBasedGC<std::shared_ptr<Target>> *gc,
      const size_t thread_num,
      const size_t garbage_num)
  {
    std::vector<std::future<std::vector<std::weak_ptr<Target>>>> futures;
    for (size_t i = 0; i < thread_num; ++i) {
      std::promise<std::vector<std::weak_ptr<Target>>> p;
      futures.emplace_back(p.get_future());
      std::thread{&EpochBasedGCFixture::AddGarbages, this, std::move(p), gc, garbage_num}.detach();
    }

    std::vector<std::weak_ptr<Target>> target_weak_ptrs;
    for (auto &&future : futures) {
      auto weak_ptrs = future.get();
      target_weak_ptrs.insert(target_weak_ptrs.end(), weak_ptrs.begin(), weak_ptrs.end());
    }

    return target_weak_ptrs;
  }

  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  std::mutex mtx;
};

/*--------------------------------------------------------------------------------------------------
 * Public utility tests
 *------------------------------------------------------------------------------------------------*/

TEST_F(EpochBasedGCFixture, Construct_GCStoped_MemberVariablesCorrectlyInitialized)
{
  auto gc = EpochBasedGC<Target>{kGCInterval};

  auto gc_is_running = gc.StopGC();

  EXPECT_FALSE(gc_is_running);
}

TEST_F(EpochBasedGCFixture, Destruct_SingleThread_GarbagesCorrectlyFreed)
{
  // keep garbage targets
  std::vector<std::weak_ptr<Target>> target_weak_ptrs;

  // register garbages to GC
  {
    auto gc = EpochBasedGC<std::shared_ptr<Target>>{kGCInterval};
    gc.StartGC();
    target_weak_ptrs = TestGC(&gc, 1, kGarbageNumLarge);

    // GC deletes all targets when it leaves this scope
  }

  // check there is no referece to target pointers
  for (auto &&target_weak : target_weak_ptrs) {
    EXPECT_TRUE(target_weak.expired());
  }
}

TEST_F(EpochBasedGCFixture, Destruct_MultiThreads_GarbagesCorrectlyFreed)
{
  // keep garbage targets
  std::vector<std::weak_ptr<Target>> target_weak_ptrs;

  // register garbages to GC
  {
    auto gc = EpochBasedGC<std::shared_ptr<Target>>{kGCInterval, kThreadNum};
    gc.StartGC();
    target_weak_ptrs = TestGC(&gc, kThreadNum, kGarbageNumLarge);

    // GC deletes all targets when it leaves this scope
  }

  // check there is no referece to target pointers
  for (auto &&target_weak : target_weak_ptrs) {
    EXPECT_TRUE(target_weak.expired());
  }
}

TEST_F(EpochBasedGCFixture, RunGC_SingleThread_GarbagesCorrectlyFreed)
{
  auto gc = EpochBasedGC<std::shared_ptr<Target>>{kGCInterval};
  gc.StartGC();

  // keep garbage targets
  std::vector<std::weak_ptr<Target>> target_weak_ptrs;

  // register garbages to GC
  target_weak_ptrs = TestGC(&gc, 1, kGarbageNumLarge);
  while (gc.GetRegisteredGarbageSize() > 0) {
    // wait all garbages are freed
  }

  // check there is no referece to target pointers
  for (auto &&target_weak : target_weak_ptrs) {
    EXPECT_TRUE(target_weak.expired());
  }
}

TEST_F(EpochBasedGCFixture, RunGC_MultiThreads_GarbagesCorrectlyFreed)
{
  auto gc = EpochBasedGC<std::shared_ptr<Target>>{kGCInterval, kThreadNum};
  gc.StartGC();

  // keep garbage targets
  std::vector<std::weak_ptr<Target>> target_weak_ptrs;

  // register garbages to GC
  target_weak_ptrs = TestGC(&gc, kThreadNum, kGarbageNumLarge);
  while (gc.GetRegisteredGarbageSize() > 0) {
    // wait all garbages are freed
  }

  // check there is no referece to target pointers
  for (auto &&target_weak : target_weak_ptrs) {
    EXPECT_TRUE(target_weak.expired());
  }
}

TEST_F(EpochBasedGCFixture, CreateEpochGuard_SingleThread_PreventGarbagesFromDeleeting)
{
  auto gc = EpochBasedGC<std::shared_ptr<Target>>{kGCInterval};
  gc.StartGC();

  // keep garbage targets
  std::vector<std::weak_ptr<Target>> target_weak_ptrs;

  // create an epoch guard on anther thread
  std::thread guarder;

  {
    const auto thread_lock = std::unique_lock<std::mutex>{mtx};
    std::promise<Target> p;
    auto f = p.get_future();
    guarder = std::thread{&EpochBasedGCFixture::KeepEpochGuard, this, std::move(p), &gc};
    f.get();

    // register garbages to GC
    target_weak_ptrs = TestGC(&gc, 1, kGarbageNumSmall);

    // wait GC
    std::this_thread::sleep_for(std::chrono::microseconds(kGCInterval * 2));

    // check target pointers remain
    for (auto &&target_weak : target_weak_ptrs) {
      EXPECT_FALSE(target_weak.expired());
    }
  }

  guarder.join();
}

TEST_F(EpochBasedGCFixture, CreateEpochGuard_MultiThreads_PreventGarbagesFromDeleeting)
{
  auto gc = EpochBasedGC<std::shared_ptr<Target>>{kGCInterval, kThreadNum};
  gc.StartGC();

  // keep garbage targets
  std::vector<std::weak_ptr<Target>> target_weak_ptrs;

  // create an epoch guard on anther thread
  std::thread guarder;

  {
    const auto thread_lock = std::unique_lock<std::mutex>{mtx};
    std::promise<Target> p;
    auto f = p.get_future();
    guarder = std::thread{&EpochBasedGCFixture::KeepEpochGuard, this, std::move(p), &gc};
    f.get();

    // register garbages to GC
    target_weak_ptrs = TestGC(&gc, kThreadNum, kGarbageNumSmall);

    // wait GC
    std::this_thread::sleep_for(std::chrono::microseconds(kGCInterval * 2));

    // check target pointers remain
    for (auto &&target_weak : target_weak_ptrs) {
      EXPECT_FALSE(target_weak.expired());
    }
  }

  guarder.join();
}

}  // namespace dbgroup::memory::test
