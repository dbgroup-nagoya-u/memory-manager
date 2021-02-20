// Copyright (c) DB Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#include "gc/epoch_based_gc.hpp"

#include <future>
#include <thread>
#include <vector>

#include "gtest/gtest.h"

namespace dbgroup::gc
{
class EpochBasedGCFixture : public ::testing::Test
{
 public:
  static constexpr size_t kDefaultGCInterval = 10;

 protected:
  void
  SetUp() override
  {
  }

  void
  TearDown() override
  {
  }
};

/*--------------------------------------------------------------------------------------------------
 * Public utility tests
 *------------------------------------------------------------------------------------------------*/

TEST_F(EpochBasedGCFixture, RunGC_AddGarbagesFromSingleThread_AllTargetsAreDeleted)
{
  constexpr auto kLoopNum = 1E5;

  // keep garbage targets
  std::vector<std::weak_ptr<size_t>> target_weak_ptrs;

  // register garbages to GC
  {
    auto gc = EpochBasedGC<std::shared_ptr<size_t>>{kDefaultGCInterval};

    for (size_t loop = 0; loop < kLoopNum; ++loop) {
      const auto guard = gc.CreateEpochGuard();
      const auto target_ptr = new size_t{loop};
      const auto target_shared = new std::shared_ptr<size_t>(target_ptr);
      target_weak_ptrs.emplace_back(*target_shared);
      gc.AddGarbage(target_shared);
    }

    // GC deletes all targets when it leaves this scope
  }

  // check there is no referece to target pointers
  for (auto &&target_weak : target_weak_ptrs) {
    EXPECT_EQ(0, target_weak.use_count());
  }
}

TEST_F(EpochBasedGCFixture, RunGC_AddGarbagesFromMultiThreads_AllTargetsAreDeleted)
{
  // keep garbage targets
  constexpr auto kThreadNum = kPartitionNum * 4;
  constexpr auto kLoopNum = 1E5;
  std::vector<std::vector<std::weak_ptr<size_t>>> target_weak_ptrs;

  // register garbages to GC
  {
    auto gc = EpochBasedGC<std::shared_ptr<size_t>>{kDefaultGCInterval};

    // a lambda function to add garbages in multi-threads
    auto f = [&](std::promise<std::vector<std::weak_ptr<size_t>>> p) {
      std::vector<std::weak_ptr<size_t>> target_vec;
      for (size_t loop = 0; loop < kLoopNum; ++loop) {
        const auto guard = gc.CreateEpochGuard();
        const auto target_ptr = new size_t{loop};
        const auto target_shared = new std::shared_ptr<size_t>(target_ptr);
        target_vec.emplace_back(*target_shared);
        gc.AddGarbage(target_shared);
      }
      p.set_value(target_vec);
    };

    // add garbages and catch original targets via promise/future
    std::vector<std::future<std::vector<std::weak_ptr<size_t>>>> target_futures;
    for (size_t count = 0; count < kThreadNum; ++count) {
      std::promise<std::vector<std::weak_ptr<size_t>>> p;
      target_futures.emplace_back(p.get_future());
      auto t = std::thread{f, std::move(p)};
      t.detach();
    }
    for (auto &&future : target_futures) {
      target_weak_ptrs.emplace_back(future.get());
    }

    // GC deletes all targets when it leaves this scope
  }

  // check there is no referece to target pointers
  for (auto &&threaded_weak_ptrs : target_weak_ptrs) {
    for (auto &&target_weak : threaded_weak_ptrs) {
      EXPECT_EQ(0, target_weak.use_count());
    }
  }
}

}  // namespace dbgroup::gc
