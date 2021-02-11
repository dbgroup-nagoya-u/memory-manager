// Copyright (c) DB Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#include "gc/epoch_based_gc.hpp"

#include <future>
#include <thread>
#include <vector>

#include "gtest/gtest.h"

namespace gc::epoch
{
class EpochBasedGCFixture : public ::testing::Test
{
 public:
  static constexpr size_t kDefaultGCInterval = 1;

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
  constexpr size_t kLoopNum = 100;

  // keep garbage targets
  std::vector<size_t *> target_ptrs;

  // register garbages to GC
  {
    auto gc = EpochBasedGC<size_t>{kDefaultGCInterval};

    for (size_t loop = 0; loop < kLoopNum; ++loop) {
      const auto guard = gc.CreateEpochGuard();
      target_ptrs.emplace_back(new size_t{loop + 1});
      gc.AddGarbage(guard, target_ptrs.back());
      std::this_thread::sleep_for(std::chrono::microseconds(10));
    }

    // GC deletes all targets when it leaves this scope
  }

  // this check is danger: these lines may fail due to freed memory space
  for (size_t loop = 0; loop < kLoopNum; ++loop) {
    const auto expected = loop + 1;
    const auto actual = *target_ptrs[loop];
    EXPECT_NE(expected, actual);
  }
}

TEST_F(EpochBasedGCFixture, RunGC_AddGarbagesFromMultiThreads_AllTargetsAreDeleted)
{
  // keep garbage targets
  constexpr size_t kThreadNum = 100;
  constexpr size_t kLoopNum = 100;
  std::vector<std::vector<size_t *>> target_ptrs;

  // register garbages to GC
  {
    auto gc = EpochBasedGC<size_t>{kDefaultGCInterval};

    // a lambda function to add garbages in multi-threads
    auto f = [&gc = gc](std::promise<std::vector<size_t *>> p) {
      std::vector<size_t *> target_vec;
      for (size_t loop = 0; loop < kLoopNum; ++loop) {
        const auto guard = gc.CreateEpochGuard();
        target_vec.emplace_back(new size_t{loop + 1});
        gc.AddGarbage(guard, target_vec.back());
        std::this_thread::sleep_for(std::chrono::microseconds(100));
      }
      p.set_value(target_vec);
    };

    // add garbages and catch original targets via promise/future
    std::vector<std::future<std::vector<size_t *>>> target_futures;
    for (size_t count = 0; count < kThreadNum; ++count) {
      std::promise<std::vector<size_t *>> p;
      target_futures.emplace_back(p.get_future());
      auto t = std::thread{f, std::move(p)};
      t.detach();
    }
    for (auto &&future : target_futures) {
      target_ptrs.emplace_back(future.get());
    }

    // GC deletes all targets when it leaves this scope
  }

  // this check is danger: these lines may fail due to freed memory space
  for (size_t thread = 0; thread < kThreadNum; ++thread) {
    for (size_t loop = 0; loop < kLoopNum; ++loop) {
      const auto expected = loop + 1;
      const auto actual = *target_ptrs[thread][loop];
      EXPECT_NE(expected, actual);
    }
  }
}

}  // namespace gc::epoch
