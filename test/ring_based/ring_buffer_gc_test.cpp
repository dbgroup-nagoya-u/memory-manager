// Copyright (c) DB Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#include "gc/ring_buffer_gc.hpp"

#include <gtest/gtest.h>

#include <future>
#include <thread>
#include <vector>

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
  constexpr auto kLoopNum = 5E3;

  // keep garbage targets
  std::vector<std::weak_ptr<size_t>> target_weak_ptrs;

  // register garbages to GC
  {
    auto gc = RingBufferBasedGC<std::shared_ptr<size_t>>{kDefaultGCInterval};

    for (size_t loop = 0; loop < kLoopNum; ++loop) {
      const auto guard = gc.CreateEpochGuard();
      const auto target_shared = new std::shared_ptr<size_t>(new size_t{loop});
      target_weak_ptrs.emplace_back(*target_shared);
      gc.AddGarbage(target_shared);

      std::this_thread::sleep_for(std::chrono::microseconds(50));
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
  constexpr auto kThreadNum = 10;
  constexpr auto kLoopNum = 5E3;
  std::vector<std::weak_ptr<size_t>> target_weak_ptrs;

  // register garbages to GC
  {
    auto gc = RingBufferBasedGC<std::shared_ptr<size_t>>{kDefaultGCInterval};

    // a lambda function to add garbages in multi-threads
    auto f = [&](std::promise<std::vector<std::weak_ptr<size_t>>> p) {
      std::vector<std::weak_ptr<size_t>> target_vec;
      for (size_t loop = 0; loop < kLoopNum; ++loop) {
        const auto guard = gc.CreateEpochGuard();
        const auto target_shared = new std::shared_ptr<size_t>(new size_t{loop});
        target_vec.emplace_back(*target_shared);
        gc.AddGarbage(target_shared);

        std::this_thread::sleep_for(std::chrono::microseconds(50));
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
      auto target_vec = future.get();
      target_weak_ptrs.insert(target_weak_ptrs.begin(), target_vec.begin(), target_vec.end());
    }

    // GC deletes all targets when it leaves this scope
  }

  // check there is no referece to target pointers
  for (auto &&target_weak : target_weak_ptrs) {
    EXPECT_EQ(0, target_weak.use_count());
  }
}

}  // namespace dbgroup::gc
