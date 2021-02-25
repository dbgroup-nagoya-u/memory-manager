// Copyright (c) DB Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#include "gc/tls_based_gc.hpp"

#include <gtest/gtest.h>

#include <future>
#include <memory>
#include <thread>
#include <vector>

namespace dbgroup::memory
{
class TLSBasedGCFixture : public ::testing::Test
{
 public:
  static constexpr size_t kGCInterval = 1E2;

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

TEST_F(TLSBasedGCFixture, Construct_GCStarted_MemberVariablesCorrectlyInitialized)
{
  auto gc = TLSBasedGC<size_t>{kGCInterval, true};

  auto gc_is_running = gc.StopGC();

  EXPECT_TRUE(gc_is_running);
}

TEST_F(TLSBasedGCFixture, Construct_GCStopped_MemberVariablesCorrectlyInitialized)
{
  auto gc = TLSBasedGC<size_t>{kGCInterval, false};

  auto gc_is_stopped = gc.StartGC();

  EXPECT_TRUE(gc_is_stopped);
}

TEST_F(TLSBasedGCFixture, Destruct_SingleThread_GarbagesCorrectlyFreed)
{
  constexpr size_t kLoopNum = 1E5;

  // keep garbage targets
  std::vector<std::weak_ptr<size_t>> target_weak_ptrs;

  // register garbages to GC
  {
    auto gc = TLSBasedGC<std::shared_ptr<size_t>>{kGCInterval};

    auto f = [&]() {
      for (size_t loop = 0; loop < kLoopNum; ++loop) {
        std::shared_ptr<size_t> *target_shared;
        {
          const auto guard = gc.CreateEpochGuard();
          target_shared = new std::shared_ptr<size_t>(new size_t{loop});
          target_weak_ptrs.emplace_back(*target_shared);
        }
        gc.AddGarbage(target_shared);
      }
    };

    std::thread{f}.join();

    // GC deletes all targets when it leaves this scope
  }

  // check there is no referece to target pointers
  for (auto &&target_weak : target_weak_ptrs) {
    EXPECT_EQ(0, target_weak.use_count());
  }
}

TEST_F(TLSBasedGCFixture, Destruct_MultiThreads_GarbagesCorrectlyFreed)
{
  constexpr size_t kLoopNum = 1E5;
  constexpr size_t kThreadNum = 10;

  // keep garbage targets
  std::vector<std::weak_ptr<size_t>> target_weak_ptrs;

  // register garbages to GC
  {
    auto gc = TLSBasedGC<std::shared_ptr<size_t>>{kGCInterval};

    auto f = [&gc](std::promise<std::vector<std::weak_ptr<size_t>>> p) {
      std::vector<std::weak_ptr<size_t>> weak_ptrs;

      for (size_t loop = 0; loop < kLoopNum; ++loop) {
        std::shared_ptr<size_t> *target_shared;
        {
          const auto guard = gc.CreateEpochGuard();
          target_shared = new std::shared_ptr<size_t>(new size_t{loop});
          weak_ptrs.emplace_back(*target_shared);
        }
        gc.AddGarbage(target_shared);
      }

      p.set_value(weak_ptrs);
    };

    std::vector<std::future<std::vector<std::weak_ptr<size_t>>>> futures;
    for (size_t count = 0; count < kThreadNum; ++count) {
      std::promise<std::vector<std::weak_ptr<size_t>>> p;
      futures.emplace_back(p.get_future());
      std::thread{f, std::move(p)}.detach();
    }
    for (auto &&future : futures) {
      auto weak_ptrs = future.get();
      target_weak_ptrs.insert(target_weak_ptrs.end(), weak_ptrs.begin(), weak_ptrs.end());
    }

    // GC deletes all targets when it leaves this scope
  }

  // check there is no referece to target pointers
  for (auto &&target_weak : target_weak_ptrs) {
    EXPECT_EQ(0, target_weak.use_count());
  }
}

}  // namespace dbgroup::memory
