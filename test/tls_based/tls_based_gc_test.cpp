// Copyright (c) DB Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#include "gc/tls_based_gc.hpp"

#include <gtest/gtest.h>

#include <future>
#include <memory>
#include <thread>
#include <vector>

namespace dbgroup::gc::tls
{
class TLSBasedGCFixture : public ::testing::Test
{
 public:
  static constexpr auto kGCInterval = 100UL;

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
  constexpr size_t kLoopNum = 1E3;

  // keep garbage targets
  std::vector<std::weak_ptr<size_t>> target_weak_ptrs;

  // register garbages to GC
  {
    auto gc = TLSBasedGC<std::shared_ptr<size_t>>{kGCInterval};

    auto f = [&]() {
      for (size_t loop = 0; loop < kLoopNum; ++loop) {
        const auto guard = gc.CreateEpochGuard();
        const auto target_shared = new std::shared_ptr<size_t>(new size_t{loop});
        target_weak_ptrs.emplace_back(*target_shared);
        gc.AddGarbage(target_shared);

        std::this_thread::sleep_for(std::chrono::microseconds(50));
      }
    };

    auto t = std::thread{f};
    t.join();

    // GC deletes all targets when it leaves this scope
  }

  // check there is no referece to target pointers
  for (auto &&target_weak : target_weak_ptrs) {
    EXPECT_EQ(0, target_weak.use_count());
  }
}

}  // namespace dbgroup::gc::tls
