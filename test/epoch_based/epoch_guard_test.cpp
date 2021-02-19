// Copyright (c) DB Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#include "epoch_based/epoch_guard.hpp"

#include <thread>
#include <vector>

#include "epoch_based/epoch_manager.hpp"
#include "gtest/gtest.h"

namespace dbagroup::gc::epoch
{
class EpochGuardFixture : public ::testing::Test
{
 public:
  EpochManager epoch_manager{};

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

TEST_F(EpochGuardFixture, Construct_ArgEpochManager_MemberVariableCorrectlyInitialized)
{
  const auto guard = EpochGuard{&epoch_manager};
  const auto epoch = guard.GetEpoch();

  EXPECT_EQ(0, epoch);
}

TEST_F(EpochGuardFixture, Destruct_HundredThreads_DestructorSuccessfullyLeaveEpoch)
{
  // keep an initial epoch
  const auto begin_epoch = epoch_manager.GetCurrentEpoch();

  // create epoch-guards with 100 threads in 10 epochs
  for (size_t loop = 0; loop < 10; ++loop) {
    std::vector<std::thread> threads;
    for (size_t count = 0; count < 100; ++count) {
      threads.emplace_back([em = &epoch_manager] { const auto guard = EpochGuard{em}; });
    }
    for (auto &&thread : threads) {
      thread.join();
    }
    epoch_manager.ForwardEpoch();
  }

  // check all the threads leave corresponding epochs by a destructor
  const auto end_epoch = epoch_manager.GetCurrentEpoch();
  const auto [freeable_begin, freeable_end] = epoch_manager.ListFreeableEpoch();

  EXPECT_EQ(begin_epoch, freeable_begin);
  EXPECT_EQ(end_epoch, freeable_end);
}

}  // namespace dbagroup::gc::epoch
