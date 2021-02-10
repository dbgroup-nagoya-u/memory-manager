// Copyright (c) DB Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#include "epoch_based/epoch_manager.hpp"

#include <thread>
#include <vector>

#include "gtest/gtest.h"

namespace gc::epoch
{
class EpochManagerFixture : public ::testing::Test
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

void
CheckReturnEpochAndPartition(  //
    EpochManager *epoch_manager,
    const size_t expected_epoch)
{
  const auto [epoch, partition] = epoch_manager->EnterEpoch();

  EXPECT_EQ(expected_epoch, epoch);
  EXPECT_GE(epoch, 0);
  EXPECT_LT(epoch, kPartitionNum);
}

TEST_F(EpochManagerFixture, Construct_NoArgument_MemberVariableCorrectlyInitialized)
{
  const auto current_epoch = epoch_manager.GetCurrentEpoch();

  EXPECT_EQ(0, current_epoch);

  const auto [begin_epoch, end_epoch] = epoch_manager.ListFreeableEpoch();

  EXPECT_EQ(0, begin_epoch);
  EXPECT_EQ(0, end_epoch);
}

TEST_F(EpochManagerFixture, ForwardEpoch_GoAroundRingBuffer_EpochGoBackToZero)
{
  for (size_t count = 0; count < kBufferSize; ++count, epoch_manager.ForwardEpoch()) {
    const auto current_epoch = epoch_manager.GetCurrentEpoch();

    EXPECT_EQ(count, current_epoch);
  }

  const auto current_epoch = epoch_manager.GetCurrentEpoch();

  EXPECT_EQ(0, current_epoch);
}

TEST_F(EpochManagerFixture, EnterEpoch_HundredThreads_AllThreadsCorrectlyPartitioned)
{
  std::vector<std::thread> threads;
  auto current_epoch = epoch_manager.GetCurrentEpoch();

  for (size_t count = 0; count < 50; ++count) {
    threads.push_back(std::thread{CheckReturnEpochAndPartition, &epoch_manager, current_epoch});
  }
  for (auto &&thread : threads) {
    thread.join();
  }
  threads.clear();

  epoch_manager.ForwardEpoch();
  current_epoch = epoch_manager.GetCurrentEpoch();

  for (size_t count = 0; count < 50; ++count) {
    threads.emplace_back(std::thread{CheckReturnEpochAndPartition, &epoch_manager, current_epoch});
  }
  for (auto &&thread : threads) {
    thread.join();
  }
}

}  // namespace gc::epoch
