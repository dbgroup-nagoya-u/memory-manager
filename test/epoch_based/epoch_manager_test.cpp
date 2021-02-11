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

/*--------------------------------------------------------------------------------------------------
 * Public utility tests
 *------------------------------------------------------------------------------------------------*/

TEST_F(EpochManagerFixture, Construct_NoArgument_MemberVariableCorrectlyInitialized)
{
  const auto begin_epoch = epoch_manager.GetCurrentEpoch();

  EXPECT_EQ(0, begin_epoch);

  for (size_t count = 0; count < kBufferSize - 1; ++count) {
    epoch_manager.ForwardEpoch();
  }

  const auto end_epoch = epoch_manager.GetCurrentEpoch();
  const auto [freeable_begin, freeable_end] = epoch_manager.ListFreeableEpoch();

  EXPECT_EQ(begin_epoch, freeable_begin);
  EXPECT_EQ(end_epoch, freeable_end);
}

TEST_F(EpochManagerFixture, ForwardEpoch_GoAroundRingBuffer_EpochGoBackToZero)
{
  for (size_t count = 0; count < kBufferSize; ++count) {
    const auto current_epoch = epoch_manager.GetCurrentEpoch();

    EXPECT_EQ(count, current_epoch);

    epoch_manager.ForwardEpoch();
  }

  const auto current_epoch = epoch_manager.GetCurrentEpoch();

  EXPECT_EQ(0, current_epoch);
}

TEST_F(EpochManagerFixture, EnterEpoch_HundredThreads_AllThreadsCorrectlyPartitioned)
{
  // a lambda function to check entered epoch and partition id
  auto f = [em = &epoch_manager](const size_t expected_epoch) {
    const auto [epoch, partition] = em->EnterEpoch();

    EXPECT_EQ(expected_epoch, epoch);
    EXPECT_GE(partition, 0);
    EXPECT_LT(partition, kPartitionNum);
  };

  for (size_t loop = 0; loop < 10; ++loop) {
    // keep a current epoch to check return values
    const auto current_epoch = epoch_manager.GetCurrentEpoch();

    // create multi threads and check return values
    std::vector<std::thread> threads;
    for (size_t count = 0; count < 100; ++count) {
      threads.push_back(std::thread{f, current_epoch});
    }
    for (auto &&thread : threads) {
      thread.join();
    }
    epoch_manager.ForwardEpoch();
  }
}

TEST_F(EpochManagerFixture, EnterEpoch_OneThread_RingBufferProtectEpoch)
{
  // keep an initial epoch
  const auto begin_epoch = epoch_manager.GetCurrentEpoch();

  // forward to creat freeable epochs
  for (size_t loop = 0; loop < 10; ++loop) {
    epoch_manager.ForwardEpoch();
  }

  // enter and forward an epoch
  epoch_manager.EnterEpoch();
  const auto end_epoch = epoch_manager.GetCurrentEpoch();
  epoch_manager.ForwardEpoch();  // become end_epoch != current_epoch

  // check not-entered epochs are freeable
  const auto [freeable_begin, freeable_end] = epoch_manager.ListFreeableEpoch();

  EXPECT_EQ(begin_epoch, freeable_begin);
  EXPECT_EQ(end_epoch, freeable_end);
}

TEST_F(EpochManagerFixture, LeaveEpoch_HundredThreads_LeftEpochsBecomeFreeable)
{
  // keep an initial epoch
  const auto begin_epoch = epoch_manager.GetCurrentEpoch();

  // a lambda function to enter/leave an epoch
  auto f = [em = &epoch_manager] {
    const auto [epoch, partition] = em->EnterEpoch();
    em->LeaveEpoch(epoch, partition);
  };

  // repeat entering/leaveing epochs with epoch forwarding
  for (size_t loop = 0; loop < 10; ++loop) {
    std::vector<std::thread> threads;
    for (size_t count = 0; count < 100; ++count) {
      threads.push_back(std::thread{f});
    }
    for (auto &&thread : threads) {
      thread.join();
    }
    epoch_manager.ForwardEpoch();
  }

  // check all the epochs are freeable
  const auto end_epoch = epoch_manager.GetCurrentEpoch();
  const auto [freeable_begin, freeable_end] = epoch_manager.ListFreeableEpoch();

  EXPECT_EQ(begin_epoch, freeable_begin);
  EXPECT_EQ(end_epoch, freeable_end);
}

}  // namespace gc::epoch
