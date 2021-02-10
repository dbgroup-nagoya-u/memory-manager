// Copyright (c) DB Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#include "epoch_based/epoch_manager.hpp"

#include "gtest/gtest.h"

namespace gc::epoch
{
class EpochManagerFixture : public testing::Test
{
 public:
  EpochManager epoch_manger{};

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

TEST_F(EpochManagerFixture, Construct_NoArgument_MemberVariableCorrectlyInitialized)
{
  const auto current_epoch = epoch_manger.GetCurrentEpoch();

  EXPECT_EQ(0, current_epoch);

  const auto [begin_epoch, end_epoch] = epoch_manger.ListFreeableEpoch();

  EXPECT_EQ(0, begin_epoch);
  EXPECT_EQ(0, end_epoch);
}

}  // namespace gc::epoch
