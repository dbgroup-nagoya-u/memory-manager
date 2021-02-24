// Copyright (c) DB Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#include "gc/tls_based/epoch_manager.hpp"

#include <gtest/gtest.h>

#include <thread>

namespace dbgroup::gc::tls
{
class EpochManagerFixture : public ::testing::Test
{
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

TEST_F(EpochManagerFixture, Construct_NoArgs_MemberVariablesCorrectlyInitialized)
{
  auto manager = EpochManager{};

  EXPECT_EQ(0, manager.GetCurrentEpoch());
}

TEST_F(EpochManagerFixture, ForwardEpoch_ForwardTenTimes_CurrentEpochCorrectlyUpdated)
{
  constexpr auto kLoopNum = 10UL;

  auto manager = EpochManager{};

  for (size_t count = 0; count < kLoopNum; ++count) {
    EXPECT_EQ(count, manager.GetCurrentEpoch());
    manager.ForwardEpoch();
  }

  EXPECT_EQ(kLoopNum, manager.GetCurrentEpoch());
}

}  // namespace dbgroup::gc::tls
