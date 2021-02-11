// Copyright (c) DB Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#include "epoch_based/epoch_guard.hpp"

#include <thread>
#include <vector>

#include "epoch_based/epoch_manager.hpp"
#include "gtest/gtest.h"

namespace gc::epoch
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

}  // namespace gc::epoch
