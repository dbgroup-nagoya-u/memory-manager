// Copyright (c) DB Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#include "gc/tls_based/epoch.hpp"

#include <gtest/gtest.h>

namespace dbgroup::memory::tls_based
{
class EpochFixture : public ::testing::Test
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

TEST_F(EpochFixture, Construct_CurrentEpochZero_MemberVariableCorrectlyInitialized)
{
  const auto epoch = Epoch{0};

  EXPECT_EQ(0, epoch.GetCurrentEpoch());
  EXPECT_EQ(std::numeric_limits<size_t>::max(), epoch.GetProtectedEpoch());
}

TEST_F(EpochFixture, SetCurrentEpoch_SetOne_CurrentEpochCorrectlyUpdated)
{
  auto epoch = Epoch{0};
  epoch.SetCurrentEpoch(1);

  EXPECT_EQ(1, epoch.GetCurrentEpoch());
}

TEST_F(EpochFixture, EnterEpoch_CurrentZero_ProtectedEpochCorrectlyUpdated)
{
  auto epoch = Epoch{0};
  epoch.EnterEpoch();

  EXPECT_EQ(0, epoch.GetProtectedEpoch());
}

TEST_F(EpochFixture, LeaveEpoch_AfterEntered_ProtectedEpochCorrectlyUpdated)
{
  auto epoch = Epoch{0};
  epoch.EnterEpoch();
  epoch.LeaveEpoch();

  EXPECT_EQ(std::numeric_limits<size_t>::max(), epoch.GetProtectedEpoch());
}

}  // namespace dbgroup::memory::tls_based
