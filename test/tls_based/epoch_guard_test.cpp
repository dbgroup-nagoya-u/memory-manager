// Copyright (c) DB Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#include "gc/tls_based/epoch_guard.hpp"

#include <gtest/gtest.h>

namespace dbgroup::gc::tls
{
class EpochGuardFixture : public ::testing::Test
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

TEST_F(EpochGuardFixture, Construct_CurrentEpochZero_ProtectedEpochCorrectlyUpdated)
{
  auto epoch = Epoch{0};
  const auto guard = EpochGuard{&epoch};

  EXPECT_EQ(0, epoch.GetProtectedEpoch());
}

TEST_F(EpochGuardFixture, Destruct_CurrentEpochZero_ProtectedEpochCorrectlyUpdated)
{
  auto epoch = Epoch{0};

  {
    const auto guard = EpochGuard{&epoch};
  }

  EXPECT_EQ(std::numeric_limits<size_t>::max(), epoch.GetProtectedEpoch());
}

}  // namespace dbgroup::gc::tls
