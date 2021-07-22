/*
 * Copyright 2021 Database Group, Nagoya University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "memory/component/epoch_guard.hpp"

#include "gtest/gtest.h"

namespace dbgroup::memory::component::test
{
class EpochGuardFixture : public ::testing::Test
{
 protected:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  std::atomic_size_t current_epoch;

  Epoch epoch{current_epoch};

  /*################################################################################################
   * Test setup/teardown
   *##############################################################################################*/

  void
  SetUp() override
  {
    current_epoch.store(0);
  }

  void
  TearDown() override
  {
  }
};

/*##################################################################################################
 * Unit test definitions
 *################################################################################################*/

TEST_F(EpochGuardFixture, Construct_CurrentEpochZero_ProtectedEpochCorrectlyUpdated)
{
  const auto guard = EpochGuard{epoch};

  EXPECT_EQ(0, epoch.GetProtectedEpoch());
}

TEST_F(EpochGuardFixture, Destruct_CurrentEpochZero_ProtectedEpochCorrectlyUpdated)
{
  {
    const auto guard = EpochGuard{epoch};
  }

  EXPECT_EQ(std::numeric_limits<size_t>::max(), epoch.GetProtectedEpoch());
}

}  // namespace dbgroup::memory::component::test
