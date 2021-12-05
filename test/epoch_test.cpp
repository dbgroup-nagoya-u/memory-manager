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

#include "memory/component/epoch.hpp"

#include <memory>

#include "common.hpp"
#include "gtest/gtest.h"

namespace dbgroup::memory::component::test
{
class EpochFixture : public ::testing::Test
{
 protected:
  /*################################################################################################
   * constants
   *##############################################################################################*/

  static constexpr auto kULMax = ::dbgroup::memory::test::kULMax;

  /*################################################################################################
   * Test setup/teardown
   *##############################################################################################*/

  void
  SetUp() override
  {
    current_epoch_ = 0;
    epoch_ = std::make_unique<Epoch>(current_epoch_);
  }

  void
  TearDown() override
  {
  }

  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  std::atomic_size_t current_epoch_{};

  std::unique_ptr<Epoch> epoch_{};
};

/*##################################################################################################
 * Unit test definitions
 *################################################################################################*/

TEST_F(EpochFixture, ConstructorWithZeroEpochInitializeMemberVariablesCorrectly)
{
  EXPECT_EQ(0, epoch_->GetCurrentEpoch());
  EXPECT_EQ(kULMax, epoch_->GetProtectedEpoch());
}

TEST_F(EpochFixture, EnterEpochWithZeroEpochProtectEpoch)
{
  epoch_->EnterEpoch();

  EXPECT_EQ(0, epoch_->GetCurrentEpoch());
  EXPECT_EQ(0, epoch_->GetProtectedEpoch());
}

TEST_F(EpochFixture, LeaveEpochAfterEnteredUnprotectEpoch)
{
  epoch_->EnterEpoch();
  epoch_->LeaveEpoch();

  EXPECT_EQ(0, epoch_->GetCurrentEpoch());
  EXPECT_EQ(kULMax, epoch_->GetProtectedEpoch());
}

}  // namespace dbgroup::memory::component::test
