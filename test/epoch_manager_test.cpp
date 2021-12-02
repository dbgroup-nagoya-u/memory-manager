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

#include "memory/component/epoch_manager.hpp"

#include <future>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "common.hpp"
#include "gtest/gtest.h"

namespace dbgroup::memory::component::test
{
class EpochManagerFixture : public ::testing::Test
{
 protected:
  /*################################################################################################
   * Internal constants
   *##############################################################################################*/

  static constexpr size_t kLoopNum = 100;
  static constexpr auto kULMax = ::dbgroup::memory::test::kULMax;

  /*################################################################################################
   * Test setup/teardown
   *##############################################################################################*/

  void
  SetUp() override
  {
    epoch_manager_ = std::make_unique<EpochManager>();
  }

  void
  TearDown() override
  {
  }

  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  std::unique_ptr<EpochManager> epoch_manager_{};  // NOLINT

  std::mutex mtx_{};  // NOLINT
};

/*##################################################################################################
 * Unit test definitions
 *################################################################################################*/

TEST_F(EpochManagerFixture, GetGlobalEpochReferenceAfterConstructGetZeroValue)
{
  EXPECT_EQ(0, epoch_manager_->GetGlobalEpochReference().load());
}

TEST_F(EpochManagerFixture, ForwardGlobalEpochAfterConstructGetIncrementedEpoch)
{
  epoch_manager_->ForwardGlobalEpoch();

  EXPECT_EQ(1, epoch_manager_->GetCurrentEpoch());
}

TEST_F(EpochManagerFixture, GetEpochAfterConstructCreateLocalEpoch)
{
  const auto *epoch = epoch_manager_->GetEpoch();

  EXPECT_EQ(0, epoch->GetCurrentEpoch());
  EXPECT_EQ(kULMax, epoch->GetProtectedEpoch());
}

TEST_F(EpochManagerFixture, GetEpochWithForwardEpochKeepRefferenceToGlobalEpoch)
{
  const auto *epoch = epoch_manager_->GetEpoch();
  epoch_manager_->ForwardGlobalEpoch();

  EXPECT_EQ(1, epoch->GetCurrentEpoch());
}

TEST_F(EpochManagerFixture, GetProtectedEpochWithoutEpochsGetCurrentEpoch)
{
  EXPECT_EQ(0, epoch_manager_->GetProtectedEpoch());
}

TEST_F(EpochManagerFixture, GetProtectedEpochWithEnteredEpochGetEnteredEpoch)
{
  constexpr size_t kRepeatNum = 10;

  // forward global epoch
  std::thread forwarder{[&]() {
    for (size_t i = 0; i < kLoopNum; ++i) {
      std::this_thread::sleep_for(std::chrono::milliseconds{1});
      epoch_manager_->ForwardGlobalEpoch();
    }
  }};

  // create entered epochs
  std::vector<std::thread> threads;
  {
    const std::unique_lock<std::mutex> guard{mtx_};
    for (size_t i = 0; i < kRepeatNum; ++i) {
      threads.emplace_back([&]() {
        epoch_manager_->GetEpoch()->EnterEpoch();
        const std::unique_lock<std::mutex> lock{mtx_};
      });
    }

    forwarder.join();
    EXPECT_LT(epoch_manager_->GetProtectedEpoch(), kLoopNum);
  }
  for (auto &&t : threads) t.join();
}

}  // namespace dbgroup::memory::component::test
