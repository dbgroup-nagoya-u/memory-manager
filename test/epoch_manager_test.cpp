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
    epoch_manager = std::make_unique<EpochManager>();
  }

  void
  TearDown() override
  {
  }

  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  std::unique_ptr<EpochManager> epoch_manager;

  std::mutex mtx;
};

/*##################################################################################################
 * Unit test definitions
 *################################################################################################*/

TEST_F(EpochManagerFixture, GetGlobalEpochReference_AfterConstruct_GlobalEpochIsZero)
{
  EXPECT_EQ(0, epoch_manager->GetGlobalEpochReference().load());
}

TEST_F(EpochManagerFixture, ForwardGlobalEpoch_AfterConstruct_GlobalEpochUpdated)
{
  epoch_manager->ForwardGlobalEpoch();

  EXPECT_EQ(1, epoch_manager->GetCurrentEpoch());
}

TEST_F(EpochManagerFixture, GetEpoch_AfterConstruct_EpochCorrectlyCreated)
{
  const auto epoch = epoch_manager->GetEpoch();

  EXPECT_EQ(0, epoch->GetCurrentEpoch());
  EXPECT_EQ(kULMax, epoch->GetProtectedEpoch());
}

TEST_F(EpochManagerFixture, GetEpoch_WithForwardGlobalEpoch_EpochCorrectlyRefferGlobal)
{
  const auto epoch = epoch_manager->GetEpoch();
  epoch_manager->ForwardGlobalEpoch();

  EXPECT_EQ(1, epoch->GetCurrentEpoch());
}

TEST_F(EpochManagerFixture, GetProtectedEpoch_WithoutEpochs_GetCurrentEpoch)
{
  EXPECT_EQ(0, epoch_manager->GetProtectedEpoch());
}

TEST_F(EpochManagerFixture, GetProtectedEpoch_WithEnteredEpoch_GetEnteredEpoch)
{
  std::mutex mtx;

  // forward global epoch
  std::thread forwarder{[&]() {
    for (size_t i = 0; i < kLoopNum; ++i) {
      std::this_thread::sleep_for(std::chrono::milliseconds{1});
      epoch_manager->ForwardGlobalEpoch();
    }
  }};

  // create entered epochs
  std::vector<std::thread> threads;
  {
    const std::unique_lock<std::mutex> guard{mtx};
    for (size_t i = 0; i < 10; ++i) {
      threads.emplace_back([&]() {
        epoch_manager->GetEpoch()->EnterEpoch();
        const std::unique_lock<std::mutex> lock{mtx};
      });
    }

    forwarder.join();
    EXPECT_LT(epoch_manager->GetProtectedEpoch(), kLoopNum);
  }
  for (auto &&t : threads) t.join();
}

}  // namespace dbgroup::memory::component::test
