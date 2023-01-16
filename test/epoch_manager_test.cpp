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

#include "memory/epoch_manager.hpp"

#include <future>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "common.hpp"
#include "gtest/gtest.h"

namespace dbgroup::memory::test
{
class EpochManagerFixture : public ::testing::Test
{
 protected:
  /*####################################################################################
   * Internal constants
   *##################################################################################*/

  static constexpr auto kULMax = ::dbgroup::memory::test::kULMax;

  /*####################################################################################
   * Test setup/teardown
   *##################################################################################*/

  void
  SetUp() override
  {
    epoch_manager_ = std::make_unique<EpochManager>();
  }

  void
  TearDown() override
  {
  }

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  std::unique_ptr<EpochManager> epoch_manager_{};

  std::mutex mtx_{};
};

/*######################################################################################
 * Unit test definitions
 *####################################################################################*/

TEST_F(EpochManagerFixture, ForwardGlobalEpochAfterConstructGetIncrementedEpoch)
{
  epoch_manager_->ForwardGlobalEpoch();

  EXPECT_EQ(1, epoch_manager_->GetCurrentEpoch());
}

TEST_F(EpochManagerFixture, GetEpochWithForwardEpochKeepReferenceToGlobalEpoch)
{
  epoch_manager_->ForwardGlobalEpoch();

  EXPECT_EQ(1, epoch_manager_->GetCurrentEpoch());
}

TEST_F(EpochManagerFixture, GetProtectedEpochWithoutEpochsGetCurrentEpoch)
{
  EXPECT_EQ(0, epoch_manager_->GetMinEpoch());

  const auto &protected_epochs = epoch_manager_->GetProtectedEpochs();
  EXPECT_NE(protected_epochs, nullptr);
  EXPECT_EQ(protected_epochs->size(), 1);
  EXPECT_EQ(protected_epochs->front(), 0);
}

TEST_F(EpochManagerFixture, GetProtectedEpochWithEnteredEpochGetEnteredEpoch)
{
  constexpr size_t kLoopNum = 1000;
  constexpr size_t kRepeatNum = 10;

  // forward global epoch
  std::thread forwarder{[&]() {
    for (size_t i = 0; i < kLoopNum; ++i) {
      std::this_thread::sleep_for(std::chrono::microseconds{1});
      epoch_manager_->ForwardGlobalEpoch();
    }
  }};

  // create entered epochs
  std::vector<std::thread> threads;
  {
    const std::unique_lock<std::mutex> guard{mtx_};
    for (size_t i = 0; i < kRepeatNum; ++i) {
      threads.emplace_back([&]() {
        [[maybe_unused]] const auto &guard = epoch_manager_->CreateEpochGuard();
        const std::unique_lock<std::mutex> lock{mtx_};
      });
    }

    forwarder.join();

    const auto &protected_epochs = epoch_manager_->GetProtectedEpochs();
    for (size_t i = 0; i < protected_epochs->size() - 1; ++i) {
      EXPECT_GT(protected_epochs->at(i), protected_epochs->at(i + 1));
    }
    EXPECT_EQ(protected_epochs->front(), epoch_manager_->GetCurrentEpoch());
    EXPECT_EQ(protected_epochs->back(), epoch_manager_->GetMinEpoch());
  }
  for (auto &&t : threads) t.join();
}

}  // namespace dbgroup::memory::test
