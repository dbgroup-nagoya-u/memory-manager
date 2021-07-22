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

#include "gtest/gtest.h"

namespace dbgroup::memory::component::test
{
class EpochManagerFixture : public ::testing::Test
{
 protected:
  using EpochPairs = std::pair<std::vector<Epoch>, std::vector<std::shared_ptr<std::atomic_bool>>>;

  /*################################################################################################
   * Internal constants
   *##############################################################################################*/

  static constexpr size_t kLoopNum = 10;

  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  std::mutex mtx;

  /*################################################################################################
   * Test setup/teardown
   *##############################################################################################*/

  void
  SetUp() override
  {
  }

  void
  TearDown() override
  {
  }

  /*################################################################################################
   * Internal utility functions
   *##############################################################################################*/

  void
  CreateEpoch(  //
      std::promise<std::pair<Epoch *, std::shared_ptr<std::atomic_bool>>> p,
      EpochManager *manager)
  {
    thread_local auto epoch = Epoch{manager->GetCurrentEpoch()};
    thread_local auto epoch_keeper = std::make_shared<std::atomic_bool>(true);

    manager->RegisterEpoch(&epoch, epoch_keeper);

    const auto guard = EpochGuard{epoch};

    // return epoch and its reference
    p.set_value(std::make_pair(&epoch, epoch_keeper));

    // wait until a main thread finishes tests
    const auto lock = std::unique_lock<std::mutex>(mtx);
  }

  void
  TestUpdateRegisteredEpochs(const size_t thread_num)
  {
    auto manager = EpochManager{};
    std::vector<std::weak_ptr<std::atomic_bool>> epoch_keepers;

    {
      // create a lock to prevent destruction of epochs
      auto lock = std::unique_lock<std::mutex>(mtx);

      // create threads and gather their epochs
      std::vector<Epoch *> epochs;
      std::vector<std::future<std::pair<Epoch *, std::shared_ptr<std::atomic_bool>>>> futures;
      for (size_t i = 0; i < thread_num; ++i) {
        std::promise<std::pair<Epoch *, std::shared_ptr<std::atomic_bool>>> p;
        futures.emplace_back(p.get_future());
        std::thread{&EpochManagerFixture::CreateEpoch, this, std::move(p), &manager}.detach();
      }
      for (auto &&future : futures) {
        auto [epoch, epoch_keeper] = future.get();
        epochs.emplace_back(epoch);
        epoch_keepers.emplace_back(epoch_keeper);
      }

      // check whether epochs are correclty updated
      for (size_t i = 0; i < kLoopNum; ++i) {
        for (auto &&epoch : epochs) {
          EXPECT_EQ(i, epoch->GetCurrentEpoch());
        }
        const auto current_epoch = manager.ForwardGlobalEpoch();
        const auto protected_epoch = manager.UpdateRegisteredEpochs(current_epoch);
        EXPECT_EQ(0, protected_epoch);
      }

      // release a lock
    }

    // wait all the threads leave
    bool all_thread_exit;
    do {
      all_thread_exit = true;
      for (auto &&epoch_keeper : epoch_keepers) {
        all_thread_exit &= epoch_keeper.use_count() == 1;
      }
    } while (!all_thread_exit);

    // there is no protecting epoch
    EXPECT_EQ(std::numeric_limits<size_t>::max(), manager.UpdateRegisteredEpochs(0));
  }
};

/*##################################################################################################
 * Unit test definitions
 *################################################################################################*/

TEST_F(EpochManagerFixture, Construct_NoArgs_MemberVariablesCorrectlyInitialized)
{
  auto manager = EpochManager{};

  EXPECT_EQ(0, manager.GetCurrentEpoch());
}

TEST_F(EpochManagerFixture, Destruct_AfterRegisterOneEpoch_RegisteredEpochFreed)
{
  std::weak_ptr<std::atomic_bool> epoch_reference;

  {
    auto manager = EpochManager{};
    auto epoch_keeper = std::make_shared<std::atomic_bool>(true);
    auto epoch = Epoch{manager.GetCurrentEpoch()};

    // register an epoch to a manager
    manager.RegisterEpoch(&epoch, epoch_keeper);

    // keep the reference to an epoch
    epoch_reference = epoch_keeper;

    // the created epoch is freed here because all the shared_ptr are out of scope
  }

  EXPECT_EQ(0, epoch_reference.use_count());
}

TEST_F(EpochManagerFixture, Destruct_AfterRegisterTenEpochs_RegisteredEpochsFreed)
{
  std::vector<std::weak_ptr<std::atomic_bool>> epoch_references;

  {
    auto manager = EpochManager{};

    for (size_t count = 0; count < kLoopNum; ++count) {
      // register an epoch to a manager
      auto epoch_keeper = std::make_shared<std::atomic_bool>(true);
      auto epoch = Epoch{manager.GetCurrentEpoch()};
      manager.RegisterEpoch(&epoch, epoch_keeper);

      // keep the reference to an epoch
      epoch_references.emplace_back(epoch_keeper);
    }

    // the created epochs are freed here because all the shared_ptr are out of scope
  }

  for (auto &&epoch_reference : epoch_references) {
    EXPECT_EQ(0, epoch_reference.use_count());
  }
}

TEST_F(EpochManagerFixture, ForwardGlobalEpoch_ForwardTenTimes_CurrentEpochCorrectlyUpdated)
{
  auto manager = EpochManager{};

  for (size_t count = 0; count < kLoopNum; ++count) {
    EXPECT_EQ(count, manager.GetCurrentEpoch());
    manager.ForwardGlobalEpoch();
  }

  EXPECT_EQ(kLoopNum, manager.GetCurrentEpoch());
}

TEST_F(EpochManagerFixture, UpdateRegisteredEpochs_SingleThread_RegisteredEpochsCorrectlyUpdated)
{
  constexpr size_t kThreadNum = 1;

  TestUpdateRegisteredEpochs(kThreadNum);
}

TEST_F(EpochManagerFixture, UpdateRegisteredEpochs_MultiThreads_RegisteredEpochsCorrectlyUpdated)
{
  constexpr size_t kThreadNum = 100;

  TestUpdateRegisteredEpochs(kThreadNum);
}

}  // namespace dbgroup::memory::component::test
