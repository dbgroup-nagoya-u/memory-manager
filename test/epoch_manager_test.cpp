// Copyright (c) DB Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#include "memory/manager/component/epoch_manager.hpp"

#include <gtest/gtest.h>

#include <future>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

namespace dbgroup::memory::manager::component
{
using EpochPairs = std::pair<std::vector<Epoch>, std::vector<std::shared_ptr<uint64_t>>>;

class EpochManagerFixture : public ::testing::Test
{
 public:
  static constexpr size_t kLoopNum = 10;

  std::mutex mtx;

  void
  CreateEpoch(  //
      std::promise<std::pair<Epoch *, std::shared_ptr<uint64_t>>> p,
      EpochManager *manager)
  {
    thread_local auto epoch = Epoch{manager->GetCurrentEpoch()};
    thread_local auto epoch_keeper = std::make_shared<uint64_t>(0);

    manager->RegisterEpoch(&epoch, epoch_keeper);

    const auto guard = EpochGuard{&epoch};

    // return epoch and its reference
    p.set_value(std::make_pair(&epoch, epoch_keeper));

    // wait until a main thread finishes tests
    const auto lock = std::unique_lock<std::mutex>(mtx);
  }

  void
  TestUpdateRegisteredEpochs(const size_t thread_num)
  {
    auto manager = EpochManager{};
    std::vector<std::weak_ptr<uint64_t>> epoch_keepers;

    {
      // create a lock to prevent destruction of epochs
      auto lock = std::unique_lock<std::mutex>(mtx);

      // create threads and gather their epochs
      std::vector<Epoch *> epochs;
      std::vector<std::future<std::pair<Epoch *, std::shared_ptr<uint64_t>>>> futures;
      for (size_t i = 0; i < thread_num; ++i) {
        std::promise<std::pair<Epoch *, std::shared_ptr<uint64_t>>> p;
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
        manager.ForwardGlobalEpoch();
        const auto protected_epoch = manager.UpdateRegisteredEpochs();
        EXPECT_EQ(0, protected_epoch);
      }

      // release a lock
    }

    // wait all the threads leave
    bool all_thread_exit;
    do {
      all_thread_exit = true;
      for (auto &&epoch_keeper : epoch_keepers) {
        all_thread_exit &= epoch_keeper.expired();
      }
    } while (!all_thread_exit);

    // there is no protecting epoch
    EXPECT_EQ(std::numeric_limits<size_t>::max(), manager.UpdateRegisteredEpochs());
  }

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

TEST_F(EpochManagerFixture, Destruct_AfterRegisterOneEpoch_RegisteredEpochFreed)
{
  std::weak_ptr<uint64_t> epoch_reference;

  {
    auto manager = EpochManager{};
    auto epoch_keeper = std::make_shared<uint64_t>(0);
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
  std::vector<std::weak_ptr<uint64_t>> epoch_references;

  {
    auto manager = EpochManager{};

    for (size_t count = 0; count < kLoopNum; ++count) {
      // register an epoch to a manager
      auto epoch_keeper = std::make_shared<uint64_t>(0);
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

}  // namespace dbgroup::memory::manager::component