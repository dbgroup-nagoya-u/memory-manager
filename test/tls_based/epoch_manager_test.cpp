// Copyright (c) DB Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#include "gc/tls_based/epoch_manager.hpp"

#include <gtest/gtest.h>

#include <future>
#include <memory>
#include <thread>
#include <vector>

namespace dbgroup::memory::tls_based
{
using EpochPairs = std::pair<std::vector<Epoch>, std::vector<std::shared_ptr<uint64_t>>>;

class EpochManagerFixture : public ::testing::Test
{
 public:
  static EpochPairs
  RegisterEpochs(  //
      const size_t num_epoch,
      EpochManager &manager)
  {
    std::vector<Epoch> epochs;
    std::vector<std::shared_ptr<uint64_t>> epoch_keepers;

    for (size_t count = 0; count < num_epoch; ++count) {
      // register an epoch to a manager
      auto epoch_keeper = std::make_shared<uint64_t>(0);
      auto epoch = Epoch{manager.GetCurrentEpoch()};
      manager.RegisterEpoch(&epoch, epoch_keeper);

      // keep a created epoch
      epochs.emplace_back(std::move(epoch));
      epoch_keepers.emplace_back(std::move(epoch_keeper));
    }

    return {std::move(epochs), std::move(epoch_keepers)};
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
  constexpr auto kLoopNum = 10UL;

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
  constexpr auto kLoopNum = 10UL;

  auto manager = EpochManager{};

  for (size_t count = 0; count < kLoopNum; ++count) {
    EXPECT_EQ(count, manager.GetCurrentEpoch());
    manager.ForwardGlobalEpoch();
  }

  EXPECT_EQ(kLoopNum, manager.GetCurrentEpoch());
}

TEST_F(EpochManagerFixture, UpdateRegisteredEpochs_SingleThread_RegisteredEpochsCorrectlyUpdated)
{
  constexpr auto kLoopNum = 10UL;

  auto manager = EpochManager{};
  std::vector<std::weak_ptr<uint64_t>> epoch_references;

  {
    // prepare epochs
    auto [epochs, epoch_keepers] = RegisterEpochs(kLoopNum, manager);
    for (auto &&epoch : epochs) {
      epoch.EnterEpoch();
    }
    for (auto &&epoch_keeper : epoch_keepers) {
      epoch_references.emplace_back(epoch_keeper);
    }

    // update epoch infomation
    manager.ForwardGlobalEpoch();
    const auto protected_epoch = manager.UpdateRegisteredEpochs();

    EXPECT_EQ(0, protected_epoch);
    for (auto &&epoch : epochs) {
      EXPECT_EQ(1, epoch.GetCurrentEpoch());
      epoch.LeaveEpoch();
    }

    // epochs become out of scope here
  }

  // add a dummy epoch to delete registered epochs
  auto dummy_keeper = std::make_shared<uint64_t>(0);
  auto dummy_epoch = Epoch{manager.GetCurrentEpoch()};
  manager.RegisterEpoch(&dummy_epoch, dummy_keeper);
  const auto protected_epoch = manager.UpdateRegisteredEpochs();

  EXPECT_EQ(std::numeric_limits<size_t>::max(), protected_epoch);
  for (auto &&epoch_reference : epoch_references) {
    EXPECT_EQ(0, epoch_reference.use_count());
  }
}

TEST_F(EpochManagerFixture, UpdateRegisteredEpochs_MultiThreads_RegisteredEpochsCorrectlyUpdated)
{
  constexpr auto kLoopNum = 10UL;
  constexpr auto kThreadNum = 100UL;

  auto manager = EpochManager{};
  std::vector<std::weak_ptr<uint64_t>> epoch_references;

  {
    // a lambda functions for a multi-threads test
    auto f = [kLoopNum, &manager](std::promise<EpochPairs> p) {
      p.set_value(RegisterEpochs(kLoopNum, manager));
    };

    // prepare epochs
    std::vector<Epoch> epochs;
    std::vector<std::shared_ptr<uint64_t>> epoch_keepers;
    std::vector<std::future<EpochPairs>> futures;
    for (size_t count = 0; count < kThreadNum; ++count) {
      std::promise<EpochPairs> p;
      futures.emplace_back(p.get_future());
      auto t = std::thread{f, std::move(p)};
      t.detach();
    }
    for (auto &&future : futures) {
      auto [f_epochs, f_keepers] = future.get();
      for (auto &&epoch : f_epochs) {
        epochs.emplace_back(std::move(epoch));
      }
      epoch_keepers.insert(epoch_keepers.end(), f_keepers.begin(), f_keepers.end());
    }

    // enter protected regions and keep references
    for (auto &&epoch : epochs) {
      epoch.EnterEpoch();
    }
    for (auto &&epoch_keeper : epoch_keepers) {
      epoch_references.emplace_back(epoch_keeper);
    }

    // update epoch infomation
    manager.ForwardGlobalEpoch();
    const auto protected_epoch = manager.UpdateRegisteredEpochs();

    EXPECT_EQ(0, protected_epoch);
    for (auto &&epoch : epochs) {
      EXPECT_EQ(1, epoch.GetCurrentEpoch());
      epoch.LeaveEpoch();
    }

    // epochs become out of scope here
  }

  // add a dummy epoch to delete registered epochs
  auto dummy_keeper = std::make_shared<uint64_t>(0);
  auto dummy_epoch = Epoch{manager.GetCurrentEpoch()};
  manager.RegisterEpoch(&dummy_epoch, dummy_keeper);
  const auto protected_epoch = manager.UpdateRegisteredEpochs();

  EXPECT_EQ(std::numeric_limits<size_t>::max(), protected_epoch);
  for (auto &&epoch_reference : epoch_references) {
    EXPECT_EQ(0, epoch_reference.use_count());
  }
}

}  // namespace dbgroup::memory::tls_based
