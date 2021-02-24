// Copyright (c) DB Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#include "gc/tls_based/epoch_manager.hpp"

#include <gtest/gtest.h>

#include <memory>
#include <thread>
#include <vector>

namespace dbgroup::gc::tls
{
class EpochManagerFixture : public ::testing::Test
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

TEST_F(EpochManagerFixture, Construct_NoArgs_MemberVariablesCorrectlyInitialized)
{
  auto manager = EpochManager{};

  EXPECT_EQ(0, manager.GetCurrentEpoch());
}

TEST_F(EpochManagerFixture, Destruct_AfterRegisterOneEpoch_RegisteredEpochFreed)
{
  std::weak_ptr<Epoch> epoch_reference;

  {
    auto manager = EpochManager{};
    const auto epoch = std::make_shared<Epoch>(manager.GetCurrentEpoch());

    // register an epoch to a manager
    manager.RegisterEpoch(epoch);

    // keep the reference to an epoch
    epoch_reference = epoch;

    // the created epoch is freed here because all the shared_ptr are out of scope
  }

  EXPECT_EQ(0, epoch_reference.use_count());
}

TEST_F(EpochManagerFixture, Destruct_AfterRegisterTenEpochs_RegisteredEpochsFreed)
{
  constexpr auto kLoopNum = 10UL;

  std::vector<std::weak_ptr<Epoch>> epoch_references;

  {
    auto manager = EpochManager{};

    for (size_t count = 0; count < kLoopNum; ++count) {
      // register an epoch to a manager
      const auto epoch = std::make_shared<Epoch>(manager.GetCurrentEpoch());
      manager.RegisterEpoch(epoch);

      // keep the reference to an epoch
      epoch_references.emplace_back(epoch);
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

}  // namespace dbgroup::gc::tls
