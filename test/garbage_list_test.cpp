// Copyright (c) DB Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#include "memory/manager/component/garbage_list.hpp"

#include <gtest/gtest.h>

#include <future>
#include <memory>
#include <thread>
#include <vector>

namespace dbgroup::memory::manager::component
{
class GarbageListFixture : public ::testing::Test
{
 public:
  static constexpr size_t kBufferSize = 256;
  static constexpr size_t kGCInterval = 100;

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

TEST_F(GarbageListFixture, Construct_WithArgs_MemberVariablesCorrectlyInitialized)
{
  auto garbage_list = GarbageList<size_t>{kBufferSize, 0, kGCInterval};

  EXPECT_EQ(0, garbage_list.Size());
}

TEST_F(GarbageListFixture, Destruct_AddTenGarbages_AddedGarbagesCorrectlyFreed)
{
  constexpr auto kGarbageNum = 10UL;

  std::vector<std::weak_ptr<size_t>> garbage_references;

  {
    auto garbage_list = GarbageList<std::shared_ptr<size_t>>{kBufferSize, 0, kGCInterval};
    for (size_t count = 0; count < kGarbageNum; ++count) {
      auto garbage = new std::shared_ptr<size_t>{new size_t{0}};
      garbage_list.AddGarbage(garbage);
      garbage_references.emplace_back(*garbage);
    }
  }

  for (auto &&garbage_reference : garbage_references) {
    EXPECT_EQ(0, garbage_reference.use_count());
  }
}

TEST_F(GarbageListFixture, AddGarbage_TenGarbages_ListSizeCorrectlyUpdated)
{
  constexpr auto kGarbageNum = 10UL;

  auto garbage_list = GarbageList<size_t>{kBufferSize, 0, kGCInterval};
  for (size_t count = 0; count < kGarbageNum; ++count) {
    auto garbage = new size_t{0};
    garbage_list.AddGarbage(garbage);
  }

  EXPECT_EQ(kGarbageNum, garbage_list.Size());
}

TEST_F(GarbageListFixture, Clear_TenFreeableTenProtectedGarbages_ProtectedGarbagesRemain)
{
  constexpr auto kGarbageNum = 10UL;

  auto garbage_list = GarbageList<std::shared_ptr<size_t>>{kBufferSize, 0, kGCInterval};
  std::vector<std::weak_ptr<size_t>> garbage_references;
  const auto protected_epoch = 1UL;

  // add unprotected garbages
  for (size_t count = 0; count < kGarbageNum; ++count) {
    auto garbage = new std::shared_ptr<size_t>{new size_t{0}};
    garbage_list.AddGarbage(garbage);
    garbage_references.emplace_back(*garbage);
  }

  garbage_list.SetCurrentEpoch(protected_epoch);

  // add protected garbages
  for (size_t count = 0; count < kGarbageNum; ++count) {
    auto garbage = new std::shared_ptr<size_t>{new size_t{0}};
    garbage_list.AddGarbage(garbage);
    garbage_references.emplace_back(*garbage);
  }

  garbage_list.Clear(protected_epoch);

  EXPECT_EQ(kGarbageNum, garbage_list.Size());
  for (size_t count = 0; count < 2 * kGarbageNum; ++count) {
    if (count < kGarbageNum) {
      EXPECT_EQ(0, garbage_references[count].use_count());
    } else {
      EXPECT_EQ(1, garbage_references[count].use_count());
    }
  }
}

TEST_F(GarbageListFixture, AddGarbage_AddManyGarbages_RingBufferReturnHead)
{
  constexpr auto kGarbageNum = kBufferSize / 2 + 10;

  auto garbage_list = GarbageList<std::shared_ptr<size_t>>{kBufferSize, 0, kGCInterval};
  std::vector<std::weak_ptr<size_t>> garbage_references;
  const auto protected_epoch = 1UL;

  // add unprotected garbages
  for (size_t count = 0; count < kGarbageNum; ++count) {
    auto garbage = new std::shared_ptr<size_t>{new size_t{0}};
    garbage_list.AddGarbage(garbage);
    garbage_references.emplace_back(*garbage);
  }

  garbage_list.SetCurrentEpoch(protected_epoch);
  garbage_list.Clear(protected_epoch);  // clear garbages to add remaining ones

  // add protected garbages
  for (size_t count = 0; count < kGarbageNum; ++count) {
    auto garbage = new std::shared_ptr<size_t>{new size_t{0}};
    garbage_list.AddGarbage(garbage);
    garbage_references.emplace_back(*garbage);
  }

  EXPECT_EQ(kGarbageNum, garbage_list.Size());
  for (size_t count = 0; count < 2 * kGarbageNum; ++count) {
    if (count < kGarbageNum) {
      EXPECT_EQ(0, garbage_references[count].use_count());
    } else {
      EXPECT_EQ(1, garbage_references[count].use_count());
    }
  }
}

}  // namespace dbgroup::memory::manager::component
