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
  using Target = uint64_t;
  using GarbageList_t = GarbageList<std::shared_ptr<Target>>;

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

TEST_F(GarbageListFixture, Construct_WithoutArgs_MemberVariablesCorrectlyInitialized)
{
  auto garbage_list = GarbageList_t{};

  EXPECT_EQ(0, garbage_list.Size());
}

TEST_F(GarbageListFixture, Destruct_AddLessGarbages_AddedGarbagesCorrectlyFreed)
{
  constexpr size_t kGarbageNum = kGarbageBufferSize * 0.5;

  std::vector<std::weak_ptr<Target>> garbage_references;

  {
    auto garbage_list = GarbageList_t{};
    for (size_t count = 0; count < kGarbageNum; ++count) {
      auto garbage = new std::shared_ptr<Target>{new Target{0}};
      GarbageList_t::AddGarbage(&garbage_list, 0, garbage);
      garbage_references.emplace_back(*garbage);
    }

    // garbages are deleted in destructor
  }

  for (auto&& garbage_reference : garbage_references) {
    EXPECT_TRUE(garbage_reference.expired());
  }
}

TEST_F(GarbageListFixture, Destruct_AddLotOfGarbages_AddedGarbagesCorrectlyFreed)
{
  constexpr size_t kGarbageNum = kGarbageBufferSize * 1.5;

  std::vector<std::weak_ptr<Target>> garbage_references;

  {
    auto garbage_list = GarbageList_t{};
    GarbageList_t* current_list = &garbage_list;
    for (size_t count = 0; count < kGarbageNum; ++count) {
      auto garbage = new std::shared_ptr<Target>{new Target{0}};
      current_list = GarbageList_t::AddGarbage(current_list, 0, garbage);
      garbage_references.emplace_back(*garbage);
    }

    // garbages are deleted in destructor
    delete current_list;
  }

  for (auto&& garbage_reference : garbage_references) {
    EXPECT_TRUE(garbage_reference.expired());
  }
}

TEST_F(GarbageListFixture, AddGarbage_LessGarbages_ListSizeCorrectlyUpdated)
{
  constexpr size_t kGarbageNum = kGarbageBufferSize * 0.5;

  auto garbage_list = GarbageList<Target>{};
  for (size_t count = 0; count < kGarbageNum; ++count) {
    auto garbage = new Target{0};
    GarbageList<Target>::AddGarbage(&garbage_list, 0, garbage);
  }

  EXPECT_EQ(kGarbageNum, garbage_list.Size());
}

TEST_F(GarbageListFixture, AddGarbage_LotOfGarbages_ListSizeCorrectlyUpdated)
{
  constexpr size_t kGarbageNum = kGarbageBufferSize * 1.5;

  auto garbage_list = GarbageList<Target>{};
  GarbageList<Target>* current_list = &garbage_list;
  for (size_t count = 0; count < kGarbageNum; ++count) {
    auto garbage = new Target{0};
    current_list = GarbageList<Target>::AddGarbage(current_list, 0, garbage);
  }

  EXPECT_EQ(kGarbageNum, garbage_list.Size());

  delete current_list;
}

TEST_F(GarbageListFixture, Clear_WithLessGarbages_ProtectedGarbagesRemain)
{
  constexpr size_t kTotalGarbageNum = kGarbageBufferSize / 2;
  constexpr size_t kHalfGarbageNum = kTotalGarbageNum / 2;

  auto garbage_list = GarbageList_t{};
  std::vector<std::weak_ptr<Target>> garbage_references;
  const size_t protected_epoch = 1;

  // add unprotected garbages
  size_t count = 0;
  for (; count < kHalfGarbageNum; ++count) {
    auto garbage = new std::shared_ptr<Target>{new Target{0}};
    GarbageList_t::AddGarbage(&garbage_list, 0, garbage);
    garbage_references.emplace_back(*garbage);
  }

  // add protected garbages
  for (; count < kTotalGarbageNum; ++count) {
    auto garbage = new std::shared_ptr<Target>{new Target{0}};
    GarbageList_t::AddGarbage(&garbage_list, protected_epoch, garbage);
    garbage_references.emplace_back(*garbage);
  }

  GarbageList_t::Clear(&garbage_list, protected_epoch);

  EXPECT_EQ(kTotalGarbageNum - kHalfGarbageNum, garbage_list.Size());
  for (count = 0; count < kTotalGarbageNum; ++count) {
    if (count < kHalfGarbageNum) {
      EXPECT_TRUE(garbage_references[count].expired());
    } else {
      EXPECT_FALSE(garbage_references[count].expired());
    }
  }
}

TEST_F(GarbageListFixture, Clear_WithLotOfGarbages_ProtectedGarbagesRemain)
{
  constexpr size_t kTotalGarbageNum = kGarbageBufferSize * 1.5;
  constexpr size_t kHalfGarbageNum = kTotalGarbageNum / 2;

  auto garbage_list = GarbageList_t{};
  GarbageList_t* current_list = &garbage_list;
  std::vector<std::weak_ptr<Target>> garbage_references;
  const size_t protected_epoch = 1;

  // add unprotected garbages
  size_t count = 0;
  for (; count < kHalfGarbageNum; ++count) {
    auto garbage = new std::shared_ptr<Target>{new Target{0}};
    current_list = GarbageList_t::AddGarbage(current_list, 0, garbage);
    garbage_references.emplace_back(*garbage);
  }

  // add protected garbages
  for (; count < kTotalGarbageNum; ++count) {
    auto garbage = new std::shared_ptr<Target>{new Target{0}};
    current_list = GarbageList_t::AddGarbage(current_list, protected_epoch, garbage);
    garbage_references.emplace_back(*garbage);
  }

  GarbageList_t::Clear(&garbage_list, protected_epoch);

  EXPECT_EQ(kTotalGarbageNum - kHalfGarbageNum, garbage_list.Size());
  for (count = 0; count < kTotalGarbageNum; ++count) {
    if (count < kHalfGarbageNum) {
      EXPECT_TRUE(garbage_references[count].expired());
    } else {
      EXPECT_FALSE(garbage_references[count].expired());
    }
  }

  delete current_list;
}

}  // namespace dbgroup::memory::manager::component
