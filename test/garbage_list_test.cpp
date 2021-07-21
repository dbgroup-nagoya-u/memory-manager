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
 protected:
  using Target = uint64_t;
  using GarbageList_t = GarbageList<std::shared_ptr<Target>>;

  /*################################################################################################
   * Internal constants
   *##############################################################################################*/

  static constexpr size_t kBufferSize = 256;
  static constexpr size_t kGCInterval = 100;

  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  size_t current_epoch;

  /*################################################################################################
   * Test setup/teardown
   *##############################################################################################*/

  void
  SetUp() override
  {
    current_epoch = 0;
  }

  void
  TearDown() override
  {
  }
};

/*##################################################################################################
 * Unit test definitions
 *################################################################################################*/

TEST_F(GarbageListFixture, Construct_WithoutArgs_MemberVariablesCorrectlyInitialized)
{
  auto garbage_list = GarbageList_t{current_epoch};

  EXPECT_EQ(0, garbage_list.Size());
}

TEST_F(GarbageListFixture, Destruct_AddLessGarbages_AddedGarbagesCorrectlyFreed)
{
  constexpr size_t kGarbageNum = kGarbageBufferSize * 0.5;

  std::vector<std::weak_ptr<Target>> garbage_references;

  {
    auto garbage_list = GarbageList_t{current_epoch};
    for (size_t count = 0; count < kGarbageNum; ++count) {
      auto garbage = New<std::shared_ptr<Target>>(new Target{0});
      GarbageList_t::AddGarbage(&garbage_list, garbage);
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
    auto garbage_list = GarbageList_t{current_epoch};
    GarbageList_t* current_list = &garbage_list;
    for (size_t count = 0; count < kGarbageNum; ++count) {
      auto garbage = New<std::shared_ptr<Target>>(new Target{0});
      current_list = GarbageList_t::AddGarbage(current_list, garbage);
      garbage_references.emplace_back(*garbage);
    }

    // garbages are deleted in destructor
    Delete(current_list);
  }

  for (auto&& garbage_reference : garbage_references) {
    EXPECT_TRUE(garbage_reference.expired());
  }
}

TEST_F(GarbageListFixture, AddGarbage_LessGarbages_ListSizeCorrectlyUpdated)
{
  constexpr size_t kGarbageNum = kGarbageBufferSize * 0.5;

  auto garbage_list = GarbageList<Target>{current_epoch};
  for (size_t count = 0; count < kGarbageNum; ++count) {
    auto garbage = New<Target>(0UL);
    GarbageList<Target>::AddGarbage(&garbage_list, garbage);
  }

  EXPECT_EQ(kGarbageNum, garbage_list.Size());
}

TEST_F(GarbageListFixture, AddGarbage_LotOfGarbages_ListSizeCorrectlyUpdated)
{
  constexpr size_t kGarbageNum = kGarbageBufferSize * 1.5;

  auto garbage_list = GarbageList<Target>{current_epoch};
  GarbageList<Target>* current_list = &garbage_list;
  for (size_t count = 0; count < kGarbageNum; ++count) {
    auto garbage = New<Target>(0UL);
    current_list = GarbageList<Target>::AddGarbage(current_list, garbage);
  }

  EXPECT_EQ(kGarbageNum, garbage_list.Size());

  Delete(current_list);
}

TEST_F(GarbageListFixture, Clear_WithLessGarbages_ProtectedGarbagesRemain)
{
  constexpr size_t kTotalGarbageNum = kGarbageBufferSize / 2;
  constexpr size_t kHalfGarbageNum = kTotalGarbageNum / 2;

  auto garbage_list = GarbageList_t{current_epoch};
  std::vector<std::weak_ptr<Target>> garbage_references;
  const size_t protected_epoch = 1;

  // add unprotected garbages
  size_t count = 0;
  for (; count < kHalfGarbageNum; ++count) {
    auto garbage = New<std::shared_ptr<Target>>(new Target{0});
    GarbageList_t::AddGarbage(&garbage_list, garbage);
    garbage_references.emplace_back(*garbage);
  }

  garbage_list.SetCurrentEpoch(protected_epoch);

  // add protected garbages
  for (; count < kTotalGarbageNum; ++count) {
    auto garbage = New<std::shared_ptr<Target>>(new Target{0});
    GarbageList_t::AddGarbage(&garbage_list, garbage);
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

  auto garbage_list = GarbageList_t{current_epoch};
  GarbageList_t* current_list = &garbage_list;
  std::vector<std::weak_ptr<Target>> garbage_references;
  const size_t protected_epoch = 1;

  // add unprotected garbages
  size_t count = 0;
  for (; count < kHalfGarbageNum; ++count) {
    auto garbage = New<std::shared_ptr<Target>>(new Target{0});
    current_list = GarbageList_t::AddGarbage(current_list, garbage);
    garbage_references.emplace_back(*garbage);
  }

  garbage_list.SetCurrentEpoch(protected_epoch);

  // add protected garbages
  for (; count < kTotalGarbageNum; ++count) {
    auto garbage = New<std::shared_ptr<Target>>(new Target{0});
    current_list = GarbageList_t::AddGarbage(current_list, garbage);
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

  Delete(current_list);
}

}  // namespace dbgroup::memory::manager::component
