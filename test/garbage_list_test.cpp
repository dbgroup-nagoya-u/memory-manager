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

#include "memory/component/garbage_list.hpp"

#include <future>
#include <memory>
#include <thread>
#include <vector>

#include "common.hpp"
#include "gtest/gtest.h"

namespace dbgroup::memory::component::test
{
class GarbageListFixture : public ::testing::Test
{
 protected:
  /*################################################################################################
   * Type aliases
   *##############################################################################################*/

  using Target = uint64_t;
  using GarbageList_t = GarbageList<std::shared_ptr<Target>>;

  /*################################################################################################
   * Test setup/teardown
   *##############################################################################################*/

  void
  SetUp() override
  {
    current_epoch = 1;
    garbage_list = std::make_shared<GarbageList_t>(current_epoch);
  }

  void
  TearDown() override
  {
  }

  /*################################################################################################
   * Internal utility functions
   *##############################################################################################*/

  void
  AddGarbages(const size_t n)
  {
    for (size_t i = 0; i < n; ++i) {
      auto target = new Target{0};
      auto page = garbage_list->GetPageIfPossible();

      std::shared_ptr<Target> *garbage;
      if (page == nullptr) {
        garbage = new std::shared_ptr<Target>{target};
      } else {
        garbage = new (page) std::shared_ptr<Target>{target};
      }

      garbage_list->AddGarbage(garbage);
      references.emplace_back(*garbage);
    }
  }

  void
  CheckGarbages(const size_t n)
  {
    for (size_t i = 0; i < n; ++i) {
      EXPECT_TRUE(references[i].expired());
    }
    for (size_t i = n; i < references.size(); ++i) {
      EXPECT_FALSE(references[i].expired());
    }
  }

  /*################################################################################################
   * Internal constants
   *##############################################################################################*/

  static constexpr size_t kSmallNum = kGarbageBufferSize / 2;
  static constexpr size_t kLargeNum = kGarbageBufferSize * 2;
  static constexpr size_t kMaxLong = std::numeric_limits<size_t>::max();

  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  std::atomic_size_t current_epoch;

  std::shared_ptr<GarbageList_t> garbage_list;

  std::vector<std::weak_ptr<Target>> references;
};

/*##################################################################################################
 * Unit test definitions
 *################################################################################################*/

TEST_F(GarbageListFixture, Destruct_AFewGarbages_AddedGarbagesCorrectlyFreed)
{
  AddGarbages(kSmallNum);
  garbage_list.reset(static_cast<GarbageList_t *>(nullptr));

  CheckGarbages(kSmallNum);
}

TEST_F(GarbageListFixture, Destruct_ManyGarbages_AddedGarbagesCorrectlyFreed)
{
  AddGarbages(kLargeNum);
  garbage_list.reset(static_cast<GarbageList_t *>(nullptr));

  CheckGarbages(kLargeNum);
}

TEST_F(GarbageListFixture, Empty_NoGarbages_ListIsEmpty)
{  //
  EXPECT_TRUE(garbage_list->Empty());
}

TEST_F(GarbageListFixture, Empty_GarbagesAdded_ListIsNotEmpty)
{
  AddGarbages(kLargeNum);

  EXPECT_FALSE(garbage_list->Empty());
}

TEST_F(GarbageListFixture, Empty_GarbagesCleared_ListIsEmpty)
{
  AddGarbages(kLargeNum);
  garbage_list->ClearGarbages(kMaxLong);

  EXPECT_TRUE(garbage_list->Empty());
}

TEST_F(GarbageListFixture, Size_NoGarbages_GetCorrectSize)
{  //
  EXPECT_EQ(0, garbage_list->Size());
}

TEST_F(GarbageListFixture, Size_GarbagesAdded_GetCorrectSize)
{
  AddGarbages(kLargeNum);

  EXPECT_EQ(kLargeNum, garbage_list->Size());
}

TEST_F(GarbageListFixture, Size_GarbagesCleared_GetCorrectSize)
{
  AddGarbages(kLargeNum);
  garbage_list->ClearGarbages(kMaxLong);

  EXPECT_EQ(0, garbage_list->Size());
}

TEST_F(GarbageListFixture, ClearGarbages_WithoutProtectedEpoch_AllGarbagesCleared)
{
  AddGarbages(kLargeNum);
  garbage_list->ClearGarbages(kMaxLong);

  CheckGarbages(kLargeNum);
}

TEST_F(GarbageListFixture, ClearGarbages_WithProtectedEpoch_ProtectedGarbagesRemain)
{
  const size_t protected_epoch = current_epoch.load(kMORelax) + 1;
  AddGarbages(kLargeNum);
  current_epoch = protected_epoch;
  AddGarbages(kLargeNum);
  garbage_list->ClearGarbages(protected_epoch);

  CheckGarbages(kLargeNum);
}

TEST_F(GarbageListFixture, GetPageIfPossible_WithoutPages_GetNullptr)
{
  auto page = garbage_list->GetPageIfPossible();

  EXPECT_EQ(nullptr, page);
}

TEST_F(GarbageListFixture, GetPageIfPossible_WithPages_GetReusablePages)
{
  AddGarbages(kLargeNum);
  garbage_list->ClearGarbages(kMaxLong);

  for (size_t i = 0; i < kLargeNum; ++i) {
    EXPECT_NE(nullptr, garbage_list->GetPageIfPossible());
  }
  EXPECT_EQ(nullptr, garbage_list->GetPageIfPossible());
}

TEST_F(GarbageListFixture, AddClearGarbages_WithMultiThreads_AllGarbagesCleared)
{
  constexpr size_t LoopNum = 1e6;
  std::atomic_bool is_running = true;

  std::thread loader{[&]() {
    for (size_t i = 0; i < LoopNum; ++i) {
      this->AddGarbages(1);
      this->current_epoch.fetch_add(1, kMORelax);
    }
  }};

  std::thread cleaner{[&]() {
    while (is_running.load(kMORelax)) {
      this->garbage_list->ClearGarbages(this->current_epoch.load(kMORelax) - 1);
    }
    this->garbage_list->ClearGarbages(this->kMaxLong);
  }};

  loader.join();
  is_running.store(false, kMORelax);
  cleaner.join();
}

}  // namespace dbgroup::memory::component::test
