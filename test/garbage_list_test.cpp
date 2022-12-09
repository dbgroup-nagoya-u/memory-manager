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
/*######################################################################################
 * Global type aliases
 *####################################################################################*/

using Target = uint64_t;

class GarbageListFixture : public ::testing::Test
{
 protected:
  /*####################################################################################
   * Internal classes
   *##################################################################################*/

  struct SharedPtrTarget {
    using T = std::shared_ptr<Target>;

    static constexpr bool kReusePages = true;

    static const inline std::function<void(void *)> deleter = [](void *ptr) {
      ::operator delete(ptr);
    };
  };

  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using GarbageList_t = GarbageList<SharedPtrTarget>;

  /*####################################################################################
   * Test setup/teardown
   *##################################################################################*/

  void
  SetUp() override
  {
    current_epoch_ = 1;
    garbage_list_ = std::make_shared<GarbageList_t>();
  }

  void
  TearDown() override
  {
  }

  /*####################################################################################
   * Internal utility functions
   *##################################################################################*/

  void
  AddGarbages(const size_t n)
  {
    for (size_t i = 0; i < n; ++i) {
      auto *target = new Target{0};
      auto *page = garbage_list_->GetPageIfPossible();

      std::shared_ptr<Target> *garbage{};
      if (page == nullptr) {
        garbage = new std::shared_ptr<Target>{target};
      } else {
        garbage = new (page) std::shared_ptr<Target>{target};
      }

      garbage_list_->AddGarbage(current_epoch_.load(), garbage);
      references_.emplace_back(*garbage);
    }
  }

  void
  CheckGarbages(const size_t n)
  {
    for (size_t i = 0; i < n; ++i) {
      EXPECT_TRUE(references_[i].expired());
    }
    for (size_t i = n; i < references_.size(); ++i) {
      EXPECT_FALSE(references_[i].expired());
    }
  }

  /*####################################################################################
   * Internal constants
   *##################################################################################*/

  static constexpr size_t kSmallNum = kGarbageBufferSize / 2;
  static constexpr size_t kLargeNum = kGarbageBufferSize * 2;
  static constexpr size_t kMaxLong = std::numeric_limits<size_t>::max();

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  std::atomic_size_t current_epoch_{};

  std::shared_ptr<GarbageList_t> garbage_list_{};

  std::vector<std::weak_ptr<Target>> references_{};
};

/*######################################################################################
 * Unit test definitions
 *####################################################################################*/

TEST_F(GarbageListFixture, DestructorWithAFewGarbagesReleaseAllGarbages)
{
  AddGarbages(kSmallNum);
  garbage_list_.reset(static_cast<GarbageList_t *>(nullptr));

  CheckGarbages(kSmallNum);
}

TEST_F(GarbageListFixture, DestructorWithManyGarbagesReleaseAllGarbages)
{
  AddGarbages(kLargeNum);
  garbage_list_.reset(static_cast<GarbageList_t *>(nullptr));

  CheckGarbages(kLargeNum);
}

TEST_F(GarbageListFixture, EmptyWithNoGarbagesReturnTrue)
{  //
  EXPECT_TRUE(garbage_list_->Empty());
}

TEST_F(GarbageListFixture, EmptyWithGarbagesReturnFalse)
{
  AddGarbages(kLargeNum);

  EXPECT_FALSE(garbage_list_->Empty());
}

TEST_F(GarbageListFixture, EmptyAfterClearingGarbagesReturnTrue)
{
  AddGarbages(kLargeNum);
  garbage_list_->ClearGarbages(kMaxLong);

  EXPECT_TRUE(garbage_list_->Empty());
}

TEST_F(GarbageListFixture, SizeWithNoGarbagesReturnZero)
{  //
  EXPECT_EQ(0, garbage_list_->Size());
}

TEST_F(GarbageListFixture, SizeWithGarbagesReturnCorrectSize)
{
  AddGarbages(kLargeNum);

  EXPECT_EQ(kLargeNum, garbage_list_->Size());
}

TEST_F(GarbageListFixture, SizeAfterClearingGarbagesReturnZero)
{
  AddGarbages(kLargeNum);
  garbage_list_->DestructGarbages(kMaxLong);

  EXPECT_EQ(0, garbage_list_->Size());
}

TEST_F(GarbageListFixture, ClearGarbagesWithoutProtectedEpochReleaseAllGarbages)
{
  AddGarbages(kLargeNum);
  garbage_list_->DestructGarbages(kMaxLong);

  CheckGarbages(kLargeNum);
}

TEST_F(GarbageListFixture, ClearGarbagesWithProtectedEpochKeepProtectedGarbages)
{
  const size_t protected_epoch = current_epoch_.load() + 1;
  AddGarbages(kLargeNum);
  current_epoch_ = protected_epoch;
  AddGarbages(kLargeNum);
  garbage_list_->DestructGarbages(protected_epoch);

  CheckGarbages(kLargeNum);
}

TEST_F(GarbageListFixture, GetPageIfPossibleWithoutPagesReturnNullptr)
{
  auto *page = garbage_list_->GetPageIfPossible();

  EXPECT_EQ(nullptr, page);
}

TEST_F(GarbageListFixture, GetPageIfPossibleWithPagesReturnReusablePage)
{
  AddGarbages(kLargeNum);
  garbage_list_->DestructGarbages(kMaxLong);

  for (size_t i = 0; i < kLargeNum; ++i) {
    EXPECT_NE(nullptr, garbage_list_->GetPageIfPossible());
  }
  EXPECT_EQ(nullptr, garbage_list_->GetPageIfPossible());
}

TEST_F(GarbageListFixture, AddAndClearGarbagesWithMultiThreadsReleaseAllGarbages)
{
  constexpr size_t kLoopNum = 1e6;
  std::atomic_bool is_running = true;

  std::thread loader{[&]() {
    for (size_t i = 0; i < kLoopNum; ++i) {
      AddGarbages(1);
      current_epoch_.fetch_add(1);
    }
  }};

  std::thread cleaner{[&]() {
    while (is_running.load()) {
      garbage_list_->DestructGarbages(current_epoch_.load() - 1);
    }
    garbage_list_->DestructGarbages(kMaxLong);
  }};

  loader.join();
  is_running.store(false);
  cleaner.join();
}

}  // namespace dbgroup::memory::component::test
