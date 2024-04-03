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

// the corresponding header
#include "memory/component/garbage_list.hpp"

// C++ standard libraries
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <thread>
#include <vector>

// external libraries
#include "gtest/gtest.h"

// library sources
#include "memory/utility.hpp"

namespace dbgroup::memory::component::test
{
/*##############################################################################
 * Global type aliases
 *############################################################################*/

using Target = uint64_t;

class GarbageListFixture : public ::testing::Test
{
 protected:
  /*############################################################################
   * Internal classes
   *##########################################################################*/

  struct SharedPtrTarget : public DefaultTarget {
    using T = std::shared_ptr<Target>;
    static constexpr bool kReusePages = true;
  };

  /*############################################################################
   * Type aliases
   *##########################################################################*/

  using GarbageList_t = GarbageList<SharedPtrTarget>;

  /*############################################################################
   * Test setup/teardown
   *##########################################################################*/

  void
  SetUp() override
  {
    current_epoch_ = 1;
    list_ = std::make_unique<GarbageList_t>();
  }

  void
  TearDown() override
  {
    list_->ClearGarbage(kMaxLong);
    list_.reset();
  }

  /*############################################################################
   * Internal utility functions
   *##########################################################################*/

  void
  AddGarbage(const size_t n)
  {
    for (size_t i = 0; i < n; ++i) {
      auto *target = new Target{0};

      auto *page = list_->GetPageIfPossible();
      if (page == nullptr) {
        page = Allocate<std::shared_ptr<Target>>();
      }
      auto *garbage = new (page) std::shared_ptr<Target>{target};

      list_->AddGarbage(current_epoch_.load(), garbage);
      references_.emplace_back(*garbage);
    }
  }

  void
  CheckGarbage(const size_t n)
  {
    for (size_t i = 0; i < n; ++i) {
      EXPECT_TRUE(references_[i].expired());
    }
    for (size_t i = n; i < references_.size(); ++i) {
      EXPECT_FALSE(references_[i].expired());
    }
  }

  /*############################################################################
   * Internal constants
   *##########################################################################*/

  static constexpr size_t kSmallNum = GarbageList_t::kBufferSize / 2;
  static constexpr size_t kLargeNum = GarbageList_t::kBufferSize * 4;
  static constexpr size_t kMaxLong = std::numeric_limits<size_t>::max();

  /*############################################################################
   * Internal member variables
   *##########################################################################*/

  std::atomic_size_t current_epoch_{};

  std::vector<std::weak_ptr<Target>> references_{};

  std::unique_ptr<GarbageList_t> list_{};
};

/*##############################################################################
 * Unit test definitions
 *############################################################################*/

TEST_F(GarbageListFixture, ClearGarbageWithoutProtectedEpochReleaseAllGarbage)
{
  AddGarbage(kLargeNum);
  list_->ClearGarbage(kMaxLong);

  CheckGarbage(kLargeNum);
}

TEST_F(GarbageListFixture, ClearGarbageWithProtectedEpochKeepProtectedGarbage)
{
  const size_t protected_epoch = current_epoch_.load() + 1;

  AddGarbage(kLargeNum);
  current_epoch_ = protected_epoch;
  AddGarbage(kLargeNum);
  list_->ClearGarbage(protected_epoch);

  CheckGarbage(kLargeNum);
}

TEST_F(GarbageListFixture, GetPageIfPossibleWithoutPagesReturnNullptr)
{
  auto *page = list_->GetPageIfPossible();

  EXPECT_EQ(nullptr, page);
}

TEST_F(GarbageListFixture, GetPageIfPossibleWithPagesReturnReusablePage)
{
  AddGarbage(kLargeNum);
  list_->ClearGarbage(kMaxLong);

  for (size_t i = 0; i < kLargeNum; ++i) {
    auto *page = list_->GetPageIfPossible();
    EXPECT_NE(nullptr, page);
    Release<std::shared_ptr<Target>>(page);
  }
  EXPECT_EQ(nullptr, list_->GetPageIfPossible());
}

TEST_F(GarbageListFixture, AddAndClearGarbageWithMultiThreadsReleaseAllGarbage)
{
  constexpr size_t kLoopNum = 1e6;
  std::atomic_bool is_running = true;

  std::thread loader{[&]() {
    for (size_t i = 0; i < kLoopNum; ++i) {
      AddGarbage(1);
      current_epoch_.fetch_add(1);
    }
  }};

  std::thread cleaner{[&]() {
    while (is_running.load()) {
      list_->ClearGarbage(current_epoch_.load() - 1);
    }
    list_->ClearGarbage(kMaxLong);
  }};

  loader.join();
  is_running.store(false);
  cleaner.join();

  CheckGarbage(kLoopNum);
}

}  // namespace dbgroup::memory::component::test
