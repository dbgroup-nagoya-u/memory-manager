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

#include "memory/component/garbage_list_on_pmem.hpp"

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

class GarbageListOnPMEMFixture : public ::testing::Test
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

  using GarbageListOnPMEM_t = GarbageListOnPMEM<SharedPtrTarget>;

  /*####################################################################################
   * Test setup/teardown
   *##################################################################################*/

  void
  SetUp() override
  {
    current_epoch_ = 1;
    garbage_list_ = std::make_shared<GarbageListOnPMEM_t>();
  }

  void
  TearDown() override
  {
  }

  /*####################################################################################
   * Internal utility functions
   *##################################################################################*/

  void
  AddGarbage(const size_t n)
  {
    for (size_t i = 0; i < n; ++i) {
      auto *target = new Target{0};
      auto *garbage = new std::shared_ptr<Target>{target};

      garbage_list_->AddGarbage(current_epoch_.load(), garbage);
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

  std::shared_ptr<GarbageListOnPMEM_t> garbage_list_{};

  std::vector<std::weak_ptr<Target>> references_{};
};

/*######################################################################################
 * Unit test definitions
 *####################################################################################*/

TEST_F(GarbageListOnPMEMFixture, DestructorWithAFewGarbageReleaseAllGarbage)
{
  AddGarbage(kSmallNum);
  garbage_list_.reset(static_cast<GarbageListOnPMEM_t *>(nullptr));

  CheckGarbage(kSmallNum);
}

TEST_F(GarbageListOnPMEMFixture, DestructorWithManyGarbageReleaseAllGarbage)
{
  AddGarbage(kLargeNum);
  garbage_list_.reset(static_cast<GarbageListOnPMEM_t *>(nullptr));

  CheckGarbage(kLargeNum);
}

TEST_F(GarbageListOnPMEMFixture, EmptyWithNoGarbageReturnTrue)
{  //
  EXPECT_TRUE(garbage_list_->Empty());
}

TEST_F(GarbageListOnPMEMFixture, EmptyWithGarbageReturnFalse)
{
  AddGarbage(kLargeNum);

  EXPECT_FALSE(garbage_list_->Empty());
}

TEST_F(GarbageListOnPMEMFixture, EmptyAfterClearingGarbageReturnTrue)
{
  AddGarbage(kLargeNum);
  garbage_list_->ClearGarbage(kMaxLong);

  EXPECT_TRUE(garbage_list_->Empty());
}

TEST_F(GarbageListOnPMEMFixture, SizeWithNoGarbageReturnZero)
{  //
  EXPECT_EQ(0, garbage_list_->Size());
}

TEST_F(GarbageListOnPMEMFixture, SizeWithGarbageReturnCorrectSize)
{
  AddGarbage(kLargeNum);

  EXPECT_EQ(kLargeNum, garbage_list_->Size());
}

TEST_F(GarbageListOnPMEMFixture, SizeAfterClearingGarbageReturnZero)
{
  AddGarbage(kLargeNum);
  garbage_list_->ClearGarbage(kMaxLong);

  EXPECT_EQ(0, garbage_list_->Size());
}

TEST_F(GarbageListOnPMEMFixture, ClearGarbageWithoutProtectedEpochReleaseAllGarbage)
{
  AddGarbage(kLargeNum);
  garbage_list_->ClearGarbage(kMaxLong);

  CheckGarbage(kLargeNum);
}

TEST_F(GarbageListOnPMEMFixture, ClearGarbageWithProtectedEpochKeepProtectedGarbage)
{
  const size_t protected_epoch = current_epoch_.load() + 1;
  AddGarbage(kLargeNum);
  current_epoch_ = protected_epoch;
  AddGarbage(kLargeNum);
  garbage_list_->ClearGarbage(protected_epoch);

  CheckGarbage(kLargeNum);
}

TEST_F(GarbageListOnPMEMFixture, AddAndClearGarbageWithMultiThreadsReleaseAllGarbage)
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
      garbage_list_->ClearGarbage(current_epoch_.load() - 1);
    }
    garbage_list_->ClearGarbage(kMaxLong);
  }};

  loader.join();
  is_running.store(false);
  cleaner.join();
}

}  // namespace dbgroup::memory::component::test
