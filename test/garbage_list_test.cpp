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
#include <future>
#include <memory>
#include <thread>
#include <vector>

// external sources
#include "gtest/gtest.h"

// local sources
#include "common.hpp"

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
  using GarbageNode_t = GarbageNode<SharedPtrTarget>;

  /*####################################################################################
   * Test setup/teardown
   *##################################################################################*/

  void
  SetUp() override
  {
    current_epoch_ = 1;
    tail_ = new GarbageList_t{};
    garbage_node_ = new GarbageNode_t{tail_, nullptr};
  }

  void
  TearDown() override
  {
    ClearGarbage(kMaxLong);
    delete garbage_node_;
    delete tail_;
  }

  /*####################################################################################
   * Internal utility functions
   *##################################################################################*/

  void
  AddGarbage(const size_t n)
  {
    for (size_t i = 0; i < n; ++i) {
      auto *target = new Target{0};
      auto *page = garbage_node_->GetPageIfPossible();

      std::shared_ptr<Target> *garbage{};
      if (page == nullptr) {
        garbage = new std::shared_ptr<Target>{target};
      } else {
        garbage = new (page) std::shared_ptr<Target>{target};
      }

      tail_ = GarbageList_t::AddGarbage(tail_, current_epoch_.load(), garbage);
      references_.emplace_back(*garbage);
    }
  }

  void
  ClearGarbage(const size_t epoch_value)
  {
    GarbageNode_t::ClearGarbage(epoch_value, &node_mtx_, &garbage_node_);
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

  std::vector<std::weak_ptr<Target>> references_{};

  GarbageList_t *tail_{nullptr};

  std::mutex node_mtx_{};

  GarbageNode_t *garbage_node_{nullptr};
};

/*######################################################################################
 * Unit test definitions
 *####################################################################################*/

TEST_F(GarbageListFixture, ClearGarbageWithoutProtectedEpochReleaseAllGarbage)
{
  AddGarbage(kLargeNum);
  ClearGarbage(kMaxLong);

  CheckGarbage(kLargeNum);
}

TEST_F(GarbageListFixture, ClearGarbageWithProtectedEpochKeepProtectedGarbage)
{
  const size_t protected_epoch = current_epoch_.load() + 1;

  AddGarbage(kLargeNum);
  current_epoch_ = protected_epoch;
  AddGarbage(kLargeNum);
  ClearGarbage(protected_epoch);

  CheckGarbage(kLargeNum);
}

TEST_F(GarbageListFixture, GetPageIfPossibleWithoutPagesReturnNullptr)
{
  auto *page = garbage_node_->GetPageIfPossible();

  EXPECT_EQ(nullptr, page);
}

TEST_F(GarbageListFixture, GetPageIfPossibleWithPagesReturnReusablePage)
{
  AddGarbage(kLargeNum);
  ClearGarbage(kMaxLong);

  for (size_t i = 0; i < kLargeNum; ++i) {
    EXPECT_NE(nullptr, garbage_node_->GetPageIfPossible());
  }
  EXPECT_EQ(nullptr, garbage_node_->GetPageIfPossible());
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
    garbage_node_->Expire();
  }};

  std::thread cleaner{[&]() {
    while (is_running.load()) {
      ClearGarbage(current_epoch_.load() - 1);
    }
    do {
      ClearGarbage(kMaxLong);
    } while (garbage_node_ != nullptr);
  }};

  loader.join();
  is_running.store(false);
  cleaner.join();

  CheckGarbage(kLoopNum);
  tail_ = nullptr;
}

}  // namespace dbgroup::memory::component::test
