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
#include "dbgroup/memory/component/list_holder.hpp"

// C++ standard libraries
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <thread>
#include <vector>

// external libraries
#include "dbgroup/lock/utility.hpp"
#include "gtest/gtest.h"

// library sources
#include "dbgroup/memory/utility.hpp"

namespace dbgroup::memory::component::test
{
/*############################################################################*
 * Global type aliases
 *############################################################################*/

using Target = uint64_t;

/*############################################################################*
 * Global constants
 *############################################################################*/

constexpr size_t kLargeNum = 1024;
constexpr size_t kMaxLong = std::numeric_limits<size_t>::max();
constexpr size_t kReusablePageNum = 32;

/*############################################################################*
 * Fixture declaration
 *############################################################################*/

class GarbageListFixture : public ::testing::Test
{
 protected:
  /*##########################################################################*
   * Internal classes
   *##########################################################################*/

  struct SharedPtrTarget : public DefaultTarget {
    using T = std::shared_ptr<Target>;
    static constexpr bool kReusePages = true;
  };

  /*##########################################################################*
   * Type aliases
   *##########################################################################*/

  using GarbageList_t = ListHolder<SharedPtrTarget>;

  /*##########################################################################*
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
    list_->ClearGarbage(kMaxLong, kReusablePageNum, reuse_pages_);
    for (auto *page : reuse_pages_) {
      Release<SharedPtrTarget>(page);
    }
    reuse_pages_.clear();
    list_.reset();
  }

  /*##########################################################################*
   * Internal utility functions
   *##########################################################################*/

  void
  AddGarbage(  //
      const size_t n)
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
  CheckGarbage(  //
      const size_t n)
  {
    for (size_t i = 0; i < n; ++i) {
      ASSERT_TRUE(references_[i].expired());
    }
    for (size_t i = n; i < references_.size(); ++i) {
      ASSERT_FALSE(references_[i].expired());
    }
  }

  /*##########################################################################*
   * Verification APIs
   *##########################################################################*/

  void
  VerifyGCWithMultiThreads(      //
      const size_t cleaner_num)  //
  {
    const size_t loop_num = 1E5 * cleaner_num;
    std::atomic_bool has_prepared = false;
    std::atomic_bool is_running = true;

    AddGarbage(loop_num);
    current_epoch_.fetch_add(1);

    std::thread loader{[&]() {
      while (!has_prepared) {
        CPP_UTILITY_SPINLOCK_HINT
      }
      for (size_t i = 0; i < loop_num; ++i) {
        AddGarbage(1);
        current_epoch_.fetch_add(1);
      }
    }};

    std::vector<std::thread> cleaners{};
    for (size_t i = 0; i < cleaner_num; ++i) {
      cleaners.emplace_back([&]() {
        while (!has_prepared) {
          CPP_UTILITY_SPINLOCK_HINT
        }
        thread_local std::vector<void *> reuse_pages{};
        while (is_running) {
          list_->ClearGarbage(current_epoch_ - 1, kReusablePageNum, reuse_pages);
          for (auto *page : reuse_pages) {
            Release<SharedPtrTarget>(page);
          }
          reuse_pages.clear();
        }
        list_->ClearGarbage(kMaxLong, kReusablePageNum, reuse_pages);
        for (auto *page : reuse_pages) {
          Release<SharedPtrTarget>(page);
        }
      });
    }

    has_prepared.store(true);
    loader.join();
    is_running.store(false);
    for (auto &&t : cleaners) {
      t.join();
    }

    CheckGarbage(2 * loop_num);
  }

  /*##########################################################################*
   * Internal member variables
   *##########################################################################*/

  std::atomic_size_t current_epoch_{};

  std::vector<std::weak_ptr<Target>> references_{};

  std::vector<void *> reuse_pages_{};

  std::unique_ptr<GarbageList_t> list_{};
};

/*############################################################################*
 * Unit test definitions
 *############################################################################*/

TEST_F(  //
    GarbageListFixture,
    ClearGarbageWithoutProtectedEpochReleaseAllGarbage)
{
  AddGarbage(kLargeNum);
  list_->ClearGarbage(kMaxLong, kReusablePageNum, reuse_pages_);

  CheckGarbage(kLargeNum);
}

TEST_F(  //
    GarbageListFixture,
    ClearGarbageWithProtectedEpochKeepProtectedGarbage)
{
  const size_t protected_epoch = current_epoch_.load() + 1;

  AddGarbage(kLargeNum);
  current_epoch_ = protected_epoch;
  AddGarbage(kLargeNum);
  list_->ClearGarbage(protected_epoch, kReusablePageNum, reuse_pages_);

  CheckGarbage(kLargeNum);
}

TEST_F(  //
    GarbageListFixture,
    GetPageIfPossibleWithoutPagesReturnNullptr)
{
  EXPECT_EQ(nullptr, list_->GetPageIfPossible());
}

TEST_F(  //
    GarbageListFixture,
    GetPageIfPossibleWithPagesReturnReusablePage)
{
  AddGarbage(kLargeNum);
  list_->ClearGarbage(kMaxLong, kReusablePageNum, reuse_pages_);

  for (size_t i = 0; i < kReusablePageNum; ++i) {
    auto *page = list_->GetPageIfPossible();
    EXPECT_NE(nullptr, page);
    Release<std::shared_ptr<Target>>(page);
  }
  EXPECT_EQ(nullptr, list_->GetPageIfPossible());
}

TEST_F(  //
    GarbageListFixture,
    AddGarbageWithSingleCleaner)
{
  VerifyGCWithMultiThreads(1);
}

TEST_F(  //
    GarbageListFixture,
    AddGarbageWithManyCleaner)
{
  VerifyGCWithMultiThreads(kMaxThreadNum);
}

}  // namespace dbgroup::memory::component::test
