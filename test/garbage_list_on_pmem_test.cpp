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
#include "memory/component/garbage_list_on_pmem.hpp"

// C++ standard libraries
#include <cstdio>
#include <filesystem>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <vector>

// external libraries
#include "gtest/gtest.h"

// local sources
#include "common.hpp"

namespace dbgroup::memory::component::test
{
/*######################################################################################
 * Global type aliases
 *####################################################################################*/

using Target = uint64_t;

/*######################################################################################
 * Global constants
 *####################################################################################*/

/// a file permission for pmemobj_pool.
constexpr int kModeRW = S_IWUSR | S_IRUSR;  // NOLINT

constexpr std::string_view kTmpPMEMPath = DBGROUP_ADD_QUOTES(DBGROUP_TEST_TMP_PMEM_PATH);
constexpr const char *kPoolName = "memory_manager_garbage_list_on_pmem_test";
constexpr const char *kLayout = "target";

class GarbageListOnPMEMFixture : public ::testing::Test
{
 protected:
  /*####################################################################################
   * Internal classes
   *##################################################################################*/

  struct SharedPtrTarget {
    using T = std::shared_ptr<Target>;

    static constexpr bool kReusePages = true;
    static constexpr bool kOnPMEM = true;
  };

  struct PMEMRoot {
    ::pmem::obj::persistent_ptr<GarbageNodeOnPMEM<SharedPtrTarget>> head{nullptr};
  };

  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using GarbageList_t = GarbageListOnPMEM<SharedPtrTarget>;
  using GarbageNode_t = GarbageNodeOnPMEM<SharedPtrTarget>;
  using GarbageList_p = typename GarbageNode_t::GarbageList_p;
  using GarbageNode_p = typename GarbageNode_t::GarbageNode_p;
  using PMEMPool_t = ::pmem::obj::pool<PMEMRoot>;

  /*####################################################################################
   * Test setup/teardown
   *##################################################################################*/

  void
  SetUp() override
  {
    current_epoch_ = 1;

    try {
      // create a user directory for testing
      const std::string user_name{std::getenv("USER")};
      std::filesystem::path pool_path{kTmpPMEMPath};
      pool_path /= user_name;
      std::filesystem::create_directories(pool_path);
      pool_path /= kPoolName;
      std::filesystem::remove(pool_path);

      // create a persistent pool for testing
      constexpr size_t kSize = PMEMOBJ_MIN_POOL * 64;
      pool_ = PMEMPool_t::create(pool_path, kLayout, kSize, kModeRW);

      // allocate regions on persistent memory
      ::pmem::obj::flat_transaction::run(pool_, [&] {
        auto &&root = pool_.root();
        tail_ = ::pmem::obj::make_persistent<GarbageList_t>();
        root->head = ::pmem::obj::make_persistent<GarbageNode_t>(tail_, nullptr);
      });
    } catch (const std::exception &e) {
      std::cerr << e.what() << std::endl;
      std::terminate();
    }
  }

  void
  TearDown() override
  {
    ClearGarbage(kMaxLong);

    try {
      // delete regions on persistent memory
      ::pmem::obj::flat_transaction::run(pool_, [&] {
        ::pmem::obj::delete_persistent<GarbageList_t>(tail_);
        ::pmem::obj::delete_persistent<GarbageNode_t>(pool_.root()->head);
      });
    } catch (const std::exception &e) {
      std::cerr << e.what() << std::endl;
      std::terminate();
    }

    pool_.close();
  }

  /*####################################################################################
   * Internal utility functions
   *##################################################################################*/

  void
  AddGarbage(const size_t n)
  {
    auto &&head = pool_.root()->head;
    for (size_t i = 0; i < n; ++i) {
      auto *target = new Target{0};
      ::pmem::obj::persistent_ptr<std::shared_ptr<Target>> garbage{nullptr};
      head->GetPageIfPossible(garbage, pool_);
      if (garbage == nullptr) {
        try {
          ::pmem::obj::flat_transaction::run(pool_, [&] {
            garbage = ::pmem::obj::make_persistent<std::shared_ptr<Target>>(target);
          });
        } catch (const std::exception &e) {
          std::cerr << e.what() << std::endl;
          std::terminate();
        }
      } else {
        new (garbage.get()) std::shared_ptr<Target>{target};
      }

      const auto cur_epoch = current_epoch_.load();
      tail_ = GarbageList_t::AddGarbage(tail_, cur_epoch, garbage, pool_);
      references_.emplace_back(*garbage);
    }
  }

  void
  ClearGarbage(const size_t epoch_value)
  {
    GarbageNode_t::ClearGarbage(epoch_value, &node_mtx_, &(pool_.root()->head));
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

  PMEMPool_t pool_{};

  GarbageList_p tail_{nullptr};

  std::mutex node_mtx_{};
};

/*######################################################################################
 * Unit test definitions
 *####################################################################################*/

TEST_F(GarbageListOnPMEMFixture, ClearGarbageWithoutProtectedEpochReleaseAllGarbage)
{
  AddGarbage(kLargeNum);
  ClearGarbage(kMaxLong);

  CheckGarbage(kLargeNum);
}

TEST_F(GarbageListOnPMEMFixture, ClearGarbageWithProtectedEpochKeepProtectedGarbage)
{
  const size_t protected_epoch = current_epoch_.load() + 1;

  AddGarbage(kLargeNum);
  current_epoch_ = protected_epoch;
  AddGarbage(kLargeNum);
  ClearGarbage(protected_epoch);

  CheckGarbage(kLargeNum);
}

TEST_F(GarbageListOnPMEMFixture, GetPageIfPossibleWithoutPagesReturnNullptr)
{
  auto &&head = pool_.root()->head;
  ::pmem::obj::persistent_ptr<std::shared_ptr<Target>> page{nullptr};
  head->GetPageIfPossible(page, pool_);

  EXPECT_EQ(nullptr, page);
}

TEST_F(GarbageListOnPMEMFixture, GetPageIfPossibleWithPagesReturnReusablePage)
{
  AddGarbage(kLargeNum);
  ClearGarbage(kMaxLong);

  auto &&head = pool_.root()->head;
  ::pmem::obj::persistent_ptr<std::shared_ptr<Target>> page{nullptr};
  for (size_t i = 0; i < kLargeNum; ++i) {
    head->GetPageIfPossible(page, pool_);
    EXPECT_NE(nullptr, page);
    try {
      ::pmem::obj::flat_transaction::run(pool_, [&] {
        ::pmem::obj::delete_persistent<std::shared_ptr<Target>>(page);
        page = nullptr;
      });
    } catch (const std::exception &e) {
      std::cerr << e.what() << std::endl;
      std::terminate();
    }
  }

  head->GetPageIfPossible(page, pool_);
  EXPECT_EQ(nullptr, page);
}

TEST_F(GarbageListOnPMEMFixture, AddAndClearGarbageWithMultiThreadsReleaseAllGarbage)
{
  constexpr size_t kLoopNum = 1e5;
  std::atomic_bool is_running = true;

  std::thread loader{[&]() {
    for (size_t i = 0; i < kLoopNum; ++i) {
      AddGarbage(1);
      current_epoch_.fetch_add(1);
    }
    pool_.root()->head->Expire();
  }};

  std::thread cleaner{[&]() {
    while (is_running.load()) {
      ClearGarbage(current_epoch_.load() - 1);
    }
    do {
      ClearGarbage(kMaxLong);
    } while (pool_.root()->head != nullptr);
  }};

  loader.join();
  is_running.store(false);
  cleaner.join();

  CheckGarbage(kLoopNum);
  tail_ = nullptr;
}

}  // namespace dbgroup::memory::component::test
