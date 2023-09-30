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
// prepare a temporary directory
auto *const env = testing::AddGlobalTestEnvironment(new TmpDirManager);

/*######################################################################################
 * Global type aliases
 *####################################################################################*/

using Target = uint64_t;

/*######################################################################################
 * Global constants
 *####################################################################################*/

/// a file permission for pmemobj_pool.
constexpr int kModeRW = S_IWUSR | S_IRUSR;  // NOLINT
constexpr const char *kPoolName = "memory_manager_garbage_list_on_pmem_test";
constexpr const char *kLayout = "target";

class GarbageListOnPMEMFixture : public ::testing::Test
{
 protected:
  /*####################################################################################
   * Internal classes
   *##################################################################################*/

  struct SharedPtrTarget : public DefaultTarget {
    using T = std::shared_ptr<Target>;
    static constexpr bool kReusePages = true;
    static constexpr bool kOnPMEM = true;
  };

  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using GarbageList_t = GarbageListOnPMEM<SharedPtrTarget>;

  /*####################################################################################
   * Test setup/teardown
   *##################################################################################*/

  void
  SetUp() override
  {
    // create a persistent pool for testing
    constexpr size_t kSize = PMEMOBJ_MIN_POOL * 16;  // 128MiB
    auto &&pool_path = GetTmpPoolPath();
    pool_path /= kPoolName;
    if (std::filesystem::exists(pool_path)) {
      pop_ = pmemobj_open(pool_path.c_str(), kLayout);
    } else {
      pop_ = pmemobj_create(pool_path.c_str(), kLayout, kSize, kModeRW);
    }
    auto *root_addr = pmemobj_direct(pmemobj_root(pop_, sizeof(PMEMoid)));
    auto *tls_oid = reinterpret_cast<PMEMoid *>(root_addr);
    list_ = std::make_unique<GarbageList_t>();
    list_->SetPMEMInfo(pop_, tls_oid);

    // initialize members
    current_epoch_ = 1;
    references_.clear();
  }

  void
  TearDown() override
  {
    list_.reset(nullptr);

    auto *root_addr = pmemobj_direct(pmemobj_root(pop_, sizeof(PMEMoid)));
    auto *tls_oid = reinterpret_cast<PMEMoid *>(root_addr);
    pmemobj_free(tls_oid);
    pmemobj_close(pop_);
  }

  /*####################################################################################
   * Internal utility functions
   *##################################################################################*/

  void
  AllocateTarget(PMEMoid *oid)
  {
    const auto rc = pmemobj_zalloc(pop_, oid, sizeof(std::shared_ptr<Target>), kDefaultPMDKType);
    if (rc != 0) {
      std::cerr << pmemobj_errormsg() << std::endl;
      throw std::bad_alloc{};
    }
  }

  void
  AddGarbage(const size_t n)
  {
    auto *garbage = list_->GetTmpField(0);
    for (size_t i = 0; i < n; ++i) {
      list_->GetPageIfPossible(garbage);
      if (OID_IS_NULL(*garbage)) {
        AllocateTarget(garbage);
      }

      auto *target = new Target{0};
      auto *shared = new (pmemobj_direct(*garbage)) std::shared_ptr<Target>{target};
      references_.emplace_back(*shared);
      list_->AddGarbage(current_epoch_.load(), garbage);
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

  static constexpr size_t kSmallNum = GarbageList_t::kBufferSize / 2;
  static constexpr size_t kLargeNum = GarbageList_t::kBufferSize * 4;
  static constexpr size_t kMaxLong = std::numeric_limits<size_t>::max();

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  std::atomic_size_t current_epoch_{};

  std::vector<std::weak_ptr<Target>> references_{};

  PMEMobjpool *pop_{nullptr};

  std::unique_ptr<GarbageList_t> list_{};
};

/*######################################################################################
 * Unit test definitions
 *####################################################################################*/

TEST_F(GarbageListOnPMEMFixture, ClearGarbageWithoutProtectedEpochReleaseAllGarbage)
{
  AddGarbage(kLargeNum);
  list_->ClearGarbage(kMaxLong);

  CheckGarbage(kLargeNum);
}

TEST_F(GarbageListOnPMEMFixture, ClearGarbageWithProtectedEpochKeepProtectedGarbage)
{
  const size_t protected_epoch = current_epoch_.load() + 1;

  AddGarbage(kLargeNum);
  current_epoch_ = protected_epoch;
  AddGarbage(kLargeNum);
  list_->ClearGarbage(protected_epoch);

  CheckGarbage(kLargeNum);
}

TEST_F(GarbageListOnPMEMFixture, GetPageIfPossibleWithoutPagesReturnNullptr)
{
  auto *oid = list_->GetTmpField(0);
  list_->GetPageIfPossible(oid);
  EXPECT_TRUE(OID_IS_NULL(*oid));
}

TEST_F(GarbageListOnPMEMFixture, GetPageIfPossibleWithPagesReturnReusablePage)
{
  AddGarbage(kLargeNum);
  list_->ClearGarbage(kMaxLong);

  auto *oid = list_->GetTmpField(0);
  for (size_t i = 0; i < kLargeNum; ++i) {
    list_->GetPageIfPossible(oid);
    EXPECT_FALSE(OID_IS_NULL(*oid));
    pmemobj_free(oid);
  }

  list_->GetPageIfPossible(oid);
  EXPECT_TRUE(OID_IS_NULL(*oid));
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
  list_ = nullptr;
}

}  // namespace dbgroup::memory::component::test
