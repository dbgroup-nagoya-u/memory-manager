/*
 * Copyright 2023 Database Group, Nagoya University
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
#include "dbgroup/memory/mapping_table.hpp"

// C++ standard libraries
#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <future>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

// external libraries
#include "dbgroup/lock/common.hpp"
#include "gtest/gtest.h"

namespace dbgroup::memory::test
{
/*############################################################################*
 * Global constants
 *############################################################################*/

constexpr size_t kOverSheetCapacity = MappingTable::kColNum * MappingTable::kRowNum * 1.1;
constexpr size_t kThreadNum = DBGROUP_TEST_THREAD_NUM;
constexpr bool kUseCASWeak = true;

/*############################################################################*
 * Fixture class definition
 *############################################################################*/

class MappingTableFixture : public testing::Test
{
  /*##########################################################################*
   * Type aliases
   *##########################################################################*/

  using PIDContainer = std::vector<uint64_t>;

 protected:
  /*##########################################################################*
   * Setup/Teardown
   *##########################################################################*/

  void
  SetUp() override
  {
    table_ = std::make_unique<MappingTable>();
  }

  void
  TearDown() override
  {
  }

  /*##########################################################################*
   * Utility functions
   *##########################################################################*/

  auto
  GetPageIDs(               //
      const size_t id_num)  //
      -> PIDContainer
  {
    PIDContainer ids{};
    ids.reserve(id_num);

    for (size_t i = 0; i < id_num; ++i) {
      const auto pid = table_->ReservePageID();
      EXPECT_TRUE(MappingTable::IsPageID(pid));
      EXPECT_EQ(table_->Load(pid), nullptr);
      table_->Store(pid, std::bit_cast<void *>(pid));
      ids.emplace_back(pid);
    }

    return ids;
  }

  auto
  GetPageIDsWithMultiThreads(  //
      const size_t id_num)     //
      -> PIDContainer
  {
    // lambda function to run tests with multi threads
    auto f = [&](std::promise<PIDContainer> p, const size_t id_num) {
      auto &&lids = GetPageIDs(id_num);
      p.set_value(std::move(lids));
    };

    // run GetNewLogicalGetID with multi-threads
    std::vector<std::future<PIDContainer>> futures{};
    futures.reserve(kThreadNum);
    for (size_t i = 0; i < kThreadNum; ++i) {
      std::promise<PIDContainer> p;
      futures.emplace_back(p.get_future());
      std::thread{f, std::move(p), id_num}.detach();
    }

    // gather results
    PIDContainer ids{};
    ids.reserve(id_num * kThreadNum);
    for (auto &&future : futures) {
      auto &&ids_per_thread = future.get();
      ids.insert(ids.end(), ids_per_thread.begin(), ids_per_thread.end());
    }

    return ids;
  }

  /*##########################################################################*
   * Functions for verification
   *##########################################################################*/

  void
  VerifyPageIDs(  //
      PIDContainer &ids)
  {
    // reserved PIDs are unique
    std::sort(ids.begin(), ids.end());
    auto &&actual_end = std::unique(ids.begin(), ids.end());
    EXPECT_EQ(ids.end(), actual_end);

    // load the sorted values
    for (auto &&pid : ids) {
      auto *val = table_->Load<size_t>(pid);
      EXPECT_EQ(pid, std::bit_cast<size_t>(val));
      table_->Store(pid, nullptr);
    }
  }

  void
  VerifyCAS(  //
      const bool use_cas_weak)
  {
    constexpr size_t kRepeatNum = 1E6;
    const auto pid = table_->ReservePageID();

    std::vector<std::thread> threads{};
    for (size_t i = 0; i < kThreadNum; ++i) {
      threads.emplace_back([&]() {
        for (size_t j = 0; j < kRepeatNum; ++j) {
          auto *expected = table_->Load(pid);
          while (true) {
            auto *desired = std::bit_cast<void *>(std::bit_cast<size_t>(expected) + 1);
            if ((use_cas_weak && table_->CAS(pid, expected, desired))
                || (!use_cas_weak && table_->CASStrong(pid, expected, desired))) {
              break;
            }
            CPP_UTILITY_SPINLOCK_HINT
          }
        }
      });
    }
    for (auto &&t : threads) {
      t.join();
    }

    auto sum = std::bit_cast<size_t>(table_->Load(pid));
    EXPECT_EQ(sum, kThreadNum * kRepeatNum);
    table_->Store(pid, nullptr);
  }

  /*##########################################################################*
   * Internal member variables
   *##########################################################################*/

  std::unique_ptr<MappingTable> table_{nullptr};
};

/*############################################################################*
 * Unit test definitions
 *############################################################################*/

TEST_F(MappingTableFixture, ReservePageIDsInRowReturnUniqueIDs)
{
  auto &&ids = GetPageIDs(MappingTable::kColNum - 1);
  VerifyPageIDs(ids);
}

TEST_F(MappingTableFixture, ReservePageIDsOverSheetsReturnUniqueIDs)
{
  auto &&ids = GetPageIDs(kOverSheetCapacity);
  VerifyPageIDs(ids);
}

TEST_F(MappingTableFixture, ReservePageIDsInRowWithMultiThreadsReturnUniqueIDs)
{
  auto &&ids = GetPageIDsWithMultiThreads((MappingTable::kColNum / kThreadNum) - 1);
  VerifyPageIDs(ids);
}

TEST_F(MappingTableFixture, ReservePageIDsOverSheetsWithMultiThreadsReturnUniqueIDs)
{
  auto &&ids = GetPageIDsWithMultiThreads(kOverSheetCapacity);
  VerifyPageIDs(ids);
}

TEST_F(MappingTableFixture, CASUpdateStoredPageAtomically)
{  //
  VerifyCAS(kUseCASWeak);
}

TEST_F(MappingTableFixture, CASStrongUpdateStoredPageAtomically)
{  //
  VerifyCAS(!kUseCASWeak);
}

}  // namespace dbgroup::memory::test
