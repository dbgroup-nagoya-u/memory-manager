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
#include <atomic>
#include <bit>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <future>
#include <memory>
#include <new>
#include <stdexcept>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

// external libraries
#include <gtest/gtest.h>

// our libraries
#include <dbgroup/constants.hpp>
#include <dbgroup/lock/utility.hpp>

// local sources
#include "dbgroup/memory/epoch_based_gc.hpp"

namespace dbgroup::memory::test
{
/*############################################################################*
 * Global constants
 *############################################################################*/

constexpr size_t kOverSheetCapacity =
    static_cast<size_t>(MappingTable::kColNum * MappingTable::kRowNum * 1.1);
constexpr size_t kThreadNum = DBGROUP_TEST_THREAD_NUM;
constexpr size_t kPageSize = k4Ki;
constexpr size_t kGCInterval = 1;
constexpr std::chrono::seconds kTimeout{10};
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
    table_ = std::make_unique<MappingTable>(kPageSize);
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
      table_->Store(pid, std::bit_cast<void*>(pid));
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
      auto&& lids = GetPageIDs(id_num);
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
    for (auto&& future : futures) {
      auto&& ids_per_thread = future.get();
      ids.insert(ids.end(), ids_per_thread.begin(), ids_per_thread.end());
    }

    return ids;
  }

  /**
   * @brief Wait until the actual memory usage becomes a given value.
   *
   * @param expected An expected actual usage in bytes.
   * @retval true if the usage becomes the expected value.
   * @retval false if timeout occurs.
   */
  auto
  WaitForUsage(               //
      const size_t expected)  //
      -> bool
  {
    const auto deadline = std::chrono::steady_clock::now() + kTimeout;
    while (std::chrono::steady_clock::now() < deadline) {
      if (table_->GetMemoryUsage().first == expected) return true;
      std::this_thread::sleep_for(std::chrono::milliseconds{kGCInterval});
    }
    return false;
  }

  /*##########################################################################*
   * Functions for verification
   *##########################################################################*/

  void
  VerifyPageIDs(  //
      PIDContainer& ids)
  {
    // reserved PIDs are unique
    std::sort(ids.begin(), ids.end());
    auto&& actual_end = std::unique(ids.begin(), ids.end());
    EXPECT_EQ(ids.end(), actual_end);

    // load the sorted values
    for (auto&& pid : ids) {
      auto* val = table_->Load<size_t>(pid);
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
          auto* expected = table_->Load(pid);
          while (true) {
            auto* desired = std::bit_cast<void*>(std::bit_cast<size_t>(expected) + 1);
            if ((use_cas_weak && table_->CAS(pid, expected, desired))
                || (!use_cas_weak && table_->CASStrong(pid, expected, desired))) {
              break;
            }
            CPP_UTILITY_SPINLOCK_HINT
          }
        }
      });
    }
    for (auto&& t : threads) {
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
  auto&& ids = GetPageIDs(MappingTable::kColNum - 1);
  VerifyPageIDs(ids);
}

TEST_F(MappingTableFixture, ReservePageIDsOverSheetsReturnUniqueIDs)
{
  auto&& ids = GetPageIDs(kOverSheetCapacity);
  VerifyPageIDs(ids);
}

TEST_F(MappingTableFixture, ReservePageIDsInRowWithMultiThreadsReturnUniqueIDs)
{
  auto&& ids = GetPageIDsWithMultiThreads((MappingTable::kColNum / kThreadNum) - 1);
  VerifyPageIDs(ids);
}

TEST_F(MappingTableFixture, ReservePageIDsOverSheetsWithMultiThreadsReturnUniqueIDs)
{
  auto&& ids = GetPageIDsWithMultiThreads(kOverSheetCapacity);
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

TEST_F(MappingTableFixture, GetMemoryUsageReturnUsageOfReservedSpaces)
{
  constexpr size_t kColNum = MappingTable::kColNum;
  constexpr size_t kRowNum = MappingTable::kRowNum;
  constexpr size_t kIDsInSheet = kColNum * kRowNum;

  size_t reserved_num = 0;
  for (const size_t target_num : {0UL, 1UL, kColNum - 1, kColNum, kColNum + 1, kIDsInSheet - 1,
                                  kIDsInSheet, kOverSheetCapacity}) {
    for (; reserved_num < target_num; ++reserved_num) {
      static_cast<void>(table_->ReservePageID());
    }

    // a table page has a header (a cache line) and pointers to sheets, rows, and pages
    const auto row_num = reserved_num / kColNum + 1;
    const auto sheet_num = (row_num - 1) / kRowNum + 1;
    const auto [used, reserved] = table_->GetMemoryUsage();
    EXPECT_EQ(reserved, (1 + sheet_num + row_num) * kVMPageSize);
    EXPECT_EQ(used, kCacheLineSize + kWordSize * (sheet_num + row_num + reserved_num));
  }
}

TEST_F(MappingTableFixture, DestructorReleaseStoredPagesWithTheirAlignment)
{
  constexpr size_t kOddPageSize = 3000;  // not a power of two
  constexpr size_t kPageNum = 8;
  for (const auto page_size : {k512, k1Ki, k2Ki, k4Ki, k8Ki, k16Ki, k32Ki, k64Ki, kOddPageSize}) {
    auto table = std::make_unique<MappingTable>(page_size);
    for (size_t i = 0; i < kPageNum; ++i) {
      const auto pid = table->ReservePageID();
      table->Store(pid, ::operator new(page_size, GetAlignValOnVirtualPages(page_size)));
    }
    // sanitizers detect leaked pages or mismatched alignments
    table.reset();
  }
}

TEST_F(MappingTableFixture, DeletePageIDReuseIDsAfterGC)
{
  constexpr size_t kIDNum = 1000;
  auto gc = Builder<MappingTable::PageIDTarget>{}.SetGCInterval(kGCInterval).Build();

  std::unordered_set<uint64_t> deleted{};
  for (size_t i = 0; i < kIDNum; ++i) {
    const auto pid = table_->ReservePageID();
    table_->Store(pid, std::bit_cast<void*>(pid));  // dummy pages
    deleted.emplace(pid);
  }
  const auto [used, reserved] = table_->GetMemoryUsage();
  for (const auto pid : deleted) {
    table_->DeletePageID(pid, *gc);
  }
  ASSERT_TRUE(WaitForUsage(used - kIDNum * kWordSize));

  // deleted page IDs are reused with empty slots
  for (size_t i = 0; i < kIDNum; ++i) {
    const auto pid = table_->ReservePageID();
    EXPECT_EQ(deleted.erase(pid), 1);
    EXPECT_EQ(table_->Load(pid), nullptr);
  }
  EXPECT_EQ(table_->GetMemoryUsage().first, used);
  EXPECT_EQ(deleted.count(table_->ReservePageID()), 0);
}

TEST_F(MappingTableFixture, DeletePageIDDeferReuseWhileGuarded)
{
  constexpr size_t kWaitIntervalNum = 10;
  auto gc = Builder<MappingTable::PageIDTarget>{}.SetGCInterval(kGCInterval).Build();
  const auto pid = table_->ReservePageID();
  auto* page = std::bit_cast<void*>(pid);  // a dummy page
  table_->Store(pid, page);
  const auto [used, reserved] = table_->GetMemoryUsage();

  {
    // a thread holding the old epoch can still read the page
    const auto guard = gc->CreateEpochGuard();
    table_->DeletePageID(pid, *gc);
    std::this_thread::sleep_for(std::chrono::milliseconds{kGCInterval * kWaitIntervalNum});
    EXPECT_EQ(table_->Load(pid), page);
    EXPECT_EQ(table_->GetMemoryUsage().first, used);
  }

  ASSERT_TRUE(WaitForUsage(used - kWordSize));
  EXPECT_EQ(table_->Load(pid), nullptr);
  EXPECT_EQ(table_->ReservePageID(), pid);
}

TEST_F(MappingTableFixture, DeletePageIDWithInvalidIDsThrowRuntimeError)
{
  auto gc = Builder<MappingTable::PageIDTarget>{}.SetGCInterval(kGCInterval).Build();
  const auto pid = table_->ReservePageID();
  EXPECT_THROW(table_->DeletePageID(0, *gc), std::runtime_error);        // not a page ID
  EXPECT_THROW(table_->DeletePageID(pid + 1, *gc), std::runtime_error);  // not reserved
}

TEST_F(MappingTableFixture, ReserveAndDeletePageIDsWithMultiThreadsKeepIDsUnique)
{
  constexpr size_t kRepeatNum = 1E4;
  constexpr size_t kMaxIDNum = kThreadNum * kRepeatNum;
  static_assert(kMaxIDNum < MappingTable::kColNum * MappingTable::kRowNum);
  auto gc = Builder<MappingTable::PageIDTarget>{}.SetGCInterval(kGCInterval).Build();

  // flags for checking that each living page ID is reserved by only one thread
  std::vector<std::atomic_bool> in_use(kMaxIDNum);
  constexpr uint64_t kIDMask = (1UL << MappingTable::kRowShift) - 1;
  auto to_index = [](const uint64_t pid) {
    return ((pid >> MappingTable::kRowShift) & kIDMask) * MappingTable::kColNum + (pid & kIDMask);
  };

  std::vector<std::thread> threads{};
  for (size_t i = 0; i < kThreadNum; ++i) {
    threads.emplace_back([&]() {
      for (size_t j = 0; j < kRepeatNum; ++j) {
        const auto guard = gc->CreateEpochGuard();
        const auto pid = table_->ReservePageID();
        ASSERT_LT(to_index(pid), kMaxIDNum);
        EXPECT_FALSE(in_use[to_index(pid)].exchange(true, std::memory_order_relaxed));
        EXPECT_EQ(table_->Load(pid), nullptr);
        table_->Store(pid, std::bit_cast<void*>(pid));  // a dummy page

        in_use[to_index(pid)].store(false, std::memory_order_relaxed);
        table_->DeletePageID(pid, *gc);
      }
    });
  }
  for (auto&& t : threads) {
    t.join();
  }
  gc.reset();  // reuse all the deleted page IDs
}

}  // namespace dbgroup::memory::test
