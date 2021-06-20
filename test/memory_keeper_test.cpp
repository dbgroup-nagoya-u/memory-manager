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

#include "memory/manager/component/memory_keeper.hpp"

#include <gtest/gtest.h>

#include <future>
#include <memory>
#include <thread>
#include <vector>

namespace dbgroup::memory::manager::component
{
using Target = size_t;

class MemoryKeeperFixture : public ::testing::Test
{
 public:
  static constexpr size_t kThreadNum = 8;
  static constexpr size_t kPageNumPerThread = 1000;
  static constexpr size_t kReservePageNumPerThread = kPageNumPerThread * kThreadNum;
  static constexpr size_t kPageNum = kReservePageNumPerThread * kThreadNum;
  static constexpr size_t kPageSize = 8;
  static constexpr size_t kPageAlignment = 8;

  void
  GetPages(  //
      std::promise<std::vector<void *>> p,
      MemoryKeeper *keeper,
      const size_t page_num)
  {
    std::vector<void *> allocated_pages;
    for (size_t count = 0; count < page_num; ++count) {
      allocated_pages.emplace_back(keeper->GetPage());
    }

    p.set_value(std::move(allocated_pages));
  }

  void
  ReturnPages(  //
      MemoryKeeper *keeper,
      const std::vector<void *> &pages)
  {
    for (auto &&page : pages) {
      keeper->ReturnPage(static_cast<Target *>(page));
    }
  }

  std::vector<void *>
  GetPagesFromMultiThreads(  //
      MemoryKeeper *keeper,
      const size_t page_num,
      const size_t thread_num)
  {
    std::vector<void *> allocated_pages;

    std::vector<std::future<std::vector<void *>>> futures;
    for (size_t i = 0; i < thread_num; ++i) {
      std::promise<std::vector<void *>> p;
      futures.emplace_back(p.get_future());
      std::thread{&MemoryKeeperFixture::GetPages, this, std::move(p), keeper, page_num}.detach();
    }
    for (auto &&f : futures) {
      auto pages = f.get();
      allocated_pages.insert(allocated_pages.end(), pages.begin(), pages.end());
    }

    return allocated_pages;
  }

  void
  ReturnPagesFromMultiThreads(  //
      MemoryKeeper *keeper,
      const std::vector<std::vector<void *>> &pages)
  {
    std::vector<std::thread> threads;
    for (auto &&pages_per_thread : pages) {
      threads.emplace_back(&MemoryKeeperFixture::ReturnPages, this, keeper, pages_per_thread);
    }
    for (auto &&thread : threads) {
      thread.join();
    }
  }

  void
  RunMemoryReserver(  //
      MemoryKeeper *keeper,
      std::atomic_bool *running)
  {
    while (running->load()) {
      std::this_thread::sleep_for(std::chrono::microseconds(500));
      keeper->ReservePagesIfNeeded();
    }
  }

 protected:
  void
  SetUp() override
  {
  }

  void
  TearDown() override
  {
  }
};

/*--------------------------------------------------------------------------------------------------
 * Public utility tests
 *------------------------------------------------------------------------------------------------*/

TEST_F(MemoryKeeperFixture, Construct_DefaultConstructor_MemberVariablesCorrectlyInitialized)
{
  auto keeper = MemoryKeeper{kPageNum, kPageSize, kPageAlignment, kThreadNum};

  EXPECT_EQ(kPageNum, keeper.GetCurrentCapacity());
}

TEST_F(MemoryKeeperFixture, GetPage_SingleThread_GetUniquePages)
{
  auto keeper = MemoryKeeper{kPageNum, kPageSize, kPageAlignment, kThreadNum};

  auto allocated_pages = GetPagesFromMultiThreads(&keeper, kPageNumPerThread, 1);

  EXPECT_EQ(kPageNum - kPageNumPerThread, keeper.GetCurrentCapacity());
  for (size_t i = 0; i < allocated_pages.size(); ++i) {
    for (size_t j = i + 1; j < allocated_pages.size(); ++j) {
      EXPECT_NE(allocated_pages[i], allocated_pages[j]);
    }
  }
}

TEST_F(MemoryKeeperFixture, GetPage_MultiThreads_GetUniquePages)
{
  auto keeper = MemoryKeeper{kPageNum, kPageSize, kPageAlignment, kThreadNum};

  auto allocated_pages = GetPagesFromMultiThreads(&keeper, kPageNumPerThread, kThreadNum);

  EXPECT_EQ(kPageNum - kPageNumPerThread * kThreadNum, keeper.GetCurrentCapacity());
  for (size_t i = 0; i < allocated_pages.size(); ++i) {
    for (size_t j = i + 1; j < allocated_pages.size(); ++j) {
      EXPECT_NE(allocated_pages[i], allocated_pages[j]);
    }
  }
}

TEST_F(MemoryKeeperFixture, ReturnPage_SingleThread_PageCapacityCorrectlyUpdated)
{
  auto keeper = MemoryKeeper{kPageNum, kPageSize, kPageAlignment, kThreadNum};

  std::vector<std::vector<void *>> allocated_pages;
  allocated_pages.emplace_back(GetPagesFromMultiThreads(&keeper, kPageNumPerThread, 1));
  ReturnPagesFromMultiThreads(&keeper, allocated_pages);

  EXPECT_EQ(kPageNum, keeper.GetCurrentCapacity());
}

TEST_F(MemoryKeeperFixture, ReturnPage_MultiThreads_PageCapacityCorrectlyUpdated)
{
  auto keeper = MemoryKeeper{kPageNum, kPageSize, kPageAlignment, kThreadNum};

  std::vector<std::vector<void *>> allocated_pages;
  for (size_t i = 0; i < kThreadNum; ++i) {
    allocated_pages.emplace_back(GetPagesFromMultiThreads(&keeper, kPageNumPerThread, 1));
  }
  ReturnPagesFromMultiThreads(&keeper, allocated_pages);

  EXPECT_EQ(kPageNum, keeper.GetCurrentCapacity());
}

TEST_F(MemoryKeeperFixture, ReservePages_KeeperHasSufficentPages_PagesNotReserved)
{
  auto keeper = MemoryKeeper{kPageNum, kPageSize, kPageAlignment, kThreadNum};

  auto running = std::atomic_bool{true};
  auto reserver = std::thread{&MemoryKeeperFixture::RunMemoryReserver, this, &keeper, &running};

  GetPagesFromMultiThreads(&keeper, kPageNumPerThread, 1);

  running.store(false);
  reserver.join();

  EXPECT_EQ(kPageNum - kPageNumPerThread, keeper.GetCurrentCapacity());
}

TEST_F(MemoryKeeperFixture, ReservePages_KeeperHasUnsufficentPages_PagesReserved)
{
  auto keeper = MemoryKeeper{kPageNum, kPageSize, kPageAlignment, kThreadNum};

  auto running = std::atomic_bool{true};
  auto reserver = std::thread{&MemoryKeeperFixture::RunMemoryReserver, this, &keeper, &running};

  GetPagesFromMultiThreads(&keeper, kReservePageNumPerThread, kThreadNum);

  running.store(false);
  reserver.join();

  EXPECT_GT(keeper.GetCurrentCapacity(), 0);
}

}  // namespace dbgroup::memory::manager::component
