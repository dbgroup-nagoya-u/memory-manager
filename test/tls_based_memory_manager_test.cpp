// Copyright (c) DB Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#include "memory/manager/tls_based_memory_manager.hpp"

#include <gtest/gtest.h>

#include <future>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

namespace dbgroup::memory::manager
{
using Target = size_t;

class TLSBasedMemoryManagerFixture : public ::testing::Test
{
 public:
  static constexpr size_t kGarbageListSize = 256;
  static constexpr size_t kGCInterval = 500;
  static constexpr size_t kThreadNum = 8;
  static constexpr size_t kGarbageNumLarge = 1E5;
  static constexpr size_t kGarbageNumSmall = 10;

  std::mutex mtx;

  void
  AddGarbages(  //
      std::promise<std::vector<std::weak_ptr<Target>>> p,
      TLSBasedMemoryManager<std::shared_ptr<Target>> *gc,
      const size_t garbage_num)
  {
    std::vector<std::weak_ptr<Target>> target_weak_ptrs;
    for (size_t loop = 0; loop < garbage_num; ++loop) {
      std::shared_ptr<Target> *target_shared;
      {
        const auto guard = gc->CreateEpochGuard();
        target_shared = new std::shared_ptr<Target>(new Target{loop});
        target_weak_ptrs.emplace_back(*target_shared);
      }
      gc->AddGarbage(target_shared);
    }
    p.set_value(std::move(target_weak_ptrs));
  }

  void
  GetAndReturnPage(  //
      TLSBasedMemoryManager<Target> *memory_manager,
      const size_t page_num)
  {
    for (size_t i = 0; i < page_num; ++i) {
      auto page_addr = memory_manager->GetPage();
      auto page = new (page_addr) Target{0};
      memory_manager->AddGarbage(page);
    }
  }

  void
  KeepEpochGuard(  //
      TLSBasedMemoryManager<std::shared_ptr<Target>> *gc)
  {
    const auto guard = gc->CreateEpochGuard();
    std::unique_lock<std::mutex>{mtx};
  }

  std::vector<std::weak_ptr<Target>>
  TestGC(  //
      TLSBasedMemoryManager<std::shared_ptr<Target>> *gc,
      const size_t thread_num,
      const size_t garbage_num)
  {
    std::vector<std::future<std::vector<std::weak_ptr<Target>>>> futures;
    for (size_t i = 0; i < thread_num; ++i) {
      std::promise<std::vector<std::weak_ptr<Target>>> p;
      futures.emplace_back(p.get_future());
      std::thread{&TLSBasedMemoryManagerFixture::AddGarbages, this, std::move(p), gc, garbage_num}
          .detach();
    }

    std::vector<std::weak_ptr<Target>> target_weak_ptrs;
    for (auto &&future : futures) {
      auto weak_ptrs = future.get();
      target_weak_ptrs.insert(target_weak_ptrs.end(), weak_ptrs.begin(), weak_ptrs.end());
    }

    return target_weak_ptrs;
  }

  void
  TestMemoryManager(  //
      TLSBasedMemoryManager<Target> *memory_manager,
      const size_t thread_num,
      const size_t page_num)
  {
    std::vector<std::thread> threads;
    for (size_t i = 0; i < thread_num; ++i) {
      threads.emplace_back(std::thread{&TLSBasedMemoryManagerFixture::GetAndReturnPage, this,
                                       memory_manager, page_num});
    }
    for (auto &&thread : threads) {
      thread.join();
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

TEST_F(TLSBasedMemoryManagerFixture, Construct_GCStarted_MemberVariablesCorrectlyInitialized)
{
  auto gc = TLSBasedMemoryManager<size_t>{kGarbageListSize, kGCInterval};

  auto gc_is_running = gc.StopGC();

  EXPECT_TRUE(gc_is_running);
}

TEST_F(TLSBasedMemoryManagerFixture, Destruct_SingleThread_GarbagesCorrectlyFreed)
{
  // keep garbage targets
  std::vector<std::weak_ptr<size_t>> target_weak_ptrs;

  // register garbages to GC
  {
    auto gc = TLSBasedMemoryManager<std::shared_ptr<size_t>>{kGarbageListSize, kGCInterval};
    target_weak_ptrs = TestGC(&gc, 1, kGarbageNumLarge);

    // GC deletes all targets when it leaves this scope
  }

  // check there is no referece to target pointers
  for (auto &&target_weak : target_weak_ptrs) {
    EXPECT_EQ(0, target_weak.use_count());
  }
}

TEST_F(TLSBasedMemoryManagerFixture, Destruct_MultiThreads_GarbagesCorrectlyFreed)
{
  // keep garbage targets
  std::vector<std::weak_ptr<size_t>> target_weak_ptrs;

  // register garbages to GC
  {
    auto gc = TLSBasedMemoryManager<std::shared_ptr<size_t>>{kGarbageListSize, kGCInterval};
    target_weak_ptrs = TestGC(&gc, kThreadNum, kGarbageNumLarge);

    // GC deletes all targets when it leaves this scope
  }

  // check there is no referece to target pointers
  for (auto &&target_weak : target_weak_ptrs) {
    EXPECT_EQ(0, target_weak.use_count());
  }
}

TEST_F(TLSBasedMemoryManagerFixture, RunGC_SingleThread_GarbagesCorrectlyFreed)
{
  auto gc = TLSBasedMemoryManager<std::shared_ptr<size_t>>{kGarbageListSize, kGCInterval};

  // keep garbage targets
  std::vector<std::weak_ptr<size_t>> target_weak_ptrs;

  // register garbages to GC
  target_weak_ptrs = TestGC(&gc, 1, kGarbageNumLarge);
  while (gc.GetRegisteredGarbageSize() > 0) {
    // wait all garbages are freed
  }

  // check there is no referece to target pointers
  for (auto &&target_weak : target_weak_ptrs) {
    EXPECT_EQ(0, target_weak.use_count());
  }
}

TEST_F(TLSBasedMemoryManagerFixture, RunGC_MultiThreads_GarbagesCorrectlyFreed)
{
  auto gc = TLSBasedMemoryManager<std::shared_ptr<size_t>>{kGarbageListSize, kGCInterval};

  // keep garbage targets
  std::vector<std::weak_ptr<size_t>> target_weak_ptrs;

  // register garbages to GC
  target_weak_ptrs = TestGC(&gc, kThreadNum, kGarbageNumLarge);
  while (gc.GetRegisteredGarbageSize() > 0) {
    // wait all garbages are freed
  }

  // check there is no referece to target pointers
  for (auto &&target_weak : target_weak_ptrs) {
    EXPECT_EQ(0, target_weak.use_count());
  }
}

TEST_F(TLSBasedMemoryManagerFixture, CreateEpochGuard_SingleThread_PreventGarbagesFromDeleeting)
{
  auto gc = TLSBasedMemoryManager<std::shared_ptr<size_t>>{kGarbageListSize, kGCInterval};

  // keep garbage targets
  std::vector<std::weak_ptr<size_t>> target_weak_ptrs;

  // create an epoch guard on anther thread
  std::thread guarder;

  {
    const auto thread_lock = std::unique_lock<std::mutex>{mtx};
    guarder = std::thread{&TLSBasedMemoryManagerFixture::KeepEpochGuard, this, &gc};

    // register garbages to GC
    target_weak_ptrs = TestGC(&gc, 1, kGarbageNumSmall);

    // wait GC
    std::this_thread::sleep_for(std::chrono::microseconds(kGCInterval * 2));

    // check target pointers remain
    for (auto &&target_weak : target_weak_ptrs) {
      EXPECT_EQ(1, target_weak.use_count());
    }
  }

  guarder.join();
}

TEST_F(TLSBasedMemoryManagerFixture, CreateEpochGuard_MultiThreads_PreventGarbagesFromDeleeting)
{
  auto gc = TLSBasedMemoryManager<std::shared_ptr<size_t>>{kGarbageListSize, kGCInterval};

  // keep garbage targets
  std::vector<std::weak_ptr<size_t>> target_weak_ptrs;

  // create an epoch guard on anther thread
  std::thread guarder;

  {
    const auto thread_lock = std::unique_lock<std::mutex>{mtx};
    guarder = std::thread{&TLSBasedMemoryManagerFixture::KeepEpochGuard, this, &gc};

    // register garbages to GC
    target_weak_ptrs = TestGC(&gc, kThreadNum, kGarbageNumSmall);

    // wait GC
    std::this_thread::sleep_for(std::chrono::microseconds(kGCInterval * 2));

    // check target pointers remain
    for (auto &&target_weak : target_weak_ptrs) {
      EXPECT_EQ(1, target_weak.use_count());
    }
  }

  guarder.join();
}

TEST_F(TLSBasedMemoryManagerFixture, GetPage_WithMemoryKeeper_ReuseReservedPages)
{
  constexpr size_t kPageNum = 4096;
  constexpr size_t kLoopNum = 100;

  auto memory_manager = TLSBasedMemoryManager<Target>{kGarbageListSize, kGCInterval, true, kPageNum,
                                                      sizeof(Target),   kThreadNum,  8};

  for (size_t loop = 0; loop < kLoopNum; ++loop) {
    TestMemoryManager(&memory_manager, kThreadNum, kGarbageListSize / 2);
    std::this_thread::sleep_for(std::chrono::microseconds(kGCInterval));
  }

  // wait GC
  while (memory_manager.GetRegisteredGarbageSize() > 0) {
    std::this_thread::sleep_for(std::chrono::microseconds(kGCInterval));
  }

  // EXPECT_EQ(0, memory_manager.GetAvailablePageSize() % kPageNum);
}

}  // namespace dbgroup::memory::manager
