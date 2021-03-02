// Copyright (c) DB Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#include "memory/manager/tls_based_memory_manager.hpp"

#include <gtest/gtest.h>

#include <future>
#include <memory>
#include <thread>
#include <vector>

namespace dbgroup::memory::manager::component
{
using Target = size_t;

class TLSBasedMemoryManagerFixture : public ::testing::Test
{
 public:
  static constexpr size_t kGarbageListSize = 256;
  static constexpr size_t kGCInterval = 1E2;
  static constexpr size_t kThreadNum = 8;
  static constexpr size_t kLoopNum = 1E5;

  void
  AddGarbages(  //
      std::promise<std::vector<std::weak_ptr<Target>>> p,
      TLSBasedMemoryManager<std::shared_ptr<Target>> *gc)
  {
    std::vector<std::weak_ptr<Target>> target_weak_ptrs;
    for (size_t loop = 0; loop < kLoopNum; ++loop) {
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

  std::vector<std::weak_ptr<Target>>
  TestGC(  //
      TLSBasedMemoryManager<std::shared_ptr<Target>> *gc,
      const size_t thread_num)
  {
    std::vector<std::future<std::vector<std::weak_ptr<Target>>>> futures;
    for (size_t i = 0; i < thread_num; ++i) {
      std::promise<std::vector<std::weak_ptr<Target>>> p;
      futures.emplace_back(p.get_future());
      std::thread{&TLSBasedMemoryManagerFixture::AddGarbages, this, std::move(p), gc}.detach();
    }

    std::vector<std::weak_ptr<Target>> target_weak_ptrs;
    for (auto &&future : futures) {
      auto weak_ptrs = future.get();
      target_weak_ptrs.insert(target_weak_ptrs.end(), weak_ptrs.begin(), weak_ptrs.end());
    }

    return target_weak_ptrs;
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
    target_weak_ptrs = TestGC(&gc, 1);

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
    target_weak_ptrs = TestGC(&gc, kThreadNum);

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
  target_weak_ptrs = TestGC(&gc, 1);
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
  target_weak_ptrs = TestGC(&gc, kThreadNum);
  while (gc.GetRegisteredGarbageSize() > 0) {
    // wait all garbages are freed
  }

  // check there is no referece to target pointers
  for (auto &&target_weak : target_weak_ptrs) {
    EXPECT_EQ(0, target_weak.use_count());
  }
}

}  // namespace dbgroup::memory::manager::component
