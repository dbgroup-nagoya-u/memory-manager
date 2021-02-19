// Copyright (c) DB Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#include "epoch_based/garbage_list.hpp"

#include <thread>
#include <vector>

#include "gtest/gtest.h"

namespace gc::epoch
{
class GarbageListFixture : public ::testing::Test
{
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

TEST_F(GarbageListFixture, Construct_NoArgs_MemberVariableCorrectlyInitialized)
{
  const auto garbage_list = GarbageList<size_t>{};

  EXPECT_EQ(0, garbage_list.Size());
  EXPECT_EQ(nullptr, garbage_list.Next());
}

TEST_F(GarbageListFixture, Destruct_AddTenGarbages_AllocatedValueFreed)
{
  constexpr auto kGarbageNum = 10UL;

  // create GC targets
  std::vector<size_t *> targets;
  for (size_t index = 0; index < kGarbageNum; ++index) {
    targets.push_back(new size_t{index});
  }

  // create shared/weak pointers
  std::vector<std::shared_ptr<size_t> *> shared_targets;
  std::vector<std::weak_ptr<size_t>> weak_targets;
  for (auto &&target : targets) {
    const auto shared_target = new std::shared_ptr<size_t>{target};
    shared_targets.push_back(shared_target);
    weak_targets.emplace_back(*shared_target);
  }

  {
    // create a garbage list
    auto garbage_list = GarbageList<std::shared_ptr<size_t>>{};

    // add garbages
    for (auto &&target : shared_targets) {
      garbage_list.AddGarbage(target);
    }

    // a garbage list deletes all the GC targets when it leaves this scope
  }

  // check there is no referece to target pointers
  for (auto &&weak_ptr : weak_targets) {
    EXPECT_EQ(0, weak_ptr.use_count());
  }
}

TEST_F(GarbageListFixture, AddGarbage_TenGarbages_ListSizeCorrectlyIncremented)
{
  constexpr auto kGarbageNum = 10UL;

  // create GC targets
  std::vector<size_t *> targets;
  for (size_t index = 0; index < kGarbageNum; ++index) {
    targets.push_back(new size_t{index});
  }

  // create a garbage list
  auto garbage_list = GarbageList<size_t>{};

  // add garbages
  for (size_t count = 0; count < kGarbageNum; ++count) {
    garbage_list.AddGarbage(targets[count]);
    EXPECT_EQ(count + 1, garbage_list.Size());
  }
}

TEST_F(GarbageListFixture, AddGarbage_ManyGarbages_NextGarbageListIsCreated)
{
  constexpr auto kGarbageNum = kGarbageListCapacity + 1;

  // create GC targets
  std::vector<size_t *> targets;
  for (size_t index = 0; index < kGarbageNum; ++index) {
    targets.push_back(new size_t{index});
  }

  // create a garbage list
  auto garbage_list = GarbageList<size_t>{};

  // add garbages
  for (auto &&target : targets) {
    garbage_list.AddGarbage(target);
  }

  const auto next_garbage_list = garbage_list.Next();
  EXPECT_NE(nullptr, next_garbage_list);
  EXPECT_EQ(1, next_garbage_list->Size());
}

TEST_F(GarbageListFixture, AddGarbages_TenGarbages_ListSizeCorrectlyIncremented)
{
  constexpr auto kGarbageNum = 10UL;

  // create GC targets
  std::vector<size_t *> targets;
  for (size_t index = 0; index < kGarbageNum; ++index) {
    targets.push_back(new size_t{index});
  }

  // create a garbage list
  auto garbage_list = GarbageList<size_t>{};

  // add garbages
  for (size_t count = 0; count < kGarbageNum; count += 2) {
    std::vector<size_t *> garbages = {targets[count], targets[count + 1]};

    garbage_list.AddGarbages(garbages);
    EXPECT_EQ(count + 2, garbage_list.Size());
  }
}

TEST_F(GarbageListFixture, AddGarbages_ManyGarbages_NextGarbageListIsCreated)
{
  constexpr auto kGarbageNum = kGarbageListCapacity + 1;

  // create GC targets
  std::vector<size_t *> targets;
  for (size_t index = 0; index < kGarbageNum; ++index) {
    targets.push_back(new size_t{index});
  }

  // create a garbage list
  auto garbage_list = GarbageList<size_t>{};

  // add garbages
  garbage_list.AddGarbages(targets);

  const auto next_garbage_list = garbage_list.Next();
  EXPECT_NE(nullptr, next_garbage_list);
  EXPECT_EQ(1, next_garbage_list->Size());
}

}  // namespace gc::epoch
