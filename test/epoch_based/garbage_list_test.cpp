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

TEST_F(GarbageListFixture, Construct_ArgTargetPointer_MemberVariableCorrectlyInitialized)
{
  // create GC targets
  const auto target_ptr_1 = new size_t{1};
  const auto target_ptr_2 = new size_t{2};

  // create a garbage item
  const auto garbage_tail = new GarbageList{target_ptr_1};

  EXPECT_EQ(nullptr, garbage_tail->Next());

  // create a garbage list
  const auto garbage_head = GarbageList{target_ptr_2, garbage_tail};

  EXPECT_EQ(garbage_tail, garbage_head.Next());
}

TEST_F(GarbageListFixture, SetNext_SwapNullptrForNextGarbage_ReferenceCorrectNext)
{
  // create GC targets
  const auto target_ptr_1 = new size_t{1};
  const auto target_ptr_2 = new size_t{2};

  // create a garbage item
  auto garbage_head = GarbageList{target_ptr_1};

  EXPECT_EQ(nullptr, garbage_head.Next());

  // expand a garbage item to a list
  const auto garbage_tail = new GarbageList{target_ptr_2};
  garbage_head.SetNext(garbage_tail);

  EXPECT_EQ(garbage_tail, garbage_head.Next());
  EXPECT_NE(nullptr, garbage_head.Next());
}

TEST_F(GarbageListFixture, Destruct_SetOne_AllocatedValueFreed)
{
  // create GC targets
  const auto target_ptr_1 = new size_t{1};
  const auto target_ptr_2 = new size_t{2};

  // create shared and weak pointers to track GC
  const auto target_shared_1 = new std::shared_ptr<size_t>(target_ptr_1);
  const auto target_shared_2 = new std::shared_ptr<size_t>(target_ptr_2);
  const std::weak_ptr<size_t> target_weak_1 = *target_shared_1;
  const std::weak_ptr<size_t> target_weak_2 = *target_shared_2;

  {
    // create a garbage list
    const auto garbage_tail = new GarbageList{target_shared_1};
    const auto garbage_head = GarbageList{target_shared_2, garbage_tail};

    // a garbage list delete GC targets when it leaves this scope
  }

  // check there is no referece to target pointers
  EXPECT_EQ(0, target_weak_1.use_count());
  EXPECT_EQ(0, target_weak_2.use_count());
}

}  // namespace gc::epoch
