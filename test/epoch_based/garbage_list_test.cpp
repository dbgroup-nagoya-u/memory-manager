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

  {
    // create a garbage list
    const auto garbage_tail = new GarbageList{target_ptr_1};
    const auto garbage_head = GarbageList{target_ptr_2, garbage_tail};

    // a garbage list delete GC targets when it leaves this scope
  }

  // this check is danger. these lines may fail due to freed memory status
  EXPECT_NE(1, *target_ptr_1);
  EXPECT_NE(2, *target_ptr_2);
}

}  // namespace gc::epoch
