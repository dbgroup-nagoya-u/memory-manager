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
  const auto target_ptr_1 = new size_t{1};
  const auto target_ptr_2 = new size_t{2};

  const auto garbage_1 = GarbageList{target_ptr_1};

  EXPECT_EQ(nullptr, garbage_1.Next());

  const auto garbage_2 = GarbageList{target_ptr_2, &garbage_1};

  EXPECT_EQ(&garbage_1, garbage_2.Next());
}

}  // namespace gc::epoch
