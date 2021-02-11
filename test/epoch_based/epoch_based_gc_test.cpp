// Copyright (c) DB Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#include "gc/epoch_based_gc.hpp"

#include <thread>
#include <vector>

#include "gtest/gtest.h"

namespace gc::epoch
{
class EpochBasedGCFixture : public ::testing::Test
{
 public:
  static constexpr size_t kDefaultGCInterval = 1;

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

TEST_F(EpochBasedGCFixture, RunGC_AddGarbagesFromSingleThread_AllTargetsAreDeleted)
{
  // keep garbage targets
  std::vector<size_t*> target_ptrs;

  // register garbages to GC
  {
    auto gc = EpochBasedGC<size_t>{kDefaultGCInterval};

    for (size_t loop = 0; loop < 100; ++loop) {
      const auto guard = gc.CreateEpochGuard();
      target_ptrs.emplace_back(new size_t{loop + 1});
      gc.AddGarbage(guard, target_ptrs.back());
      std::this_thread::sleep_for(std::chrono::microseconds(10));
    }

    // GC deletes all targets when it leaves this scope
  }

  // wait GC
  std::this_thread::sleep_for(std::chrono::milliseconds(10 * kDefaultGCInterval));

  // this check is danger: these lines may fail due to freed memory space
  for (size_t loop = 0; loop < 100; ++loop) {
    const auto expected = loop + 1;
    const auto actual = *target_ptrs[loop];
    EXPECT_NE(expected, actual);
  }
}

}  // namespace gc::epoch
