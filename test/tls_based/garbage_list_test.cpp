// Copyright (c) DB Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#include "gc/tls_based/garbage_list.hpp"

#include <gtest/gtest.h>

#include <future>
#include <memory>
#include <thread>
#include <vector>

namespace dbgroup::gc::tls
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

TEST_F(GarbageListFixture, Construct_WithArgs_MemberVariablesCorrectlyInitialized)
{
  constexpr auto kGCInterval = 100UL;

  auto garbage_list = GarbageList<size_t>{0, kGCInterval};

  EXPECT_EQ(0, garbage_list.Size());
}

TEST_F(GarbageListFixture, AddGarbage_TenGarbages_ListSizeCorrectlyUpdated)
{
  constexpr auto kGCInterval = 100UL;
  constexpr auto kGarbageNum = 10UL;

  auto garbage_list = GarbageList<size_t>{0, kGCInterval};
  for (size_t count = 0; count < kGarbageNum; ++count) {
    auto garbage = new size_t{0};
    garbage_list.AddGarbage(garbage);
  }

  EXPECT_EQ(kGarbageNum, garbage_list.Size());
}

}  // namespace dbgroup::gc::tls
