// Copyright (c) DB Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#include "memory/manager/component/page_stack.hpp"

#include <gtest/gtest.h>

#include <future>
#include <memory>
#include <thread>
#include <vector>

namespace dbgroup::memory::manager::component
{
using Target = size_t;

class PageStackFixture : public ::testing::Test
{
 public:
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

TEST_F(PageStackFixture, Construct_DefaultConstructor_MemberVariablesCorrectlyInitialized)
{
  auto stack = PageStack{};

  EXPECT_EQ(0, stack.Size());
}

TEST_F(PageStackFixture, AddPage_AddTenPages_StackSizeCorrectlyUpdated)
{
  constexpr size_t kPageNum = 10;

  auto stack = PageStack{};
  std::vector<std::unique_ptr<Target>> reserved_pages;

  for (size_t count = 1; count <= kPageNum; ++count) {
    auto page = std::make_unique<Target>(0);
    stack.AddPage(page.get());

    EXPECT_EQ(count, stack.Size());

    reserved_pages.emplace_back(std::move(page));
  }
}

TEST_F(PageStackFixture, AddPages_AddTenPages_StackSizeCorrectlyUpdated)
{
  constexpr size_t kPageNum = 10;

  auto stack = PageStack{};
  std::vector<void*> page_addresses;
  std::vector<std::unique_ptr<Target>> reserved_pages;

  for (size_t count = 1; count <= kPageNum; ++count) {
    auto page = std::make_unique<Target>(0);
    page_addresses.emplace_back(page.get());
    reserved_pages.emplace_back(std::move(page));
  }

  stack.AddPages(page_addresses);

  EXPECT_EQ(kPageNum, stack.Size());
}

TEST_F(PageStackFixture, GetPage_AddAndGetTenPages_GetAddedPageAddresses)
{
  constexpr size_t kPageNum = 10;

  auto stack = PageStack{};
  std::vector<void*> page_addresses;
  std::vector<std::unique_ptr<Target>> reserved_pages;

  for (size_t count = 1; count <= kPageNum; ++count) {
    auto page = std::make_unique<Target>(0);
    page_addresses.emplace_back(page.get());
    reserved_pages.emplace_back(std::move(page));
  }
  stack.AddPages(page_addresses);

  for (size_t count = 1; count <= kPageNum; ++count) {
    auto page = stack.GetPage();

    const auto remaining_size = kPageNum - count;
    EXPECT_EQ(remaining_size, stack.Size());
    EXPECT_EQ(page_addresses[remaining_size], page);
  }
}

}  // namespace dbgroup::memory::manager::component
