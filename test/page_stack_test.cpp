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
  static constexpr size_t kPageNum = 1000;
  static constexpr size_t kThreadNum = 8;

  void
  AddPages(  //
      std::promise<std::vector<std::unique_ptr<Target>>> p,
      PageStack *stack,
      const size_t page_num)
  {
    std::vector<std::unique_ptr<Target>> reserved_pages;
    for (size_t count = 0; count < page_num; ++count) {
      auto page = std::make_unique<Target>(0);
      stack->AddPage(page.get());
      reserved_pages.emplace_back(std::move(page));
    }

    p.set_value(std::move(reserved_pages));
  }

  std::vector<std::unique_ptr<Target>>
  AddPagesFromMultiThreads(  //
      PageStack *stack,
      const size_t page_num,
      const size_t thread_num)
  {
    std::vector<std::unique_ptr<Target>> reserved_pages;

    std::vector<std::future<std::vector<std::unique_ptr<Target>>>> futures;
    for (size_t i = 0; i < thread_num; ++i) {
      std::promise<std::vector<std::unique_ptr<Target>>> p;
      futures.emplace_back(p.get_future());
      std::thread{&PageStackFixture::AddPages, this, std::move(p), stack, page_num}.detach();
    }
    for (auto &&f : futures) {
      auto pages = f.get();
      for (auto &&page : pages) {
        reserved_pages.emplace_back(std::move(page));
      }
    }

    return reserved_pages;
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

TEST_F(PageStackFixture, Construct_DefaultConstructor_MemberVariablesCorrectlyInitialized)
{
  auto stack = PageStack{};

  EXPECT_EQ(0, stack.Size());
}

TEST_F(PageStackFixture, AddPage_SingleThread_StackSizeCorrectlyUpdated)
{
  auto stack = PageStack{};
  std::vector<std::unique_ptr<Target>> reserved_pages;

  for (size_t count = 1; count <= kPageNum; ++count) {
    auto page = std::make_unique<Target>(0);
    stack.AddPage(page.get());

    EXPECT_EQ(count, stack.Size());

    reserved_pages.emplace_back(std::move(page));
  }
}

TEST_F(PageStackFixture, AddPage_MultiThreads_StackSizeCorrectlyIncremented)
{
  auto stack = PageStack{};
  auto reserved_pages = AddPagesFromMultiThreads(&stack, kPageNum, kThreadNum);

  EXPECT_EQ(kPageNum * kThreadNum, stack.Size());
}

TEST_F(PageStackFixture, AddPages_SingleThread_StackSizeCorrectlyUpdated)
{
  auto stack = PageStack{};
  std::vector<void *> page_addresses;
  std::vector<std::unique_ptr<Target>> reserved_pages;

  for (size_t count = 1; count <= kPageNum; ++count) {
    auto page = std::make_unique<Target>(0);
    page_addresses.emplace_back(page.get());
    reserved_pages.emplace_back(std::move(page));
  }

  stack.AddPages(page_addresses);

  EXPECT_EQ(kPageNum, stack.Size());
}

TEST_F(PageStackFixture, GetPage_SingleThread_GetAddedPageAddresses)
{
  auto stack = PageStack{};
  auto reserved_pages = AddPagesFromMultiThreads(&stack, kPageNum, 1);

  for (size_t count = 1; count <= kPageNum; ++count) {
    auto page = stack.GetPage();

    const auto remaining_size = kPageNum - count;
    EXPECT_EQ(remaining_size, stack.Size());
    EXPECT_EQ(reserved_pages[remaining_size].get(), page);
  }
}

TEST_F(PageStackFixture, GetPage_MultiThreads_GetAddedPageAddresses)
{
  auto stack = PageStack{};
  auto reserved_pages = AddPagesFromMultiThreads(&stack, kPageNum, kThreadNum);

  auto f = [&]() {
    for (size_t i = 0; i < kPageNum; ++i) {
      stack.GetPage();
    }
  };

  std::vector<std::thread> threads;
  for (size_t i = 0; i < kThreadNum; ++i) {
    threads.emplace_back(std::thread{f});
  }
  for (auto &&t : threads) {
    t.join();
  }

  EXPECT_EQ(0, stack.Size());
}

}  // namespace dbgroup::memory::manager::component
