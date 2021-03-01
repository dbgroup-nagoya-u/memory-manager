// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <limits>
#include <memory>
#include <vector>

#include "page_stack.hpp"

namespace dbgroup::memory::manager::component
{
class MemoryKeeper
{
 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  const size_t page_num_;

  const size_t page_size_;

  const size_t page_alignment_;

  const size_t partition_num_;

  std::atomic_size_t current_capacity_;

  std::vector<void*> reserved_pages_;

  std::vector<std::unique_ptr<PageStack>> page_stacks_;

  /*################################################################################################
   * Internal utility functions
   *##############################################################################################*/

  void
  ReservePages()
  {
    // reserve target pages
    auto page_addr = malloc(page_size_ * page_num_);
    reserved_pages_.emplace_back(page_addr);

    // divide pages into partitions
    const auto pages_per_partition = page_num_ / partition_num_;
    for (size_t partition = 0; partition < partition_num_; ++partition) {
      std::vector<void*> addresses;
      addresses.reserve(pages_per_partition);
      for (size_t i = 0; i < pages_per_partition; ++i) {
        addresses.emplace_back(page_addr);
        page_addr = static_cast<std::byte*>(page_addr) + page_size_;
      }
      page_stacks_[partition]->AddPages(addresses);
    }

    current_capacity_.fetch_add(page_num_, mo_relax);
  }

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  MemoryKeeper(  //
      const size_t page_num,
      const size_t page_size,
      const size_t page_alignment,
      const size_t partition_num)
      : page_num_{page_num},
        page_size_{page_size},
        page_alignment_{page_alignment},
        partition_num_{partition_num},
        current_capacity_{0}
  {
    assert(page_num_ > 0);
    assert(partition_num_ > 0);
    assert(page_num_ % partition_num_ == 0);

    page_stacks_.reserve(partition_num_);
    for (size_t partition = 0; partition < partition_num_; ++partition) {
      page_stacks_.emplace_back();
    }

    ReservePages();
  }

  ~MemoryKeeper()
  {
    for (auto&& reserved_page : reserved_pages_) {
      free(reserved_page);
    }
  }

  MemoryKeeper(const MemoryKeeper&) = delete;
  MemoryKeeper& operator=(const MemoryKeeper&) = delete;
  MemoryKeeper(MemoryKeeper&&) = delete;
  MemoryKeeper& operator=(MemoryKeeper&&) = delete;

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  void*
  GetPage()
  {
    thread_local auto partition =
        (std::hash<std::thread::id>()(std::this_thread::get_id())) % page_num_;

    current_capacity_.fetch_add(1, mo_relax);
    return page_stacks_[partition]->GetPage();
  }

  template <class T>
  void
  ReturnPage(T* page)
  {
    // check the capacity of each partition
    size_t min_partition;
    auto min_page_num = std::numeric_limits<size_t>::max();
    for (size_t partition = 0; partition < partition_num_; ++partition) {
      const auto remaining_page_num = page_stacks_[partition]->Size();
      if (remaining_page_num < min_page_num) {
        min_page_num = remaining_page_num;
        min_partition = partition;
      }
    }

    // return a page
    page->~T();
    page_stacks_[min_partition]->AddPage(page);
    current_capacity_.fetch_sub(1, mo_relax);
  }

  void
  ReservePagesIfNeeded()
  {
    const auto current_capacity = current_capacity_.load(mo_relax);
    if (current_capacity < page_num_ * 0.7) {
      ReservePages();
    }
  }
};

}  // namespace dbgroup::memory::manager::component
