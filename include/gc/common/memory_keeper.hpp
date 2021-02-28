// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <limits>
#include <memory>
#include <vector>

#include "lock_free_ring_buffer.hpp"
#include "util.hpp"

namespace dbgroup::memory::manager::component
{
template <class T>
class MemoryKeeper
{
 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  size_t page_num_;

  size_t page_size_;

  size_t partition_num_;

  T* pages_;

  std::vector<std::unique_ptr<LockFreeRingBuffer<size_t>>> partitioned_pages_;

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  MemoryKeeper(  //
      const size_t page_num,
      const size_t partition_num)
      : page_num_{page_num}, partition_num_{partition_num}
  {
    assert(page_num_ > 0);
    assert(partition_num_ > 0);
    assert(page_num_ % partition_num_ == 0);

    // reserve target pages
    pages_ = new T[page_num_];
    page_size_ = (pages_ + 1) - pages_;

    // divide pages into partitions
    partitioned_pages_.reserve(partition_num_);
    const auto pages_per_partition = page_num_ / partition_num_;
    size_t count = 0;
    for (size_t partition = 0; partition < partition_num_; ++partition) {
      std::vector<size_t> partitioned;
      partitioned.reserve(pages_per_partition);
      for (size_t i = 0; i < pages_per_partition; ++i) {
        partitioned.emplace_back(count++);
      }
      partitioned_pages_.emplace_back(new LockFreeRingBuffer<size_t>(partitioned));
    }
  }

  ~MemoryKeeper() { delete pages_; }

  MemoryKeeper(const MemoryKeeper&) = delete;
  MemoryKeeper& operator=(const MemoryKeeper&) = delete;
  MemoryKeeper(MemoryKeeper&&) = delete;
  MemoryKeeper& operator=(MemoryKeeper&&) = delete;

  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  T*
  GetPage(const size_t partition)
  {
    const auto index = partitioned_pages_[partition]->Pop();
    return &pages_[index];
  }

  void
  ReturnPage(T* page)
  {
    // eagerly initialize a page
    *page = T{};

    // check the capacity of each partition
    size_t min_partition;
    size_t min_page_num = std::numeric_limits<size_t>::max();
    for (size_t partition = 0; partition < partition_num_; ++partition) {
      const auto remaining_page_num = partitioned_pages_[partition]->Size();
      if (remaining_page_num < min_page_num) {
        min_page_num = remaining_page_num;
        min_partition = partition;
      }
    }

    // return a page
    const size_t index = (pages_ - page) / page_size_;
    partitioned_pages_[min_partition]->Push(index);
  }
};

}  // namespace dbgroup::memory::manager::component
