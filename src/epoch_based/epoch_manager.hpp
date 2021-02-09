// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <array>
#include <atomic>
#include <thread>

#include "epoch_based/common.hpp"

namespace gc::epoch
{
class alignas(kCacheLineSize) EpochManager
{
 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  std::array<std::array<std::atomic_uint64_t, kPartitionNum>, kBufferSize> ring_buffer_;

  std::atomic_size_t current_index_;

  size_t last_freed_index_;

  /*################################################################################################
   * Internal utility functions
   *##############################################################################################*/

  void
  ForwardEpoch()
  {
    auto previous_index = current_index_.fetch_add(1);
    if (previous_index == kBufferSize - 1) {
      current_index_ = 0;
    }
  }

  size_t
  GetFreeableEpoch()
  {
    size_t index = last_freed_index_ + 1;
    for (; index < current_index_; ++index) {
      // check each epoch has no eintering thread
      for (size_t partition = 0; partition < kPartitionNum; ++partition) {
        if (ring_buffer_[index][partition].load() != 0) {
          goto TO_RETURN;
        }
      }
    }
  TO_RETURN:
    last_freed_index_ = index - 1;
    return last_freed_index_;
  }

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  EpochManager() : current_index_{0}, last_freed_index_{0}
  {
    ring_buffer_.fill({0, 0, 0, 0, 0, 0, 0, 0});
  }

  ~EpochManager() {}

  EpochManager(const EpochManager &) = delete;
  EpochManager &operator=(const EpochManager &) = delete;
  EpochManager(EpochManager &&) = delete;
  EpochManager &operator=(EpochManager &&) = delete;

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  size_t
  EnterEpoch()
  {
    const auto thread_id = std::this_thread::get_id();
    const auto partition_id = std::hash<std::thread::id>()(thread_id) & kPartitionMask;
    const auto current_epoch = current_index_.load();

    ++ring_buffer_[current_epoch][partition_id];

    return current_epoch;
  }

  void
  LeaveEpoch(const size_t entered_epoch)
  {
    const auto thread_id = std::this_thread::get_id();
    const auto partition_id = std::hash<std::thread::id>()(thread_id) & kPartitionMask;

    --ring_buffer_[entered_epoch][partition_id];
  }
};

}  // namespace gc::epoch
