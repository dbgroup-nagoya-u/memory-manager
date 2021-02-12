// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <array>
#include <atomic>
#include <thread>
#include <utility>

#include "epoch_based/common.hpp"

namespace gc::epoch
{
class alignas(kCacheLineSize) EpochManager
{
 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  std::array<std::array<std::atomic_size_t, kPartitionNum>, kBufferSize> epoch_ring_buffer_;

  std::atomic_size_t current_index_;

  size_t check_begin_index_;

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  EpochManager() : current_index_{0}, check_begin_index_{0}
  {
    for (size_t epoch = 0; epoch < kBufferSize; ++epoch) {
      for (size_t partition = 0; partition < kPartitionNum; ++partition) {
        epoch_ring_buffer_[epoch][partition] = 0;
      }
    }
  }

  ~EpochManager() {}

  EpochManager(const EpochManager &) = delete;
  EpochManager &operator=(const EpochManager &) = delete;
  EpochManager(EpochManager &&) = delete;
  EpochManager &operator=(EpochManager &&) = delete;

  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  size_t
  GetCurrentEpoch() const
  {
    return current_index_.load();
  }

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  std::pair<size_t, size_t>
  EnterEpoch()
  {
    const auto thread_id = std::this_thread::get_id();
    const auto partition_id = std::hash<std::thread::id>()(thread_id) & kPartitionMask;
    const auto current_epoch = current_index_.load();

    ++epoch_ring_buffer_[current_epoch][partition_id];

    return {current_epoch, partition_id};
  }

  void
  LeaveEpoch(  //
      const size_t entered_epoch,
      const size_t partition_id)
  {
    --epoch_ring_buffer_[entered_epoch][partition_id];
  }

  void
  ForwardEpoch()
  {
    const auto previous_index = current_index_.load();
    const auto next_index = (previous_index == kBufferSize - 1) ? 0 : previous_index + 1;

    current_index_ = next_index;
  }

  std::pair<size_t, size_t>
  ListFreeableEpoch()
  {
    const size_t freeable_begin_index = check_begin_index_;
    size_t index = freeable_begin_index;
    while (index != current_index_) {
      // check each epoch has no entering thread
      for (size_t partition = 0; partition < kPartitionNum; ++partition) {
        if (epoch_ring_buffer_[index][partition].load() != 0) {
          goto TO_RETURN;
        }
      }
      index = (index == kBufferSize - 1) ? 0 : index + 1;
    }
  TO_RETURN:
    check_begin_index_ = index;
    return {freeable_begin_index, index};
  }
};

}  // namespace gc::epoch
