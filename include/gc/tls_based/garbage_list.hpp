// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <array>
#include <atomic>
#include <limits>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

#include "common.hpp"

namespace dbgroup::gc::tls
{
template <class T>
class GarbageList
{
 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  std::array<std::pair<size_t, T*>, kBufferSize> garbage_ring_buffer_;

  std::atomic_size_t begin_index_;

  std::atomic_size_t end_index_;

  std::atomic_size_t current_epoch_;

  const size_t gc_interval_micro_;

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  GarbageList(const size_t current_epoch, const size_t gc_interval_micro)
      : begin_index_{0},
        end_index_{0},
        current_epoch_{current_epoch},
        gc_interval_micro_{gc_interval_micro}
  {
    garbage_ring_buffer_.fill({std::numeric_limits<size_t>::max(), nullptr});
  }

  ~GarbageList()
  {
    const auto current_end = end_index_.load();
    for (auto index = begin_index_.load(); index != current_end; ++index) {
      auto [deleted_epoch, garbage] = garbage_ring_buffer_[index];
      delete garbage;
    }
  }

  GarbageList(const GarbageList&) = delete;
  GarbageList& operator=(const GarbageList&) = delete;
  GarbageList(GarbageList&&) = default;
  GarbageList& operator=(GarbageList&&) = default;

  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  size_t
  Size() const
  {
    const int64_t current_begin = begin_index_.load();
    const int64_t current_end = end_index_.load();
    return std::abs(current_end - current_begin);
  }

  void
  SetCurrentEpoch(const size_t current_epoch)
  {
    current_epoch_.store(current_epoch);
  }

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  void
  AddGarbage(const T* garbage)
  {
    // reserve a garbage region
    const auto current_end = end_index_.load();
    const auto next_end = (current_end == kBufferSize) ? 0 : current_end + 1;
    auto current_begin = begin_index_.load();
    while (next_end == current_begin) {
      // wait GC
      std::this_thread::sleep_for(std::chrono::microseconds(gc_interval_micro_));
      current_begin = begin_index_.load();
    }

    // add garbage
    garbage_ring_buffer_[current_end] = {current_epoch_.load(), const_cast<T*>(garbage)};

    // set incremented index
    end_index_.store(next_end);
  }

  void
  Clear(const size_t protected_epoch)
  {
    const auto current_end = end_index_.load();

    auto index = begin_index_.load();
    while (index != current_end) {
      auto [deleted_epoch, garbage] = garbage_ring_buffer_[index];
      if (deleted_epoch < protected_epoch) {
        delete garbage;
        garbage_ring_buffer_[index] = {std::numeric_limits<size_t>::max(), nullptr};
      } else {
        break;
      }
      index = (index + 1 == kBufferSize) ? 0 : index + 1;
    }

    begin_index_.store(index);
  }
};

}  // namespace dbgroup::gc::tls
