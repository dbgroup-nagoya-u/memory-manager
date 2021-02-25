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

  std::array<std::pair<size_t, T*>, kGarbageListCapacity> garbage_ring_buffer_;

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
    const auto current_end = end_index_.load(mo_relax);
    auto index = begin_index_.load(mo_relax);
    while (index != current_end) {
      auto [deleted_epoch, garbage] = garbage_ring_buffer_[index];
      delete garbage;
      index = (index == kGarbageListCapacity - 1) ? 0 : index + 1;
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
    const auto current_begin = begin_index_.load(mo_relax);
    const auto current_end = end_index_.load(mo_relax);
    const auto resized_end =
        (current_begin > current_end) ? current_end + kGarbageListCapacity : current_end;
    return resized_end - current_begin;
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
    const auto current_end = end_index_.load(mo_relax);
    const auto next_end = (current_end == kGarbageListCapacity - 1) ? 0 : current_end + 1;
    auto current_begin = begin_index_.load(mo_relax);
    while (next_end == current_begin) {
      // wait GC
      std::this_thread::sleep_for(std::chrono::nanoseconds(100));
      current_begin = begin_index_.load(mo_relax);
    }

    // add garbage
    garbage_ring_buffer_[current_end] = {current_epoch_.load(mo_relax), const_cast<T*>(garbage)};

    // set incremented index
    end_index_.store(next_end);
  }

  void
  Clear(const size_t protected_epoch)
  {
    const auto current_end = end_index_.load(mo_relax);

    auto index = begin_index_.load(mo_relax);
    while (index != current_end) {
      auto [deleted_epoch, garbage] = garbage_ring_buffer_[index];
      if (deleted_epoch < protected_epoch) {
        delete garbage;
        garbage_ring_buffer_[index] = {std::numeric_limits<size_t>::max(), nullptr};
      } else {
        break;
      }
      index = (index == kGarbageListCapacity - 1) ? 0 : index + 1;
    }

    begin_index_.store(index);
  }
};

}  // namespace dbgroup::gc::tls
