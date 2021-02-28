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
#include "memory_keeper.hpp"

namespace dbgroup::memory::manager::component
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

  MemoryKeeper<T>* memory_keeper_;

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  constexpr GarbageList()
      : garbage_ring_buffer_{},
        begin_index_{0},
        end_index_{0},
        current_epoch_{0},
        gc_interval_micro_{std::numeric_limits<size_t>::max()},
        memory_keeper_{nullptr}
  {
  }

  GarbageList(  //
      const size_t current_epoch,
      const size_t gc_interval_micro,
      MemoryKeeper<T>* memory_keeper = nullptr)
      : begin_index_{0},
        end_index_{0},
        current_epoch_{current_epoch},
        gc_interval_micro_{gc_interval_micro},
        memory_keeper_{memory_keeper}
  {
    garbage_ring_buffer_.fill({std::numeric_limits<size_t>::max(), nullptr});
  }

  ~GarbageList()
  {
    const auto current_end = end_index_.load(mo_relax);
    auto index = begin_index_.load(mo_relax);
    while (index != current_end) {
      auto [deleted_epoch, garbage] = garbage_ring_buffer_[index];
      if (memory_keeper_ == nullptr) {
        delete garbage;
      } else {
        memory_keeper_->ReturnPage(garbage);
      }
      index = (index == kGarbageListCapacity - 1) ? 0 : index + 1;
    }
  }

  GarbageList(const GarbageList&) = delete;
  GarbageList& operator=(const GarbageList&) = delete;
  GarbageList(GarbageList&&) = delete;
  GarbageList& operator=(GarbageList&&) = delete;

  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  size_t
  Size() const
  {
    const auto current_begin = begin_index_.load(mo_relax);
    const auto current_end = end_index_.load(mo_relax);

    if (current_begin <= current_end) {
      return current_end - current_begin;
    } else {
      return kGarbageListCapacity - (current_begin - current_end);
    }
  }

  void
  SetCurrentEpoch(const size_t current_epoch)
  {
    current_epoch_.store(current_epoch, mo_relax);
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
    end_index_.store(next_end, mo_relax);
  }

  void
  Clear(const size_t protected_epoch)
  {
    const auto current_end = end_index_.load(mo_relax);

    auto index = begin_index_.load(mo_relax);
    while (index != current_end) {
      auto [deleted_epoch, garbage] = garbage_ring_buffer_[index];
      if (deleted_epoch < protected_epoch) {
        garbage_ring_buffer_[index] = {std::numeric_limits<size_t>::max(), nullptr};
        if (memory_keeper_ == nullptr) {
          delete garbage;
        } else {
          memory_keeper_->ReturnPage(garbage);
        }
      } else {
        break;
      }
      index = (index == kGarbageListCapacity - 1) ? 0 : index + 1;
    }

    begin_index_.store(index, mo_relax);
  }
};

}  // namespace dbgroup::memory::manager::component
