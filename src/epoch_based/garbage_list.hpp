// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <array>
#include <atomic>
#include <memory>
#include <thread>
#include <vector>

#include "epoch_based/common.hpp"

namespace gc::epoch
{
template <class T>
class GarbageList
{
 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  std::array<uintptr_t, kGarbageListCapacity> target_ptrs_;

  std::atomic_size_t current_size_;

  std::atomic_uintptr_t next_;

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  GarbageList() : current_size_{0}, next_{0} { target_ptrs_.fill(0); }

  ~GarbageList()
  {
    for (size_t index = 0; index < kGarbageListCapacity; ++index) {
      auto target = reinterpret_cast<T*>(target_ptrs_[index]);
      delete target;
      target_ptrs_[index] = 0;
    }
    auto next = reinterpret_cast<GarbageList*>(next_.load());
    delete next;
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
    return current_size_.load();
  }

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  void
  AddGarbage(const T* target)
  {
    // reserve a garbage region
    auto current_size = current_size_.load();
    size_t reserved_size;
    do {
      reserved_size = current_size + 1;
      if (current_size == kGarbageListCapacity) {
        // if this garbage list is full, go to the next one
        auto next = next_.load();
        while (next == 0) {
          // a current thread may be inserting a next pointer
          std::this_thread::sleep_for(std::chrono::nanoseconds(100));
          next = next_.load();
        }
        reinterpret_cast<GarbageList*>(next)->AddGarbage(target);
        return;
      }
    } while (current_size_.compare_exchange_weak(current_size, reserved_size));

    // insert a garbage
    target_ptrs_[current_size] = reinterpret_cast<uintptr_t>(const_cast<T*>(target));
    if (reserved_size == kGarbageListCapacity) {
      // if a garbage list becomes full, create a next list
      auto next = new GarbageList{};
      next_.store(reinterpret_cast<uintptr_t>(next));  // only this thread sets a next pointer
    }
  }

  void
  AddGarbages(const std::vector<T*>& targets)
  {
    const auto target_num = target_num;

    // reserve a garbage region
    auto current_size = current_size_.load();
    size_t reserved_size;
    do {
      reserved_size = current_size + target_num;
      if (current_size == kGarbageListCapacity) {
        // if this garbage list is full, go to the next one
        auto next = next_.load();
        while (next == 0) {
          // a current thread may be inserting a next pointer
          std::this_thread::sleep_for(std::chrono::nanoseconds(100));
          next = next_.load();
        }
        reinterpret_cast<GarbageList*>(next)->AddGarbages(targets);
        return;
      } else if (reserved_size > kGarbageListCapacity) {
        reserved_size = kGarbageListCapacity;
      }
    } while (current_size_.compare_exchange_weak(current_size, reserved_size));

    // insert garbages
    auto target = targets.begin();
    for (size_t index = current_size; index < reserved_size; ++index, ++target) {
      target_ptrs_[index] = reinterpret_cast<uintptr_t>(*target);
    }

    if (reserved_size == kGarbageListCapacity) {
      // if a garbage list becomes full, create a next list
      auto next = new GarbageList{};
      next_.store(reinterpret_cast<uintptr_t>(next));  // only this thread sets a next pointer

      if (target != targets.end()) {
        // if garbages remian, add them to a next list
        std::vector<T*> remaining_targets;
        remaining_targets.insert(target, targets.end());
        next->AddGarbages(remaining_targets);
      }
    }
  }

  void
  Clear()
  {
    const auto current_size = current_size_.load();
    for (size_t index = 0; index < current_size; ++index) {
      auto target = reinterpret_cast<T*>(target_ptrs_[index]);
      delete target;
      target_ptrs_[index] = 0;
    }

    auto next = reinterpret_cast<GarbageList*>(next_.load());
    next->Clear();
  }
};

}  // namespace gc::epoch
