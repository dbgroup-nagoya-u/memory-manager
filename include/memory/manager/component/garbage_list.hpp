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

  std::array<std::pair<size_t, T*>, kGarbageBufferSize> garbages_;

  std::atomic_size_t head_index_;

  size_t tail_index_;

  std::atomic<GarbageList*> next_;

  MemoryKeeper* memory_keeper_;

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  constexpr GarbageList() : head_index_{0}, tail_index_{0}, next_{nullptr}, memory_keeper_{nullptr}
  {
  }

  explicit GarbageList(MemoryKeeper* memory_keeper)
      : head_index_{0}, tail_index_{0}, next_{nullptr}, memory_keeper_{memory_keeper}
  {
  }

  ~GarbageList() = default;

  GarbageList(const GarbageList&) = delete;
  GarbageList& operator=(const GarbageList&) = delete;
  GarbageList(GarbageList&&) = delete;
  GarbageList& operator=(GarbageList&&) = delete;

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  GarbageList*
  AddGarbage(  //
      const size_t current_epoch,
      const T* garbage)
  {
    const auto current_head = head_index_.load(mo_relax);
    garbages_[current_head] = {current_epoch, garbage};
    head_index_.store(1, mo_relax);

    if (current_head < kGarbageBufferSize - 1) {
      return this;
    } else {
      const auto new_garbage_list = new GarbageList{memory_keeper_};
      next_.store(new_garbage_list, mo_relax);
      return new_garbage_list;
    }
  }

  static GarbageList*
  Clear(  //
      GarbageList* garbage_list,
      const size_t protected_epoch)
  {
    const auto current_head = head_index_.load(mo_relax);
    auto memory_keeper = garbage_list->memory_keeper_;

    auto index = garbage_list->tail_index_;
    for (; index < current_head; ++index) {
      const auto [epoch, garbage] = garbage_list->garbages_[index];
      if (epoch < protected_epoch) {
        if (memory_keeper == nullptr) {
          delete garbage;
        } else {
          memory_keeper->ReturnPage(garbage);
        }
      } else {
        break;
      }
    }

    if (index < kGarbageBufferSize) {
      garbage_list->tail_index_ = index;
      return garbage_list;
    } else {
      auto next_list = garbage_list->next_.load(mo_relax);
      while (next_list == nullptr) {
        next_list = garbage_list->next_.load(mo_relax);
      }
      delete garbage_list;
      return GarbageList::Clear(next_list, protected_epoch);
    }
  }
};

}  // namespace dbgroup::memory::manager::component
