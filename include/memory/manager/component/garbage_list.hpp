/*
 * Copyright 2021 Database Group, Nagoya University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

  std::atomic_size_t current_epoch_;

  std::atomic<GarbageList*> next_;

  MemoryKeeper* memory_keeper_;

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  constexpr GarbageList()
      : head_index_{0}, tail_index_{0}, current_epoch_{0}, next_{nullptr}, memory_keeper_{nullptr}
  {
  }

  explicit GarbageList(  //
      const size_t current_epoch,
      MemoryKeeper* memory_keeper = nullptr)
      : head_index_{0},
        tail_index_{0},
        current_epoch_{current_epoch},
        next_{nullptr},
        memory_keeper_{memory_keeper}
  {
  }

  ~GarbageList()
  {
    const auto current_head = head_index_.load(mo_relax);
    for (size_t index = tail_index_; index < current_head; ++index) {
      delete garbages_[index].second;
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
    const auto size = head_index_.load(mo_relax) - tail_index_;
    const auto next = next_.load(mo_relax);
    if (next == nullptr) {
      return size;
    } else {
      return next->Size() + size;
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

  static GarbageList*
  AddGarbage(  //
      GarbageList* garbage_list,
      const T* garbage)
  {
    const auto current_head = garbage_list->head_index_.load(mo_relax);
    const auto current_epoch = garbage_list->current_epoch_.load(mo_relax);
    garbage_list->garbages_[current_head] = {current_epoch, const_cast<T*>(garbage)};
    garbage_list->head_index_.fetch_add(1, mo_relax);

    if (current_head < kGarbageBufferSize - 1) {
      return garbage_list;
    } else {
      const auto new_garbage_list = new GarbageList{current_epoch, garbage_list->memory_keeper_};
      garbage_list->next_.store(new_garbage_list, mo_relax);
      return new_garbage_list;
    }
  }

  static GarbageList*
  Clear(  //
      GarbageList* garbage_list,
      const size_t protected_epoch)
  {
    if (garbage_list == nullptr) {
      return nullptr;
    }

    const auto current_head = garbage_list->head_index_.load(mo_relax);
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

    garbage_list->tail_index_ = index;
    if (index < kGarbageBufferSize) {
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
