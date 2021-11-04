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

#ifndef MEMORY_MANAGER_MEMORY_COMPONENT_GARBAGE_LIST_H_
#define MEMORY_MANAGER_MEMORY_COMPONENT_GARBAGE_LIST_H_

#include <array>
#include <atomic>
#include <limits>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

#include "common.hpp"

namespace dbgroup::memory::component
{
/**
 * @brief A class to represent a buffer of garbage instances.
 *
 * @tparam T a target class of garbage collection.
 */
template <class T>
class GarbageList
{
 public:
  /*################################################################################################
   * Public constructors and assignment operators
   *##############################################################################################*/

  /**
   * @brief Construct a new instance.
   *
   * @param current_epoch an initial epoch value.
   */
  constexpr explicit GarbageList(const size_t current_epoch)
      : head_index_{0}, tail_index_{0}, current_epoch_{current_epoch}, next_{nullptr}
  {
  }

  GarbageList(const GarbageList&) = delete;
  GarbageList& operator=(const GarbageList&) = delete;
  GarbageList(GarbageList&&) = delete;
  GarbageList& operator=(GarbageList&&) = delete;

  /*################################################################################################
   * Public destructors
   *##############################################################################################*/

  /**
   * @brief Destroy the instance.
   *
   */
  ~GarbageList()
  {
    const auto current_head = head_index_.load(kMORelax);
    for (size_t index = tail_index_; index < current_head; ++index) {
      Delete(garbages_[index].second);
    }
  }

  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  /**
   * @return size_t the number of garbases in entire lists.
   */
  size_t
  Size() const
  {
    const auto size = head_index_.load(kMORelax) - tail_index_;
    const auto next = next_.load(kMORelax);
    if (next == nullptr) {
      return size;
    } else {
      return next->Size() + size;
    }
  }

  /**
   * @brief Set a current epoch value.
   *
   * @param current_epoch a epoch value to be set.
   */
  void
  SetCurrentEpoch(const size_t current_epoch)
  {
    current_epoch_.store(current_epoch, kMORelax);

    auto next_list = next_.load(kMORelax);
    if (next_list != nullptr) {
      next_list->SetCurrentEpoch(current_epoch);
    }
  }

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  /**
   * @brief Add a new garbage instance to a specified buffer.
   *
   * If the buffer becomes full, create a new garbage buffer and link them.
   *
   * @param garbage_list a garbage buffer to be added.
   * @param garbage a new garbage instance.
   * @return GarbageList* a pointer to a new garbage buffer if created.
   */
  static GarbageList*
  AddGarbage(  //
      GarbageList* garbage_list,
      const T* garbage)
  {
    const auto current_head = garbage_list->head_index_.load(kMORelax);
    const auto current_epoch = garbage_list->current_epoch_.load(kMORelax);

    garbage_list->garbages_[current_head] = {current_epoch, const_cast<T*>(garbage)};
    garbage_list->head_index_.fetch_add(1, kMORelax);

    if (current_head < kGarbageBufferSize - 1) {
      return nullptr;
    } else {
      const auto new_garbage_list = new GarbageList{current_epoch};
      garbage_list->next_.store(new_garbage_list, kMORelax);
      return new_garbage_list;
    }
  }

  /**
   * @brief Clear garbages where their epoch is less than a protected one.
   *
   * @param garbage_list a target barbage buffer.
   * @param protected_epoch a protected epoch.
   * @return GarbageList* a head of garbage buffers.
   */
  static GarbageList*
  Clear(  //
      GarbageList* garbage_list,
      const size_t protected_epoch)
  {
    if (garbage_list == nullptr) return nullptr;

    // release unprotected garbages
    const auto current_head = garbage_list->head_index_.load(kMORelax);
    auto index = garbage_list->tail_index_;
    for (; index < current_head; ++index) {
      const auto [epoch, garbage] = garbage_list->garbages_[index];
      if (epoch < protected_epoch) {
        Delete(garbage);
      } else {
        break;
      }
    }
    garbage_list->tail_index_ = index;

    if (index < kGarbageBufferSize) {
      return garbage_list;
    } else {
      // release an empty list
      auto next_list = garbage_list->next_.load(kMORelax);
      while (next_list == nullptr) {
        // if the garbage buffer is full but does not have a next buffer, wait insertion of it
        next_list = garbage_list->next_.load(kMORelax);
      }
      delete garbage_list;

      // release the next list recursively
      return GarbageList::Clear(next_list, protected_epoch);
    }
  }

 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  /// a buffer of garbage instances with added epochs.
  std::array<std::pair<size_t, T*>, kGarbageBufferSize> garbages_;

  /// the index to represent a head position.
  std::atomic_size_t head_index_;

  /// the index to represent a tail position.
  size_t tail_index_;

  /// a current epoch. Note: this is maintained individually to improve performance.
  std::atomic_size_t current_epoch_;

  /// a pointer to a next garbage buffer.
  std::atomic<GarbageList*> next_;
};

}  // namespace dbgroup::memory::component

#endif  // MEMORY_MANAGER_MEMORY_COMPONENT_GARBAGE_LIST_H_
