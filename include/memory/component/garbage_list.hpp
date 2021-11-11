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
#include <utility>

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
   * @param global_epoch a reference to the global epoch.
   */
  constexpr explicit GarbageList(const std::atomic_size_t& global_epoch)
      : begin_idx_{0}, end_idx_{0}, released_idx_{0}, current_epoch_{global_epoch}, next_{nullptr}
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
    // if the list has garbages, release them before deleting oneself
    const auto end_idx = end_idx_.load(kMORelax);
    for (size_t idx = begin_idx_; idx < end_idx; ++idx) {
      delete garbages_[idx].second;
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
    const auto size = end_idx_.load(kMORelax) - released_idx_.load(kMORelax);
    const auto next = next_.load(kMORelax);
    if (next == nullptr) {
      return size;
    } else {
      return next->Size() + size;
    }
  }

  /**
   * @retval true if this list is empty.
   * @retval false otherwise
   */
  bool
  Empty() const
  {
    if (end_idx_.load(kMORelax) - begin_idx_ > 0) return false;

    const auto next = next_.load(kMORelax);
    if (next == nullptr) return true;
    return next->Empty();
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
   * @return GarbageList* a pointer to a current tail garbage buffer.
   */
  static GarbageList*
  AddGarbage(  //
      GarbageList* garbage_list,
      const T* garbage)
  {
    const auto end_idx = garbage_list->end_idx_.load(kMORelax);
    const auto current_epoch = garbage_list->current_epoch_.load(kMORelax);

    // insert a new garbage
    garbage_list->garbages_[end_idx] = {current_epoch, const_cast<T*>(garbage)};
    garbage_list->end_idx_.fetch_add(1, kMORelax);

    // check whether the list is full
    if (end_idx >= kGarbageBufferSize - 1) {
      auto full_list = garbage_list;
      garbage_list = new GarbageList{current_epoch};
      full_list->next_.store(garbage_list, kMORelax);
    }

    return garbage_list;
  }

  static std::pair<void*, GarbageList*>
  ReusePage(GarbageList* garbage_list)
  {
    const auto released_idx = garbage_list->released_idx_.load(kMORelax);
    auto cur_idx = garbage_list->begin_idx_;

    // check whether there are released garbages
    if (cur_idx == released_idx) return {nullptr, garbage_list};

    // get a released page
    void* page = garbage_list->garbages_[cur_idx].second;
    if (++cur_idx < kGarbageBufferSize) {
      // the list has reusable pages
      garbage_list->begin_idx_ = cur_idx;
    } else {
      // the list has become empty, so delete it
      auto empty_list = garbage_list;
      do {  // if the garbage buffer is empty but does not have a next buffer, wait insertion
        garbage_list = empty_list->next_.load(kMORelax);
      } while (garbage_list == nullptr);
      delete empty_list;
    }

    return {page, garbage_list};
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
    // release unprotected garbages
    const auto end_idx = garbage_list->end_idx_.load(kMORelax);
    auto idx = garbage_list->released_idx_.load(kMORelax);
    for (; idx < end_idx; ++idx) {
      auto [epoch, garbage] = garbage_list->garbages_[idx];
      if (epoch >= protected_epoch) break;

      // only call destructor to reuse pages
      garbage->~T();
    }
    garbage_list->released_idx_.store(idx, kMORelax);

    // check whether there is space in this list
    if (idx < kGarbageBufferSize) return garbage_list;

    // release the next list recursively
    return GarbageList::Clear(garbage_list->next_.load(kMORelax), protected_epoch);
  }

 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  /// a buffer of garbage instances with added epochs.
  std::array<std::pair<size_t, T*>, kGarbageBufferSize> garbages_;

  /// the index to represent a head position.
  size_t begin_idx_;

  /// the index to represent a tail position.
  std::atomic_size_t end_idx_;

  /// the end of released indexes
  std::atomic_size_t released_idx_;

  /// a current epoch. Note: this is maintained individually to improve performance.
  const std::atomic_size_t& current_epoch_;

  /// a pointer to a next garbage buffer.
  std::atomic<GarbageList*> next_;
};

}  // namespace dbgroup::memory::component

#endif  // MEMORY_MANAGER_MEMORY_COMPONENT_GARBAGE_LIST_H_
