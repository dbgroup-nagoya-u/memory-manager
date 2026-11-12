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

#ifndef MEMORY_MANAGER_DBGROUP_MEMORY_COMPONENT_GARBAGE_LIST_HPP_
#define MEMORY_MANAGER_DBGROUP_MEMORY_COMPONENT_GARBAGE_LIST_HPP_

// C++ standard libraries
#include <atomic>
#include <bit>
#include <cstddef>
#include <vector>

// our libraries
#include <dbgroup/constants.hpp>
#include <dbgroup/types.hpp>

// local sources
#include "dbgroup/memory/utility.hpp"

namespace dbgroup::memory::component
{
/**
 * @brief A class for representing the buffer of garbage instances.
 *
 */
class alignas(kVMPageSize) GarbageList
{
 public:
  /*##########################################################################*
   * Public constructors and assignment operators
   *##########################################################################*/

  constexpr GarbageList() = default;

  GarbageList(const GarbageList&) = delete;
  GarbageList(GarbageList&&) = delete;

  auto operator=(const GarbageList&) -> GarbageList& = delete;
  auto operator=(GarbageList&&) -> GarbageList& = delete;

  /*##########################################################################*
   * Public destructors
   *##########################################################################*/

  ~GarbageList() = default;

  /*##########################################################################*
   * Public APIs for clients
   *##########################################################################*/

  /**
   * @brief Add a new garbage instance to a specified list.
   *
   * If the buffer becomes full, create a new garbage buffer and link them.
   *
   * @param tail_addr The address of the tail list container.
   * @param epoch An epoch when garbage is added.
   * @param garbage A new garbage instance.
   */
  static void AddGarbage(  //
      std::atomic<GarbageList*>* tail_addr,
      Serial64_t epoch,
      void* garbage);

  /*##########################################################################*
   * Public APIs for cleaners
   *##########################################################################*/

  /**
   * @brief Release garbage where their epoch is less than a protected one.
   *
   * @tparam Target A class for representing target garbage.
   * @param head_addr The address of the head list container.
   * @param min_epoch A protected (the minimum) epoch.
   * @param reuse_pages A temporary buffer for reusable pages.
   * @retval true if all the garbage pages are released.
   * @retval false otherwise.
   */
  template <class Target>
  static auto
  Clear(  //
      std::atomic_uintptr_t* const head_addr,
      const Serial64_t min_epoch,
      std::vector<void*>* reuse_pages = nullptr)  //
      -> bool
  {
    using T = typename Target::T;
    uint64_t tail;

    auto uptr = head_addr->load(kAcquire);
    if (!head_addr->compare_exchange_strong(uptr, uptr + kCntUnit, kAcquire, kRelaxed)) {
      return false;  // the thread cannot read this list, so work pessimistically
    }

    auto* list = std::bit_cast<GarbageList*>(uptr & kPtrMask);
    uptr = (uptr & kPtrMask) | kCntUnit;
    while (true) {
      tail = list->tail_.load(kAcquire);
      auto head = list->head_.load(kRelaxed);

      // claim releasable garbage in batches
      while (true) {
        auto new_head = head;
        while (new_head < tail && new_head - head < kMaxBatchSize
               && list->garbage_[new_head].epoch < min_epoch) {
          ++new_head;
        }
        if (new_head == head) break;
        if (!list->head_.compare_exchange_strong(head, new_head, kRelaxed, kRelaxed)) goto end;
        for (; head < new_head; ++head) {
          auto* const page = list->garbage_[head].ptr;
          if constexpr (!std::is_same_v<T, void>) {
            std::bit_cast<T*>(page)->~T();
          }
          if (reuse_pages) {
            reuse_pages->emplace_back(page);
          } else {
            Release<Target>(page);
          }
        }
      }
      auto* const next = list->next_.load(kAcquire);
      if (head < kGarbageListCapacity || !next) goto end;

      const auto next_ptr = std::bit_cast<uintptr_t>(next) | kCntUnit;
      if (head_addr->load(kRelaxed) != uptr
          || !head_addr->compare_exchange_strong(uptr, next_ptr, kAcqRel, kRelaxed)) {
        uptr = next_ptr;
        list = next;
        continue;
      }

      delete list;
      uptr = next_ptr;
      list = next;
    }

  end:
    const auto no_garbage =
        !list->next_.load(kAcquire) && (tail == 0 || list->garbage_[tail - 1].epoch < min_epoch);
    head_addr->fetch_sub(kCntUnit, kRelease);
    return no_garbage;
  }

  /*##########################################################################*
   * Public APIs for destruction
   *##########################################################################*/

  /**
   * @brief Destroy all the linked lists.
   *
   * Cleaner threads may leave consumed lists if other cleaners are reading
   * them, so this function destroys all the lists linked from a given one.
   *
   * @param list The head list of a linked list.
   * @note This function is not lock-free. Only a single thread must call this
   * function.
   */
  static void
  DestroyLists(  //
      GarbageList* list)
  {
    while (list) {
      auto* const next = list->next_.load(kAcquire);
      delete list;
      list = next;
    }
  }

 private:
  /*##########################################################################*
   * Internal constants
   *##########################################################################*/

  /// @brief The size of buffers for retaining garbages.
  static constexpr size_t kGarbageListCapacity = (kVMPageSize - 32) / 16;

  /// @brief The maximum number of garbage instances claimed at once.
  /// @note Claiming too many instances at once prevents cleaners from deleting
  /// consumed lists because each cleaner reads a list for a long time.
  static constexpr size_t kMaxBatchSize = 16;

  /// @brief The begin bit position of an inline counter.
  static constexpr uint64_t kCntShift = 48;

  /// @brief A unit of an inline counter.
  static constexpr uintptr_t kCntUnit = 1UL << kCntShift;

  /// @brief A flag for indicating client threads have already used a pointer.
  static constexpr uintptr_t kPtrMask = kCntUnit - 1UL;

  /*##########################################################################*
   * Internal classes
   *##########################################################################*/

  /**
   * @brief A class for retaining registered garbage instances.
   *
   */
  struct alignas(2 * kWordSize) Garbage {
    /// @brief An epoch when garbage is registered.
    Serial64_t epoch;

    /// @brief A registered garbage pointer.
    void* ptr;
  };

  /*##########################################################################*
   * Internal member variables
   *##########################################################################*/

  /// @brief The end position of registered garbage.
  std::atomic_uint64_t tail_{};

  /// @brief A buffer of garbage instances with added epochs.
  Garbage garbage_[kGarbageListCapacity] = {};

  /// @brief The begin position of not freed garbage.
  std::atomic_uint64_t head_{};

  /// @brief The next garbage list.
  std::atomic<GarbageList*> next_{};
};

}  // namespace dbgroup::memory::component

#endif  // MEMORY_MANAGER_DBGROUP_MEMORY_COMPONENT_GARBAGE_LIST_HPP_
