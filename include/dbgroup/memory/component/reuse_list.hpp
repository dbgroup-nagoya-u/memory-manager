/*
 * Copyright 2024 Database Group, Nagoya University
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

#ifndef MEMORY_MANAGER_DBGROUP_MEMORY_COMPONENT_REUSE_LIST_HPP_
#define MEMORY_MANAGER_DBGROUP_MEMORY_COMPONENT_REUSE_LIST_HPP_

// C++ standard libraries
#include <atomic>
#include <cstddef>
#include <utility>
#include <vector>

// local sources
#include "dbgroup/memory/utility.hpp"

namespace dbgroup::memory::component
{
/**
 * @brief A class for representing a reusable page list.
 *
 */
class alignas(kVMPageSize) ReuseList
{
 public:
  /*############################################################################
   * Public constructors and assignment operators
   *##########################################################################*/

  /**
   * @brief Construct a reusable page list.
   *
   * @param prev The previous reusable page list.
   */
  constexpr explicit ReuseList(  //
      ReuseList *prev = nullptr)
      : prev_{prev}
  {
  }

  ReuseList(const ReuseList &) = delete;
  ReuseList(ReuseList &&) = delete;

  auto operator=(const ReuseList &) -> ReuseList & = delete;
  auto operator=(ReuseList &&) -> ReuseList & = delete;

  /*############################################################################
   * Public destructors
   *##########################################################################*/

  ~ReuseList() = default;

  /*############################################################################
   * Public APIs for clients
   *##########################################################################*/

  /**
   * @brief Add a new garbage instance to a specified list.
   *
   * If the buffer becomes full, create a new garbage buffer and link them.
   *
   * @param tail_addr The address of the tail list container.
   * @param pages Reusable pages.
   * @param reuse_capacity The maximum number of reusable pages for each thread.
   */
  static void AddPages(  //
      std::atomic_uintptr_t *tail_addr,
      std::vector<void *> &pages,
      size_t reuse_capacity);

  /**
   * @brief Reuse a destructed page if exist.
   *
   * @param head_addr The address of the head list container.
   * @retval A memory page if exist.
   * @retval nullptr otherwise.
   */
  static auto GetPage(                      //
      std::atomic<ReuseList *> *head_addr)  //
      -> void *;

  /*############################################################################
   * Public APIs for destruction
   *##########################################################################*/

  /**
   * @brief Destroy all the linked list and their contents.
   *
   * @tparam Target A class for representing target garbage.
   * @param list The head list of a linked list.
   * @note This function is not lock-free. Only a single thread must call this
   * function.
   */
  template <class Target>
  static void
  DestroyPages(  //
      ReuseList *list)
  {
    while (list != nullptr) {
      const auto tail = list->tail_.load(kAcquire);
      for (size_t i = list->head_.load(kRelaxed); i < tail; ++i) {
        Release<Target>(list->pages_[i].load(kRelaxed));
      }
      auto *next = list->next_.load(kAcquire);
      delete list;
      list = next;
    }
  }

 private:
  /*############################################################################
   * Internal constants
   *##########################################################################*/

  /// @brief The size of buffers for retaining garbages.
  static constexpr size_t kReuseListCapacity = (kVMPageSize - 32) / 8;

  /// @brief The begin bit position of an inline counter.
  static constexpr uint64_t kCntShift = 48;

  /// @brief A unit of an inline counter.
  static constexpr uintptr_t kCntUnit = 1UL << kCntShift;

  /// @brief A flag for indicating client threads have already used a pointer.
  static constexpr uintptr_t kPtrMask = kCntUnit - 1UL;

  /*############################################################################
   * Internal member variables
   *##########################################################################*/

  /// @brief The begin position of reusable pages.
  std::atomic_uint64_t head_{};

  /// @brief The previous reusable page list.
  ReuseList *prev_{};

  /// @brief A reusable page buffer.
  std::atomic<void *> pages_[kReuseListCapacity] = {};

  /// @brief The end position of registered pages.
  std::atomic_uint64_t tail_{};

  /// @brief The next reusable page list.
  std::atomic<ReuseList *> next_{};
};

}  // namespace dbgroup::memory::component

#endif  // MEMORY_MANAGER_DBGROUP_MEMORY_COMPONENT_REUSE_LIST_HPP_
