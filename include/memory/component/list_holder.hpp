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

#ifndef DBGROUP_MEMORY_COMPONENT_LIST_HOLDER_HPP
#define DBGROUP_MEMORY_COMPONENT_LIST_HOLDER_HPP

// C++ standard libraries
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>

// external libraries
#include "thread/id_manager.hpp"

// local sources
#include "memory/component/garbage_list.hpp"
#include "memory/utility.hpp"

namespace dbgroup::memory::component
{
/**
 * @brief An interface class of garbage lists.
 *
 * @tparam Target A class for representing target garbage.
 */
template <class Target>
class alignas(kCacheLineSize) ListHolder
{
 public:
  /*############################################################################
   * Type aliases
   *##########################################################################*/

  using IDManager = ::dbgroup::thread::IDManager;

  /*############################################################################
   * Public constructors and assignment operators
   *##########################################################################*/

  constexpr ListHolder() = default;

  ListHolder(const ListHolder &) = delete;
  ListHolder(ListHolder &&) = delete;

  auto operator=(const ListHolder &) -> ListHolder & = delete;
  auto operator=(ListHolder &&) -> ListHolder & = delete;

  /*############################################################################
   * Public destructors
   *##########################################################################*/

  /**
   * @brief Destroy the object.
   *
   * If the list contains unreleased garbage, it will be forcibly released.
   */
  ~ListHolder()
  {
    if (gc_head_.load(kRelaxed) != nullptr) {
      GarbageList::Clear<Target>(&gc_head_, std::numeric_limits<size_t>::max());
      delete gc_head_.load(kRelaxed);
    }
  }

  /*############################################################################
   * Public utility functions for client threads
   *##########################################################################*/

  /**
   * @brief Add a new garbage instance.
   *
   * @param epoch An epoch when garbage is added.
   * @param garbage A new garbage instance.
   */
  void
  AddGarbage(  //
      const size_t epoch,
      typename Target::T *garbage)
  {
    AssignCurrentThreadIfNeeded();
    GarbageList::AddGarbage(&cli_tail_, epoch, garbage);
  }

  /**
   * @brief Reuse a destructed page if exist.
   *
   * @retval A memory page if exist.
   * @retval nullptr otherwise.
   */
  auto
  GetPageIfPossible()  //
      -> void *
  {
    AssignCurrentThreadIfNeeded();
    return GarbageList::ReusePage(&cli_head_);
  }

  /*############################################################################
   * Public utility functions for GC threads
   *##########################################################################*/

  /**
   * @brief Release registered garbage if possible.
   *
   * @param protected_epoch An epoch to check whether garbage can be freed.
   */
  void
  ClearGarbage(  //
      const size_t protected_epoch)
  {
    std::unique_lock guard{mtx_, std::defer_lock};
    auto *head = gc_head_.load(kRelaxed);
    if (!guard.try_lock() || head == nullptr) return;

    // destruct or release garbages
    if constexpr (!Target::kReusePages) {
      GarbageList::Clear<Target>(&gc_head_, protected_epoch);
    } else {
      if (!heartbeat_.expired()) {
        GarbageList::Destruct<Target>(&gc_head_, protected_epoch);
        return;
      }
      GarbageList::Clear<Target>(&gc_head_, protected_epoch);
      head = gc_head_.load(kRelaxed);
      cli_head_.store(head, kRelaxed);
    }

    // check this list is alive
    if (!heartbeat_.expired() || !head->Empty()) return;

    // release buffers if the thread has exitted
    delete head;
    cli_tail_.store(nullptr, kRelaxed);
    cli_head_.store(nullptr, kRelaxed);
    gc_head_.store(nullptr, kRelaxed);
  }

 private:
  /*############################################################################
   * Internal utilities
   *##########################################################################*/

  /**
   * @brief Assign this list to the current thread.
   *
   */
  void
  AssignCurrentThreadIfNeeded()
  {
    if (!heartbeat_.expired()) return;

    const std::lock_guard guard{mtx_};
    if (cli_tail_.load(kRelaxed) == nullptr) {
      auto *list = new GarbageList{};
      cli_tail_.store(list, kRelaxed);
      if constexpr (Target::kReusePages) {
        cli_head_.store(list, kRelaxed);
      }
      gc_head_.store(list, kRelaxed);
    }
    heartbeat_ = IDManager::GetHeartBeat();
  }

  /*############################################################################
   * Internal member variables
   *##########################################################################*/

  /// @brief A flag for checking the corresponding thread has exited.
  std::weak_ptr<size_t> heartbeat_{};

  /// @brief A garbage list that has free space for new garbage.
  std::atomic<GarbageList *> cli_tail_{nullptr};

  /// @brief A garbage list that has destructed pages.
  std::atomic<GarbageList *> cli_head_{nullptr};

  /// @brief A dummy array for cache line alignments
  uint64_t padding_[4]{};

  /// @brief A mutex instance for modifying buffer pointers.
  std::mutex mtx_{};

  /// @brief A garbage list that has not destructed pages.
  std::atomic<GarbageList *> gc_head_{nullptr};
};

}  // namespace dbgroup::memory::component

#endif  // DBGROUP_MEMORY_COMPONENT_LIST_HOLDER_HPP
