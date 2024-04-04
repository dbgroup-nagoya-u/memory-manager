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
class alignas(kCashLineSize) ListHolder
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
    auto *head = Target::kReusePages ? head_.load(std::memory_order_relaxed) : mid_;
    if (head != nullptr) {
      GarbageList::Clear<Target>(&head, std::numeric_limits<size_t>::max());
      delete head;
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
    GarbageList::AddGarbage(&tail_, epoch, garbage);
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
    return GarbageList::ReusePage(&head_);
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
    if (!guard.try_lock() || mid_ == nullptr) return;

    // destruct or release garbages
    if constexpr (!Target::kReusePages) {
      GarbageList::Clear<Target>(&mid_, protected_epoch);
    } else {
      if (!heartbeat_.expired()) {
        GarbageList::Destruct<Target>(&mid_, protected_epoch);
        return;
      }

      auto *head = head_.load(std::memory_order_relaxed);
      if (head != nullptr) {
        mid_ = head;
      }
      GarbageList::Clear<Target>(&mid_, protected_epoch);
      head_.store(mid_, std::memory_order_relaxed);
    }

    // check this list is alive
    if (!heartbeat_.expired() || !mid_->Empty()) return;

    // release buffers if the thread has exitted
    delete mid_;
    head_.store(nullptr, std::memory_order_relaxed);
    mid_ = nullptr;
    tail_ = nullptr;
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
    if (tail_ == nullptr) {
      tail_ = new GarbageList{};
      mid_ = tail_;
      if constexpr (Target::kReusePages) {
        head_.store(tail_, std::memory_order_relaxed);
      }
    }
    heartbeat_ = IDManager::GetHeartBeat();
  }

  /*############################################################################
   * Internal member variables
   *##########################################################################*/

  /// @brief A flag for checking the corresponding thread has exited.
  std::weak_ptr<size_t> heartbeat_{};

  /// @brief A garbage list that has destructed pages.
  std::atomic<GarbageList *> head_{nullptr};

  /// @brief A garbage list that has free space for new garbage.
  GarbageList *tail_{nullptr};

  /// @brief A dummy array for cache line alignments
  uint64_t padding_[4]{};

  /// @brief A mutex instance for modifying buffer pointers.
  std::mutex mtx_{};

  /// @brief A garbage list that has not destructed pages.
  GarbageList *mid_{nullptr};
};

}  // namespace dbgroup::memory::component

#endif  // DBGROUP_MEMORY_COMPONENT_LIST_HOLDER_HPP
