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

#ifndef MEMORY_MANAGER_DBGROUP_MEMORY_COMPONENT_LIST_HOLDER_HPP_
#define MEMORY_MANAGER_DBGROUP_MEMORY_COMPONENT_LIST_HOLDER_HPP_

// C++ standard libraries
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>

// external libraries
#include "dbgroup/lock/common.hpp"
#include "dbgroup/thread/id_manager.hpp"

// local sources
#include "dbgroup/memory/component/garbage_list.hpp"
#include "dbgroup/memory/component/reuse_list.hpp"
#include "dbgroup/memory/utility.hpp"

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

  ListHolder()
  {
    auto *glist = new GarbageList{};
    cl_glist_.store(glist, kRelease);
    gc_glist_.store(reinterpret_cast<uintptr_t>(glist), kRelease);

    if constexpr (Target::kReusePages) {
      auto *rlist = new ReuseList{};
      cl_rlist_.store(rlist, kRelease);
      gc_rlist_.store(reinterpret_cast<uintptr_t>(rlist), kRelease);
    }
  }

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
   */
  ~ListHolder()
  {
    auto *glist = reinterpret_cast<GarbageList *>(gc_glist_.load(kRelaxed));
    delete glist;

    if constexpr (Target::kReusePages) {
      auto *rlist = cl_rlist_.load(kAcquire);
      ReuseList::DestroyPages<Target>(rlist);
    }
  }

  /*############################################################################
   * Public APIs for clients
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
      void *garbage)
  {
    AssignCurrentThreadIfNeeded();
    GarbageList::AddGarbage(&cl_glist_, epoch, garbage);
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
    return ReuseList::GetPage(&cl_rlist_);
  }

  /*############################################################################
   * Public APIs for cleaners
   *##########################################################################*/

  /**
   * @brief Release registered garbage if possible.
   *
   * @param min_epoch An epoch to check whether garbage can be freed.
   * @param reuse_capacity The maximum number of reusable pages for each thread.
   * @param reuse_pages A temporary buffer for reusable pages.
   * @retval true if all the garbage pages are released.
   * @retval false otherwise.
   */
  auto
  ClearGarbage(  //
      const size_t min_epoch,
      const size_t reuse_capacity,
      std::vector<void *> &reuse_pages)  //
      -> bool
  {
    fence_.test_and_set(kAcquire);
    if constexpr (!Target::kReusePages) {
      return GarbageList::Clear<Target>(&gc_glist_, min_epoch);
    } else {
      if (heartbeat_.expired()) {
        return GarbageList::Clear<Target>(&gc_glist_, min_epoch);
      }

      const auto no_garbage = GarbageList::Clear<Target>(&gc_glist_, min_epoch, &reuse_pages);
      if (!reuse_pages.empty()) {
        ReuseList::AddPages(&gc_rlist_, reuse_pages, reuse_capacity);
      }
      return no_garbage;
    }
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
    heartbeat_ = IDManager::GetHeartBeat();
    fence_.clear(kRelease);
  }

  /*############################################################################
   * Internal member variables
   *##########################################################################*/

  /// @brief A flag for checking the corresponding thread has exited.
  std::weak_ptr<size_t> heartbeat_{};

  /// @brief A garbage list for a client thread.
  std::atomic<GarbageList *> cl_glist_{};

  /// @brief A reusable page list for a client thread.
  std::atomic<ReuseList *> cl_rlist_{};

  /// @brief A padding region for cache line alignment.
  uint64_t padding_[4] = {};

  /// @brief A dummy fence.
  std::atomic_flag fence_{};

  /// @brief A garbage list for cleaner threads.
  std::atomic_uintptr_t gc_glist_{};

  /// @brief A reusable page list for cleaner threads.
  std::atomic_uintptr_t gc_rlist_{};
};

}  // namespace dbgroup::memory::component

#endif  // MEMORY_MANAGER_DBGROUP_MEMORY_COMPONENT_LIST_HOLDER_HPP_
