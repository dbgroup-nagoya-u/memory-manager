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

#ifndef DBGROUP_MEMORY_EPOCH_BASED_GC_HPP
#define DBGROUP_MEMORY_EPOCH_BASED_GC_HPP

// C++ standard libraries
#include <atomic>
#include <chrono>
#include <filesystem>
#include <functional>
#include <limits>
#include <memory>
#include <shared_mutex>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

// external libraries
#include "thread/epoch_guard.hpp"
#include "thread/epoch_manager.hpp"
#include "thread/id_manager.hpp"

// local sources
#include "memory/component/garbage_list.hpp"
#include "memory/utility.hpp"

namespace dbgroup::memory
{
/**
 * @brief A class to manage garbage collection.
 *
 * @tparam T a target class of garbage collection.
 */
template <class... GCTargets>
class EpochBasedGC
{
  /*############################################################################
   * Type aliases
   *##########################################################################*/

  using IDManager = ::dbgroup::thread::IDManager;
  using EpochGuard = ::dbgroup::thread::EpochGuard;
  using EpochManager = ::dbgroup::thread::EpochManager;
  using Clock_t = ::std::chrono::high_resolution_clock;

  template <class Target>
  using GarbageList = component::GarbageList<Target>;

 public:
  /*############################################################################
   * Public constructors and assignment operators
   *##########################################################################*/

  /**
   * @brief Construct a new instance.
   *
   * @param gc_interval_micro_sec the duration of interval for GC.
   * @param gc_thread_num the maximum number of threads to perform GC.
   */
  constexpr explicit EpochBasedGC(  //
      const size_t gc_interval_micro_sec = kDefaultGCTime,
      const size_t gc_thread_num = kDefaultGCThreadNum)
      : gc_interval_{gc_interval_micro_sec}, gc_thread_num_{gc_thread_num}
  {
    InitializeGarbageLists<DefaultTarget, GCTargets...>();
    cleaner_threads_.reserve(gc_thread_num_);
  }

  EpochBasedGC(const EpochBasedGC &) = delete;
  EpochBasedGC(EpochBasedGC &&) = delete;

  auto operator=(const EpochBasedGC &) -> EpochBasedGC & = delete;
  auto operator=(EpochBasedGC &&) -> EpochBasedGC & = delete;

  /*############################################################################
   * Public destructors
   *##########################################################################*/

  /**
   * @brief Destroy the instance.
   *
   * If protected garbage remains, this destructor waits for them to be free.
   */
  ~EpochBasedGC()
  {
    // stop garbage collection
    StopGC();
  }

  /*############################################################################
   * Public utility functions
   *##########################################################################*/

  /**
   * @brief Create a guard instance to protect garbage based on the scoped locking
   * pattern.
   *
   * @return EpochGuard a created epoch guard.
   */
  auto
  CreateEpochGuard()  //
      -> EpochGuard
  {
    return epoch_manager_.CreateEpochGuard();
  }

  /**
   * @brief Add a new garbage instance.
   *
   * @tparam Target a class for representing target garbage.
   * @param garbage_ptr a pointer to a target garbage.
   */
  template <class Target = DefaultTarget>
  void
  AddGarbage(const void *garbage_ptr)
  {
    auto *ptr = static_cast<typename Target::T *>(const_cast<void *>(garbage_ptr));
    GetGarbageList<Target>()->AddGarbage(epoch_manager_.GetCurrentEpoch(), ptr);
  }

  /**
   * @brief Reuse a released memory page if it exists.
   *
   * @tparam Target a class for representing target garbage.
   * @retval nullptr if there are no reusable pages.
   * @retval a memory page.
   */
  template <class Target = DefaultTarget>
  auto
  GetPageIfPossible()  //
      -> void *
  {
    static_assert(Target::kReusePages);
    return GetGarbageList<Target>()->GetPageIfPossible();
  }

  /*############################################################################
   * Public GC control functions
   *##########################################################################*/

  /**
   * @brief Start garbage collection.
   *
   * @retval true if garbage collection has started.
   * @retval false if garbage collection is already running.
   */
  auto
  StartGC()  //
      -> bool
  {
    if (gc_is_running_.load(std::memory_order_relaxed)) return false;

    gc_is_running_.store(true, std::memory_order_relaxed);
    gc_thread_ = std::thread{&EpochBasedGC::RunGC, this};
    return true;
  }

  /**
   * @brief Stop garbage collection.
   *
   * @retval true if garbage collection has stopped.
   * @retval false if garbage collection is not running.
   */
  auto
  StopGC()  //
      -> bool
  {
    if (!gc_is_running_.load(std::memory_order_relaxed)) return false;

    gc_is_running_.store(false, std::memory_order_relaxed);
    gc_thread_.join();

    DestroyGarbageLists<DefaultTarget, GCTargets...>();
    return true;
  }

 private:
  /*############################################################################
   * Internal constants
   *##########################################################################*/

  /// The expected maximum number of threads.
  static constexpr size_t kMaxThreadNum = ::dbgroup::thread::kMaxThreadNum;

  /*############################################################################
   * Internal utilities for initialization and finalization
   *##########################################################################*/

  /**
   * @brief A dummy function for creating type aliases.
   *
   */
  template <class Target, class... Tails>
  static auto
  ConvToTuple()
  {
    using ListsPtr = std::unique_ptr<GarbageList<Target>[]>;

    if constexpr (sizeof...(Tails) > 0) {
      return std::tuple_cat(std::tuple<ListsPtr>{}, ConvToTuple<Tails...>());
    } else {
      return std::tuple<ListsPtr>{};
    }
  }

  /**
   * @brief Create the space for garbage lists for all the target garbage.
   *
   * @tparam Target the current class in garbage targets.
   * @tparam Tails the remaining classes in garbage targets.
   */
  template <class Target, class... Tails>
  void
  InitializeGarbageLists()
  {
    using ListsPtr = std::unique_ptr<GarbageList<Target>[]>;

    auto &lists = std::get<ListsPtr>(garbage_lists_);
    lists.reset(new GarbageList<Target>[kMaxThreadNum]);

    if constexpr (sizeof...(Tails) > 0) {
      InitializeGarbageLists<Tails...>();
    }
  }

  /**
   * @brief Destroy all the garbage lists for destruction.
   *
   * @tparam Target the current class in garbage targets.
   * @tparam Tails the remaining classes in garbage targets.
   */
  template <class Target, class... Tails>
  void
  DestroyGarbageLists()
  {
    using ListsPtr = std::unique_ptr<GarbageList<Target>[]>;

    auto &lists = std::get<ListsPtr>(garbage_lists_);
    lists.reset(nullptr);

    if constexpr (sizeof...(Tails) > 0) {
      DestroyGarbageLists<Tails...>();
    }
  }

  /*############################################################################
   * Internal utility functions
   *##########################################################################*/

  /**
   * @tparam Target a class for representing target garbage.
   * @return the head of a linked list of garbage nodes and its mutex object.
   */
  template <class Target>
  [[nodiscard]] auto
  GetGarbageList()
  {
    using ListsPtr = std::unique_ptr<GarbageList<Target>[]>;

    return &(std::get<ListsPtr>(garbage_lists_)[IDManager::GetThreadID()]);
  }

  /**
   * @brief Clear registered garbage if possible.
   *
   * @tparam Target the current class in garbage targets.
   * @tparam Tails the remaining classes in garbage targets.
   * @param protected_epoch an epoch value to be protected.
   */
  template <class Target, class... Tails>
  void
  ClearGarbage(const size_t protected_epoch)
  {
    using ListsPtr = std::unique_ptr<GarbageList<Target>[]>;

    auto &lists = std::get<ListsPtr>(garbage_lists_);
    for (size_t i = 0; i < kMaxThreadNum; ++i) {
      lists[i].ClearGarbage(protected_epoch);
    }

    if constexpr (sizeof...(Tails) > 0) {
      ClearGarbage<Tails...>(protected_epoch);
    }
  }

  /**
   * @brief Run a procedure of garbage collection.
   *
   */
  void
  RunGC()
  {
    // create cleaner threads
    for (size_t i = 0; i < gc_thread_num_; ++i) {
      cleaner_threads_.emplace_back([&]() {
        for (auto wake_time = Clock_t::now() + gc_interval_;  //
             gc_is_running_.load(std::memory_order_relaxed);  //
             wake_time += gc_interval_)                       //
        {
          // release unprotected garbage
          ClearGarbage<DefaultTarget, GCTargets...>(epoch_manager_.GetMinEpoch());

          // wait until the next epoch
          std::this_thread::sleep_until(wake_time);
        }
      });
    }

    // manage the global epoch
    for (auto wake_time = Clock_t::now() + gc_interval_;  //
         gc_is_running_.load(std::memory_order_relaxed);  //
         wake_time += gc_interval_)                       //
    {
      // wait until the next epoch
      std::this_thread::sleep_until(wake_time);
      epoch_manager_.ForwardGlobalEpoch();
    }

    // wait all the cleaner threads return
    for (auto &&t : cleaner_threads_) {
      t.join();
    }
    cleaner_threads_.clear();
  }

  /*############################################################################
   * Internal member variables
   *##########################################################################*/

  /// The duration of garbage collection in micro seconds.
  const std::chrono::microseconds gc_interval_{};

  /// The maximum number of cleaner threads
  const size_t gc_thread_num_{1};

  /// An epoch manager.
  EpochManager epoch_manager_{};

  /// A mutex to protect liked garbage lists
  std::shared_mutex garbage_lists_lock_{};

  /// A thread to run garbage collection.
  std::thread gc_thread_{};

  /// Worker threads to release garbage
  std::vector<std::thread> cleaner_threads_{};

  /// A flag to check whether garbage collection is running.
  std::atomic_bool gc_is_running_{false};

  /// The heads of linked lists for each GC target.
  decltype(ConvToTuple<DefaultTarget, GCTargets...>()) garbage_lists_ =
      ConvToTuple<DefaultTarget, GCTargets...>();
};

}  // namespace dbgroup::memory

#endif  // DBGROUP_MEMORY_EPOCH_BASED_GC_HPP
