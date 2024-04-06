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
#include <cstddef>
#include <thread>
#include <tuple>
#include <vector>

// external libraries
#include "thread/epoch_guard.hpp"
#include "thread/epoch_manager.hpp"
#include "thread/id_manager.hpp"

// local sources
#include "memory/component/list_holder.hpp"
#include "memory/utility.hpp"

namespace dbgroup::memory
{
/**
 * @brief A class for managing garbage collection.
 *
 * @tparam GCTargets Classes for representing target garbage.
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
  using GarbageList = component::ListHolder<Target>;

 public:
  /*############################################################################
   * Public constructors and assignment operators
   *##########################################################################*/

  /**
   * @brief Construct a new instance.
   *
   * @param gc_interval_micro_sec The duration of GC interval.
   * @param gc_thread_num The maximum number of threads for performing GC.
   */
  explicit EpochBasedGC(  //
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
  ~EpochBasedGC() { StopGC(); }

  /*############################################################################
   * Public utility functions
   *##########################################################################*/

  /**
   * @brief Create a guard instance to protect garbage based on the scoped
   * locking pattern.
   *
   * @return A guard instance.
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
   * @tparam Target A class for representing target garbage.
   * @param garbage_ptr A pointer to target garbage.
   */
  template <class Target = DefaultTarget>
  void
  AddGarbage(  //
      const void *garbage_ptr)
  {
    auto *ptr = static_cast<typename Target::T *>(const_cast<void *>(garbage_ptr));
    GetGarbageList<Target>()->AddGarbage(epoch_manager_.GetCurrentEpoch(), ptr);
  }

  /**
   * @brief Reuse a destructed page if exist.
   *
   * @tparam Target A class for representing target garbage.
   * @retval A memory page if exist.
   * @retval nullptr otherwise.
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
    if (gc_is_running_.load(kRelaxed)) return false;

    gc_is_running_.store(true, kRelaxed);
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
    if (!gc_is_running_.load(kRelaxed)) return false;

    gc_is_running_.store(false, kRelaxed);
    gc_thread_.join();

    DestroyGarbageLists<DefaultTarget, GCTargets...>();
    return true;
  }

 private:
  /*############################################################################
   * Internal constants
   *##########################################################################*/

  /// @brief The expected maximum number of threads.
  static constexpr size_t kMaxThreadNum = ::dbgroup::thread::kMaxThreadNum;

  /*############################################################################
   * Internal utilities for initialization and finalization
   *##########################################################################*/

  /**
   * @brief A dummy function for creating type aliases.
   *
   * @tparam Target The current class in garbage targets.
   * @tparam Tails The remaining classes in garbage targets.
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
   * @brief Allocate the space of garbage lists for each target recursively.
   *
   * @tparam Target The current class in garbage targets.
   * @tparam Tails The remaining classes in garbage targets.
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
   * @brief Destroy all the garbage lists.
   *
   * @tparam Target The current class in garbage targets.
   * @tparam Tails The remaining classes in garbage targets.
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
   * @tparam Target A class for representing target garbage.
   * @return A garbage list for a given target class.
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
   * @tparam Target The current class in garbage targets.
   * @tparam Tails The remaining classes in garbage targets.
   * @param protected_epoch An epoch to check whether garbage can be freed.
   */
  template <class Target, class... Tails>
  void
  ClearGarbage(  //
      const size_t protected_epoch)
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
   * @brief Create and run cleaner threads for garbage collection.
   *
   */
  void
  RunGC()
  {
    // create cleaner threads
    for (size_t i = 0; i < gc_thread_num_; ++i) {
      cleaner_threads_.emplace_back([&]() {
        for (auto wake_time = Clock_t::now() + gc_interval_;  //
             gc_is_running_.load(kRelaxed);                   //
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
         gc_is_running_.load(kRelaxed);                   //
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

  /// @brief The duration of garbage collection in micro seconds.
  const std::chrono::microseconds gc_interval_{};

  /// @brief The maximum number of cleaner threads
  const size_t gc_thread_num_{1};

  /// @brief An epoch manager.
  EpochManager epoch_manager_{};

  /// @brief A thread for managing garbage collection.
  std::thread gc_thread_{};

  /// @brief Worker threads for releasing garbage.
  std::vector<std::thread> cleaner_threads_{};

  /// @brief A flag for checking if garbage collection is running.
  std::atomic_bool gc_is_running_{false};

  /// @brief A tuple containing the garbage lists of each GC target.
  decltype(ConvToTuple<DefaultTarget, GCTargets...>()) garbage_lists_ =
      ConvToTuple<DefaultTarget, GCTargets...>();
};

}  // namespace dbgroup::memory

#endif  // DBGROUP_MEMORY_EPOCH_BASED_GC_HPP
