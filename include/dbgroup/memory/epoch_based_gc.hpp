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

#ifndef MEMORY_MANAGER_DBGROUP_MEMORY_EPOCH_BASED_GC_HPP_
#define MEMORY_MANAGER_DBGROUP_MEMORY_EPOCH_BASED_GC_HPP_

// C++ standard libraries
#include <atomic>
#include <chrono>
#include <cstddef>
#include <memory>
#include <random>
#include <thread>
#include <tuple>
#include <vector>

// external libraries
#include "dbgroup/constants.hpp"
#include "dbgroup/thread/epoch_guard.hpp"
#include "dbgroup/thread/epoch_manager.hpp"
#include "dbgroup/thread/id_manager.hpp"
#include "dbgroup/types.hpp"

// local sources
#include "dbgroup/memory/component/list_holder.hpp"
#include "dbgroup/memory/utility.hpp"

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
  /*##########################################################################*
   * Type aliases
   *##########################################################################*/

  using IDManager = ::dbgroup::thread::IDManager;
  using EpochGuard = ::dbgroup::thread::EpochGuard;
  using EpochManager = ::dbgroup::thread::EpochManager<Serial64_t>;
  using Clock_t = ::std::chrono::system_clock;

  template <class Target>
  using GarbageList = component::ListHolder<Target>;

 public:
  /*##########################################################################*
   * Public constructors and assignment operators
   *##########################################################################*/

  /**
   * @brief Construct a new instance.
   *
   * @param gc_interval_ms The interval of garbage collection in milli seconds.
   * @param gc_thread_num The maximum number of cleaner threads.
   * @param reuse_capacity The maximum number of reusable pages for each thread.
   */
  explicit EpochBasedGC(  //
      const size_t gc_interval_ms = kDefaultGCTime,
      const size_t gc_thread_num = kDefaultGCThreadNum,
      const size_t reuse_capacity = kDefaultReusePageCapacity)
      : gc_interval_{gc_interval_ms},
        gc_thread_num_{gc_thread_num},
        reuse_capacity_{reuse_capacity},
        epoch_manager_{gc_interval_ms}
  {
    StartGC();
  }

  EpochBasedGC(const EpochBasedGC &) = delete;
  EpochBasedGC(EpochBasedGC &&) = delete;

  auto operator=(const EpochBasedGC &) -> EpochBasedGC & = delete;
  auto operator=(EpochBasedGC &&) -> EpochBasedGC & = delete;

  /*##########################################################################*
   * Public destructors
   *##########################################################################*/

  /**
   * @brief Destroy the instance.
   *
   * If protected garbage remains, this destructor waits for them to be free.
   */
  ~EpochBasedGC() { StopGC(); }

  /*##########################################################################*
   * Public utility functions
   *##########################################################################*/

  /**
   * @tparam Target A class for representing target garbage.
   * @retval true if GC includes a given class as a target.
   * @retval false otherwise.
   */
  template <class Target>
  static constexpr auto
  HasTarget()  //
      -> bool
  {
    if constexpr (sizeof...(GCTargets) == 0) {
      return false;
    } else if constexpr (sizeof...(GCTargets) == 1) {
      return std::is_same_v<Target, GCTargets...>;
    } else {
      return HasTargetRecursive<Target, GCTargets...>();
    }
  }

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

  /*##########################################################################*
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
    if (running_.load(kRelaxed)) return false;

    InitializeGarbageLists<DefaultTarget, GCTargets...>();

    // create cleaner threads
    running_.store(true, kRelaxed);
    for (size_t i = 0; i < gc_thread_num_; ++i) {
      cleaner_threads_.emplace_back([this]() {
        auto wake_time = Clock_t::now();
        auto min_epoch = epoch_manager_.GetMinEpoch();
        while (true) {
          wake_time += gc_interval_;
          if (const auto new_min = epoch_manager_.GetMinEpoch(); new_min != min_epoch) {
            min_epoch = new_min;
            const auto no_garbage = ClearGarbage<DefaultTarget, GCTargets...>(min_epoch);
            if (!running_.load(kRelaxed) && no_garbage) break;
          }
          std::this_thread::sleep_until(wake_time);
        }
      });
    }
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
    if (!running_.load(kRelaxed)) return false;

    // wait all the cleaner threads return
    running_.store(false, kRelaxed);
    for (auto &&t : cleaner_threads_) {
      t.join();
    }
    cleaner_threads_.clear();

    DestroyGarbageLists<DefaultTarget, GCTargets...>();
    return true;
  }

 private:
  /*##########################################################################*
   * Internal utilities for initialization and finalization
   *##########################################################################*/

  /**
   * @tparam Target A class for representing target garbage.
   * @tparam T The current garbage class.
   * @tparam Tails The remaining garbage classes.
   * @retval true if the current class is same with a given one.
   * @retval false otherwise.
   */
  template <class Target, class T, class... Tails>
  static constexpr auto
  HasTargetRecursive()  //
      -> bool
  {
    if constexpr (std::is_same_v<Target, T>) {
      return true;
    } else if (sizeof...(Tails) >= 2) {
      return HasTargetRecursive<Target, Tails...>();
    } else {
      return std::is_same_v<Target, Tails...>;
    }
  }

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

  /*##########################################################################*
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
   * @param min_epoch An epoch to check whether garbage can be freed.
   */
  template <class Target, class... Tails>
  auto
  ClearGarbage(                    //
      const Serial64_t min_epoch)  //
      -> bool
  {
    using ListsPtr = std::unique_ptr<GarbageList<Target>[]>;
    thread_local std::vector<void *> reuse_pages{};
    thread_local std::uniform_int_distribution<size_t> dist{0, kMaxThreadNum};
    thread_local std::mt19937_64 rand{std::random_device{}()};

    auto &lists = std::get<ListsPtr>(garbage_lists_);
    auto no_garbage = true;
    for (size_t i = 0, pos = dist(rand); i < kMaxThreadNum; ++i) {
      if (++pos >= kMaxThreadNum) {
        pos = 0;
      }
      no_garbage &= lists[pos].ClearGarbage(min_epoch, reuse_capacity_, reuse_pages);
    }
    for (auto *page : reuse_pages) {
      Release<Target>(page);
    }
    reuse_pages.clear();

    if constexpr (sizeof...(Tails) > 0) {
      return no_garbage && ClearGarbage<Tails...>(min_epoch);
    }
    return no_garbage;
  }

  /*##########################################################################*
   * Internal member variables
   *##########################################################################*/

  /// @brief The interval of garbage collection in milli seconds.
  std::chrono::milliseconds gc_interval_{};

  /// @brief The maximum number of cleaner threads.
  size_t gc_thread_num_{};

  /// @brief The maximum number of reusable pages for each thread.
  size_t reuse_capacity_{};

  /// @brief An epoch manager.
  EpochManager epoch_manager_{};

  /// @brief Worker threads for releasing garbage.
  std::vector<std::thread> cleaner_threads_{};

  /// @brief A flag for checking if garbage collection is running.
  std::atomic_bool running_{};

  /// @brief A tuple containing the garbage lists of each GC target.
  decltype(ConvToTuple<DefaultTarget, GCTargets...>()) garbage_lists_ =
      ConvToTuple<DefaultTarget, GCTargets...>();
};

/**
 * @brief A class for constructing an object.
 *
 * @tparam GCTargets Classes for representing target garbage.
 */
template <class... GCTargets>
class Builder
{
  /*##########################################################################*
   * Type aliases
   *##########################################################################*/

  using GC_t = EpochBasedGC<GCTargets...>;

 public:
  /*##########################################################################*
   * Public constructors and assignment operators
   *##########################################################################*/

  constexpr Builder() = default;

  constexpr Builder(const Builder &) = default;
  constexpr Builder(Builder &&) noexcept = default;

  constexpr Builder &operator=(const Builder &) = default;
  constexpr Builder &operator=(Builder &&) noexcept = default;

  /*##########################################################################*
   * Public destructor
   *##########################################################################*/

  ~Builder() = default;

  /*##########################################################################*
   * Public utilities
   *##########################################################################*/

  /**
   * @brief Construct an object using the set parameters.
   *
   * @return The @c std::unique_ptr of an object.
   */
  [[nodiscard]] auto
  Build() const  //
      -> std::unique_ptr<GC_t>
  {
    return std::make_unique<GC_t>(gc_interval_, gc_thread_num_, reuse_capacity_);
  }

  /*##########################################################################*
   * Public setters
   *##########################################################################*/

  /**
   * @param gc_interval_ms The interval of garbage collection in milli seconds.
   * @return Oneself.
   */
  constexpr auto
  SetGCInterval(                    //
      const size_t gc_interval_ms)  //
      -> Builder &
  {
    gc_interval_ = gc_interval_ms;
    return *this;
  }

  /**
   * @param gc_thread_num The maximum number of cleaner threads.
   * @return Oneself.
   */
  constexpr auto
  SetGCThreadNum(                  //
      const size_t gc_thread_num)  //
      -> Builder &
  {
    gc_thread_num_ = gc_thread_num;
    return *this;
  }

  /**
   * @param reuse_capacity The maximum number of reusable pages for each thread.
   * @return Oneself.
   */
  constexpr auto
  SetReusablePageNum(               //
      const size_t reuse_capacity)  //
      -> Builder &
  {
    reuse_capacity_ = reuse_capacity;
    return *this;
  }

 private:
  /*##########################################################################*
   * Internal member variables
   *##########################################################################*/

  /// @brief The interval of garbage collection in milli seconds.
  size_t gc_interval_{kDefaultGCTime};

  /// @brief The maximum number of cleaner threads.
  size_t gc_thread_num_{kDefaultGCThreadNum};

  /// @brief The maximum number of reusable pages for each thread.
  size_t reuse_capacity_{kDefaultReusePageCapacity};
};

}  // namespace dbgroup::memory

#endif  // MEMORY_MANAGER_DBGROUP_MEMORY_EPOCH_BASED_GC_HPP_
