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

#ifndef MEMORY_EPOCH_BASED_GC_HPP
#define MEMORY_EPOCH_BASED_GC_HPP

#include <atomic>
#include <chrono>
#include <limits>
#include <memory>
#include <shared_mutex>
#include <thread>
#include <utility>
#include <vector>

#include "component/epoch_guard.hpp"
#include "component/epoch_manager.hpp"
#include "component/garbage_list.hpp"
#include "utility.hpp"

namespace dbgroup::memory
{
/**
 * @brief A class to manage garbage collection.
 *
 * @tparam T a target class of garbage collection.
 */
template <class T>
class EpochBasedGC
{
  /*################################################################################################
   * Type aliases
   *##############################################################################################*/

  using Epoch = component::Epoch;
  using EpochGuard = component::EpochGuard;
  using EpochManager = component::EpochManager;
  using GarbageList_t = component::GarbageList<T>;
  using Clock_t = ::std::chrono::high_resolution_clock;

 public:
  /*################################################################################################
   * Public constructors and assignment operators
   *##############################################################################################*/

  /**
   * @brief Construct a new instance.
   *
   * @param gc_interval_micro_sec the duration of interval for GC (default: 1e5us = 100ms).
   * @param gc_thread_num the maximum number of threads to perform GC.
   * @param start_gc a flag to start GC after construction
   */
  explicit constexpr EpochBasedGC(  //
      const size_t gc_interval_micro_sec,
      const size_t gc_thread_num = 1,
      const bool start_gc = false)
      : gc_interval_{gc_interval_micro_sec}, gc_thread_num_{gc_thread_num}
  {
    cleaner_threads_.reserve(gc_thread_num_);
    if (start_gc) StartGC();
  }

  EpochBasedGC(const EpochBasedGC &) = delete;
  EpochBasedGC &operator=(const EpochBasedGC &) = delete;
  EpochBasedGC(EpochBasedGC &&) = delete;
  EpochBasedGC &operator=(EpochBasedGC &&) = delete;

  /*################################################################################################
   * Public destructors
   *##############################################################################################*/

  /**
   * @brief Destroy the instance.
   *
   * If protected garbages remains, this destructor waits for them to be free.
   */
  ~EpochBasedGC()
  {
    // stop garbage collection
    StopGC();

    // delete all the registered nodes
    auto *cur_node = garbage_lists_.load(std::memory_order_acquire);
    while (cur_node != nullptr) {
      auto *prev_node = cur_node;
      cur_node = cur_node->next;
      delete prev_node;
    }
  }

  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  /**
   * @return the total number of registered garbages.
   */
  [[nodiscard]] auto
  GetRegisteredGarbageSize()  //
      -> size_t
  {
    const std::shared_lock guard{garbage_lists_lock_};

    auto garbage_node = garbage_lists_.load(std::memory_order_acquire);
    size_t sum = 0;
    while (garbage_node != nullptr) {
      sum += garbage_node->Size();
      garbage_node = garbage_node->next;
    }

    return sum;
  }

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  /**
   * @brief Create a guard instance to protect garbages based on the scoped locking pattern.
   *
   * @return EpochGuard a created epoch guard.
   */
  auto
  CreateEpochGuard()  //
      -> EpochGuard
  {
    return EpochGuard{epoch_manager_.GetEpoch()};
  }

  /**
   * @brief Add a new garbage instance.
   *
   * @param garbage_ptr a pointer to a target garbage.
   */
  void
  AddGarbage(const T *garbage_ptr)
  {
    if (!garbage_list_) {
      garbage_list_ = std::make_shared<GarbageList_t>(epoch_manager_.GetGlobalEpochReference());

      // register this garbage list
      auto garbage_node =
          new GarbageNode{garbage_lists_.load(std::memory_order_acquire), garbage_list_};
      while (!garbage_lists_.compare_exchange_weak(garbage_node->next, garbage_node,
                                                   std::memory_order_acq_rel)) {
        // continue until inserting succeeds
      }
    }

    garbage_list_->AddGarbage(const_cast<T *>(garbage_ptr));  // NOLINT
  }

  /**
   * @brief Reuse a released memory page if it exists.
   *
   * @retval nullptr if there are no reusable pages.
   * @retval a memory page.
   */
  auto
  GetPageIfPossible()  //
      -> void *
  {
    if (!garbage_list_) return nullptr;
    return garbage_list_->GetPageIfPossible();
  }

  /*################################################################################################
   * Public GC control functions
   *##############################################################################################*/

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
    if (gc_is_running_.load(std::memory_order_acquire)) {
      return false;
    }
    gc_is_running_.store(true, std::memory_order_release);
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
    if (!gc_is_running_.load(std::memory_order_acquire)) {
      return false;
    }
    gc_is_running_.store(false, std::memory_order_release);
    gc_thread_.join();
    return true;
  }

 private:
  /*################################################################################################
   * Internal classes
   *##############################################################################################*/

  /**
   * @brief A class of nodes for composing a linked list of gabage lists in each thread.
   *
   */
  class GarbageNode
  {
   public:
    /*##############################################################################################
     * Public constructors and assignment operators
     *############################################################################################*/

    /**
     * @brief Construct a new instance.
     *
     * @param garbage_list a pointer to a target list.
     * @param node_keeper an original pointer for monitoring the lifetime of a target list.
     * @param next a pointer to a next node.
     */
    GarbageNode(  //
        GarbageNode *next,
        std::shared_ptr<GarbageList_t> garbage_list)
        : next{next}, garbage_list_{std::move(garbage_list)}
    {
    }

    GarbageNode(const GarbageNode &) = delete;
    GarbageNode &operator=(const GarbageNode &) = delete;
    GarbageNode(GarbageNode &&) = delete;
    GarbageNode &operator=(GarbageNode &&) = delete;

    /*##############################################################################################
     * Public destructor
     *############################################################################################*/

    /**
     * @brief Destroy the instance.
     *
     */
    ~GarbageNode() = default;

    /*##############################################################################################
     * Public getters
     *############################################################################################*/

    /**
     * @retval true if any thread may access this garbage list.
     * @retval false otherwise.
     */
    [[nodiscard]] auto
    IsAlive() const  //
        -> bool
    {
      return garbage_list_.use_count() > 1 || !garbage_list_->Empty();
    }

    /**
     * @return the number of remaining garbages.
     */
    [[nodiscard]] auto
    Size() const  //
        -> size_t
    {
      return garbage_list_->Size();
    }

    /*##############################################################################################
     * Public utility functions
     *############################################################################################*/

    /**
     * @brief Release registered garbages if possible.
     *
     * @param protected_epoch an epoch value to check whether garbages can be freed.
     */
    void
    ClearGarbages(const size_t protected_epoch)
    {
      std::unique_lock guard{mtx_, std::defer_lock};
      if (guard.try_lock()) {
        if (garbage_list_.use_count() > 1) {
          garbage_list_->DestructGarbages(protected_epoch);
        } else {
          garbage_list_->ClearGarbages(protected_epoch);
        }
      }
    }

    /**
     * @brief Delete all the garbages.
     *
     */
    void
    DestroyGarbages()
    {
      std::unique_lock guard{mtx_, std::defer_lock};
      if (guard.try_lock()) {
        garbage_list_->ClearGarbages(std::numeric_limits<size_t>::max());
      }
    }

    /*##############################################################################################
     * Public member variables
     *############################################################################################*/

    /// a pointer to a next node.
    GarbageNode *next{nullptr};  // NOLINT

   private:
    /*##############################################################################################
     * Internal member variables
     *############################################################################################*/

    /// a pointer to a target garbage list.
    std::shared_ptr<GarbageList_t> garbage_list_{};

    /// a mutex for preventing multi-threads modify the same node concurrently
    std::mutex mtx_{};
  };

  /*################################################################################################
   * Internal utility functions
   *##############################################################################################*/

  template <class Target, class Head, class... Tails>
  [[nodiscard]] auto
  GetThreadLocalGarbageList() const  //
      -> std::shared_ptr<GarbageList<Target>> &
  {
    if constexpr (std::is_same_v<Head, Target> || sizeof...(Tails) == 0) {
      static_assert(std::is_same_v<Head, Target>);
      thread_local auto garbage_list =
          std::make_shared<GarbageList<Target>>(epoch_manager_.GetGlobalEpochReference());
      return garbage_list;
    } else {
      return GetThreadLocalGarbageList<Target, Tails...>();
    }
  }

  template <class Target, class Head, class... Tails>
  [[nodiscard]] auto
  GetGarbageNodeHead() const  //
      -> std::atomic<GarbageNode<Target> *> &
  {
    if constexpr (std::is_same_v<Head, Target> || sizeof...(Tails) == 0) {
      static_assert(std::is_same_v<Head, Target>);
      static std::atomic<GarbageNode<Target> *> node_head{nullptr};
      return node_head;
    } else {
      return GetGarbageNodeHead<Target, Tails...>();
    }
  }

  /**
   * @brief Remove expired nodes from the internal list.
   *
   */
  void
  RemoveExpiredNodes()
  {
    // if garbage-list nodes are variable, do nothing
    auto cur_node = garbage_lists_.load(std::memory_order_acquire);
    if (cur_node == nullptr) return;

    // create lock to prevent cleaner threads from running
    std::unique_lock guard{garbage_lists_lock_, std::defer_lock};
    if (guard.try_lock()) {
      // check whether there are expired nodes
      auto prev_node = cur_node;
      while (true) {
        cur_node = prev_node->next;
        if (cur_node == nullptr) break;

        if (cur_node->IsAlive()) {
          prev_node = cur_node;
        } else {
          prev_node->next = cur_node->next;
          delete cur_node;
        }
      }
    }
  }

  /**
   * @brief Release/destruct registered garbages if possible.
   *
   * @param protected_epoch a protected epoch value.
   */
  void
  ClearGarbages()
  {
    const auto protected_epoch = protected_epoch_.load(std::memory_order_acquire);
    const std::shared_lock guard{garbage_lists_lock_};

    auto cur_node = garbage_lists_.load(std::memory_order_acquire);
    while (cur_node != nullptr) {
      cur_node->ClearGarbages(protected_epoch);
      cur_node = cur_node->next;
    }
  }

  /**
   * @brief Delete all the garbages.
   *
   */
  void
  DestroyGarbages()
  {
    const std::shared_lock guard{garbage_lists_lock_};

    auto cur_node = garbage_lists_.load(std::memory_order_acquire);
    while (cur_node != nullptr) {
      cur_node->DestroyGarbages();
      cur_node = cur_node->next;
    }
  }

  /**
   * @brief Run garbage collection.
   *
   *  This function is assumed to be called in std::thread constructors.
   */
  void
  RunGC()
  {
    Clock_t::time_point sleep_time;

    {
      // create a lock to prevent cleaner threads from running
      const std::unique_lock guard{garbage_lists_lock_};

      // create cleaner threads
      for (size_t i = 0; i < gc_thread_num_; ++i) {
        cleaner_threads_.emplace_back([&] {
          while (gc_is_running_.load(std::memory_order_acquire)) {
            ClearGarbages();
            std::this_thread::sleep_until(
                LongToTimePoint(sleep_until_.load(std::memory_order_acquire)));
          }
          DestroyGarbages();
        });
      }

      // set sleep-interval
      sleep_time = Clock_t::now() + gc_interval_;
      sleep_until_.store(TimePointToLong(sleep_time), std::memory_order_release);
    }

    // manage epochs and sleep-interval
    while (gc_is_running_.load(std::memory_order_acquire)) {
      std::this_thread::sleep_until(sleep_time);
      sleep_time += gc_interval_;
      sleep_until_.store(TimePointToLong(sleep_time), std::memory_order_release);

      epoch_manager_.ForwardGlobalEpoch();
      protected_epoch_.store(epoch_manager_.GetProtectedEpoch(), std::memory_order_release);
      RemoveExpiredNodes();
    }

    // wait all the cleaner threads return
    for (auto &&t : cleaner_threads_) t.join();
    cleaner_threads_.clear();
  }

  /**
   * @param t a time point.
   * @return a converted unsigned integer value.
   */
  size_t
  TimePointToLong(const Clock_t::time_point t)
  {
    auto t_us = std::chrono::time_point_cast<std::chrono::microseconds>(t);
    return t_us.time_since_epoch().count();
  }

  /**
   * @param t an unsigned interger value.
   * @return a converted time point.
   */
  Clock_t::time_point
  LongToTimePoint(const size_t t)
  {
    std::chrono::microseconds t_us{t};
    return Clock_t::time_point{t_us};
  }

  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  /// the duration of garbage collection in micro seconds.
  const std::chrono::microseconds gc_interval_{static_cast<size_t>(1e5)};

  /// the maximum number of cleaner threads
  const size_t gc_thread_num_{1};

  /// an epoch manager.
  EpochManager epoch_manager_{};

  /// the head of a linked list of garbage buffers.
  std::atomic<GarbageNode *> garbage_lists_{nullptr};

  /// a mutex to protect liked garbage lists
  std::shared_mutex garbage_lists_lock_{};

  /// a thread to run garbage collection.
  std::thread gc_thread_{};

  /// worker threads to release garbages
  std::vector<std::thread> cleaner_threads_{};

  /// an epoch value to protect garbages
  std::atomic_size_t protected_epoch_{0};

  /// a converted time point for GC interval
  std::atomic_size_t sleep_until_{0};

  /// a flag to check whether garbage collection is running.
  std::atomic_bool gc_is_running_{false};

  /// a thread-local garbage list.
  inline static thread_local std::shared_ptr<GarbageList_t> garbage_list_{};  // NOLINT
};

}  // namespace dbgroup::memory

#endif  // MEMORY_EPOCH_BASED_GC_HPP
