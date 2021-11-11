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

#ifndef MEMORY_MANAGER_MEMORY_EPOCH_BASED_GC_H_
#define MEMORY_MANAGER_MEMORY_EPOCH_BASED_GC_H_

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
  constexpr EpochBasedGC(  //
      const size_t gc_interval_micro_sec = 1e5,
      const size_t gc_thread_num = 1,
      const bool start_gc = false)
      : gc_interval_{gc_interval_micro_sec},
        gc_thread_num_{gc_thread_num},
        epoch_manager_{},
        garbage_lists_{nullptr},
        garbage_lists_lock_{},
        gc_thread_{},
        cleaner_threads_{},
        protected_epoch_{0},
        sleep_until_{0},
        gc_is_running_{false}
  {
    cleaner_threads_.reserve(gc_thread_num_);
    if (start_gc) StartGC();
  }

  EpochBasedGC(const EpochBasedGC&) = delete;
  EpochBasedGC& operator=(const EpochBasedGC&) = delete;
  EpochBasedGC(EpochBasedGC&&) = delete;
  EpochBasedGC& operator=(EpochBasedGC&&) = delete;

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
    epoch_manager_.ForwardGlobalEpoch();

    // wait until all epoch guards are released
    const auto current_epoch = epoch_manager_.GetCurrentEpoch();
    while (epoch_manager_.GetProtectedEpoch() < current_epoch || !cleaner_threads_.empty()) {
      // wait for garbages to be out of scope
      std::this_thread::sleep_for(gc_interval_);
    }

    // delete all garbages
    GarbageNode *cur_node, *next_node;
    next_node = garbage_lists_.load();
    while (next_node != nullptr) {
      cur_node = next_node;
      next_node = cur_node->next;
      cur_node->ClearGarbages(std::numeric_limits<size_t>::max());
      delete cur_node;
    }
  }

  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  /**
   * @return the total number of registered garbages.
   */
  size_t
  GetRegisteredGarbageSize() const
  {
    auto garbage_node = garbage_lists_.load(kMORelax);
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
  EpochGuard
  CreateEpochGuard()
  {
    return EpochGuard{epoch_manager_.GetEpoch()};
  }

  /**
   * @brief Add a new garbage instance.
   *
   * @param garbage_ptr a pointer to a target garbage.
   */
  void
  AddGarbage(const T* garbage_ptr)
  {
    if (garbage_list_.use_count() <= 1) {
      garbage_list_->SetEpoch(epoch_manager_.GetGlobalEpochReference());

      // register this garbage list
      auto garbage_node = new GarbageNode{garbage_lists_.load(kMORelax), garbage_list_};
      while (!garbage_lists_.compare_exchange_weak(garbage_node->next, garbage_node, kMORelax)) {
        // continue until inserting succeeds
      }
    }

    garbage_list_->AddGarbage(garbage_ptr);
  }

  void*
  GetPageIfPossible()
  {
    if (garbage_list_.use_count() <= 1) return nullptr;
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
  bool
  StartGC()
  {
    if (gc_is_running_.load(kMORelax)) {
      return false;
    } else {
      gc_is_running_.store(true, kMORelax);
      gc_thread_ = std::thread{&EpochBasedGC::RunGC, this};
      return true;
    }
  }

  /**
   * @brief Stop garbage collection.
   *
   * @retval true if garbage collection has stopped.
   * @retval false if garbage collection is not running.
   */
  bool
  StopGC()
  {
    if (!gc_is_running_.load(kMORelax)) {
      return false;
    } else {
      gc_is_running_.store(false, kMORelax);
      gc_thread_.join();
      return true;
    }
  }

 private:
  /*################################################################################################
   * Internal constants
   *##############################################################################################*/

  /// abbreviation of std::memory_order_relaxed
  static constexpr auto kMORelax = component::kMORelax;

  /*################################################################################################
   * Internal structs
   *##############################################################################################*/

  class ThreadGarbage
  {
   public:
    /*##############################################################################################
     * Public constructors and assignment operators
     *############################################################################################*/

    constexpr ThreadGarbage() : head_{nullptr}, reuse_{nullptr}, tail_{nullptr} {}

    /*##############################################################################################
     * Public destructor
     *############################################################################################*/

    ~ThreadGarbage()
    {
      ClearGarbages(std::numeric_limits<size_t>::max());
      delete head_;
    }

    /*##############################################################################################
     * Public getters/setters
     *############################################################################################*/

    bool
    Empty() const
    {
      return head_->Empty();
    }

    size_t
    Size() const
    {
      return head_->Size();
    }

    /*##############################################################################################
     * Public utility functions
     *############################################################################################*/

    void
    SetEpoch(const std::atomic_size_t& global_epoch)
    {
      delete head_;

      head_ = new GarbageList_t{global_epoch};
      reuse_ = head_;
      tail_ = head_;
    }

    /**
     * @brief Add a new garbage instance.
     *
     * @param garbage_ptr a pointer to a target garbage.
     */
    void
    AddGarbage(const T* garbage_ptr)
    {
      tail_ = GarbageList_t::AddGarbage(tail_, garbage_ptr);
    }

    void*
    GetPageIfPossible()
    {
      void* page;
      std::tie(page, reuse_) = GarbageList_t::ReusePage(reuse_);

      return page;
    }

    void
    ClearGarbages(const size_t protected_epoch)
    {
      head_ = GarbageList_t::Clear(head_, protected_epoch);
    }

   private:
    /*##############################################################################################
     * Internal member variables
     *############################################################################################*/

    /// a pointer to a target garbage list.
    GarbageList_t* head_;

    /// a pointer to a target garbage list.
    GarbageList_t* reuse_;

    /// a pointer to a target garbage list.
    GarbageList_t* tail_;
  };

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
        GarbageNode* next,
        const std::shared_ptr<ThreadGarbage>& garbage_list)
        : next{next}, garbage_list_{garbage_list}, in_progress_{false}
    {
    }

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
     * @retval true if the corresponding thread is still running.
     * @retval false otherwise.
     */
    bool
    IsAlive() const
    {
      return garbage_list_.use_count() > 1 || !garbage_list_->Empty();
    }

    /**
     * @return the number of remaining garbages.
     */
    size_t
    Size() const
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
      auto in_progress = in_progress_.load(kMORelax);
      if (!in_progress && in_progress_.compare_exchange_strong(in_progress, true, kMORelax)) {
        garbage_list_->ClearGarbages(protected_epoch);
        in_progress_.store(false, kMORelax);
      }
    }

    /*##############################################################################################
     * Public member variables
     *############################################################################################*/

    /// a pointer to a next node.
    GarbageNode* next;

   private:
    /*##############################################################################################
     * Internal member variables
     *############################################################################################*/

    /// a pointer to a target garbage list.
    std::shared_ptr<ThreadGarbage> garbage_list_;

    /// a flag to indicate that a certain thread modifies this node.
    std::atomic_bool in_progress_;
  };

  /*################################################################################################
   * Internal utility functions
   *##############################################################################################*/

  /**
   * @brief Remove expired nodes from the internal list.
   *
   */
  void
  RemoveExpiredNodes()
  {
    // if garbage-list nodes are variable, do nothing
    auto cur_node = garbage_lists_.load(kMORelax);
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
   * @brief Delete registered garbages if possible.
   *
   * @param protected_epoch a protected epoch value.
   */
  void
  DeleteGarbages()
  {
    const auto protected_epoch = protected_epoch_.load(kMORelax);
    const std::shared_lock guard{garbage_lists_lock_};

    auto cur_node = garbage_lists_.load(kMORelax);
    while (cur_node != nullptr) {
      cur_node->ClearGarbages(protected_epoch);
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
          while (gc_is_running_.load(kMORelax)) {
            DeleteGarbages();
            std::this_thread::sleep_until(LongToTimePoint(sleep_until_.load(kMORelax)));
          }
        });
      }

      // set sleep-interval
      sleep_time = Clock_t::now() + gc_interval_;
      sleep_until_.store(TimePointToLong(sleep_time), kMORelax);
    }

    // manage epochs and sleep-interval
    while (gc_is_running_.load(kMORelax)) {
      std::this_thread::sleep_until(sleep_time);
      sleep_time += gc_interval_;
      sleep_until_.store(TimePointToLong(sleep_time), kMORelax);

      epoch_manager_.ForwardGlobalEpoch();
      protected_epoch_.store(epoch_manager_.GetProtectedEpoch(), kMORelax);
      RemoveExpiredNodes();
    }

    // wait all the cleaner threads return
    for (auto&& t : cleaner_threads_) t.join();
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
  const std::chrono::microseconds gc_interval_;

  /// the maximum number of cleaner threads
  const size_t gc_thread_num_;

  /// an epoch manager.
  EpochManager epoch_manager_;

  /// the head of a linked list of garbage buffers.
  std::atomic<GarbageNode*> garbage_lists_;

  /// a mutex to protect liked garbage lists
  std::shared_mutex garbage_lists_lock_;

  /// a thread to run garbage collection.
  std::thread gc_thread_;

  /// worker threads to release garbages
  std::vector<std::thread> cleaner_threads_;

  /// an epoch value to protect garbages
  std::atomic_size_t protected_epoch_;

  /// a converted time point for GC interval
  std::atomic_size_t sleep_until_;

  /// a flag to check whether garbage collection is running.
  std::atomic_bool gc_is_running_;

  inline static thread_local std::shared_ptr<ThreadGarbage> garbage_list_ =
      std::make_shared<ThreadGarbage>();
};

}  // namespace dbgroup::memory

#endif  // MEMORY_MANAGER_MEMORY_EPOCH_BASED_GC_H_
