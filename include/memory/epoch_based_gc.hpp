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
#include <future>
#include <list>
#include <memory>
#include <thread>
#include <utility>

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
      const size_t gc_thread_num = 2,
      const bool start_gc = false)
      : gc_interval_{gc_interval_micro_sec},
        gc_thread_num_{gc_thread_num},
        epoch_manager_{},
        garbage_lists_{nullptr},
        gc_thread_{},
        cleaner_threads_{},
        gc_is_running_{false}
  {
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
      cur_node->ReleaseGarbages(current_epoch);
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
    thread_local auto list_keeper = std::make_shared<std::atomic_bool>(false);
    thread_local GarbageList_t* garbage_list = nullptr;

    if (list_keeper.use_count() <= 1) {
      garbage_list = new GarbageList_t{epoch_manager_.GetGlobalEpochReference()};

      // register this garbage list
      auto garbage_node = new GarbageNode{garbage_lists_.load(kMORelax), garbage_list, list_keeper};
      while (!garbage_lists_.compare_exchange_weak(garbage_node->next, garbage_node, kMORelax)) {
        // continue until inserting succeeds
      }
    }

    garbage_list = GarbageList_t::AddGarbage(garbage_list, garbage_ptr);
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

  /**
   * @brief A class of nodes for composing a linked list of gabage lists in each thread.
   *
   */
  class GarbageNode
  {
   public:
    /*##############################################################################################
     * Public constructors/destructors
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
        GarbageList_t* garbage_list,
        const std::shared_ptr<std::atomic_bool>& node_keeper)
        : next{next}, garbage_list_{garbage_list}, node_keeper_{node_keeper}, in_progress_{false}
    {
    }

    /**
     * @brief Destroy the instance.
     *
     */
    ~GarbageNode() { delete garbage_list_; }

    /*##############################################################################################
     * Public getters
     *############################################################################################*/

    bool
    IsAlive() const
    {
      return node_keeper_.use_count() > 1 || !garbage_list_->Empty();
    }

    size_t
    Size() const
    {
      return garbage_list_->Size();
    }

    /*##############################################################################################
     * Public utility functions
     *############################################################################################*/

    void
    ReleaseGarbages(const size_t protected_epoch)
    {
      auto in_progress = in_progress_.load(kMORelax);
      if (!in_progress && in_progress_.compare_exchange_strong(in_progress, true, kMORelax)) {
        garbage_list_ = GarbageList_t::Clear(garbage_list_, protected_epoch);
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
    GarbageList_t* garbage_list_;

    /// a shared pointer for monitoring the lifetime of a target list.
    std::shared_ptr<std::atomic_bool> node_keeper_;

    ///
    std::atomic_bool in_progress_;
  };

  /*################################################################################################
   * Internal utility functions
   *##############################################################################################*/

  void
  RemoveExpiredNodes()
  {
    // if garbage-list nodes are variable, do nothing
    auto cur_node = garbage_lists_.load(kMORelax);
    if (!cleaner_threads_.empty() || cur_node == nullptr) return;

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

  /**
   * @brief Delete registered garbages if possible.
   *
   * @param protected_epoch a protected epoch value.
   */
  void
  DeleteGarbages(const size_t protected_epoch)
  {
    // if all the cleaner thread are already running, do nothing
    if (cleaner_threads_.size() >= gc_thread_num_) return;

    // add a new cleaner thread
    cleaner_threads_.emplace_back(std::async([&] {
      auto cur_node = garbage_lists_.load(kMORelax);
      while (cur_node != nullptr) {
        cur_node->ReleaseGarbages(protected_epoch);
        cur_node = cur_node->next;
      }
    }));
  }

  /**
   * @brief Wait cleaner threads until the next GC interval
   *
   * @param sleep_time
   */
  void
  WaitCleanerThreads(const Clock_t::time_point sleep_time)
  {
    auto iter = cleaner_threads_.begin();
    while (iter != cleaner_threads_.end()) {
      if (iter->wait_until(sleep_time) == std::future_status::ready) {
        iter = cleaner_threads_.erase(iter);
      } else {
        ++iter;
      }
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
    auto sleep_time = Clock_t::now();

    while (gc_is_running_.load(kMORelax)) {
      sleep_time += gc_interval_;

      // forward a global epoch
      epoch_manager_.ForwardGlobalEpoch();
      const auto protected_epoch = epoch_manager_.GetProtectedEpoch();

      // release garbages with multi-threads
      RemoveExpiredNodes();
      DeleteGarbages(protected_epoch);
      WaitCleanerThreads(sleep_time);
    }
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

  /// a thread to run garbage collection.
  std::thread gc_thread_;

  std::list<std::future<void>> cleaner_threads_;

  /// a flag to check whether garbage collection is running.
  std::atomic_bool gc_is_running_;
};

}  // namespace dbgroup::memory

#endif  // MEMORY_MANAGER_MEMORY_EPOCH_BASED_GC_H_
