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

#pragma once

#include <array>
#include <atomic>
#include <memory>
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
  using Epoch = component::Epoch;
  using EpochGuard = component::EpochGuard;
  using EpochManager = component::EpochManager;
  using GarbageList_t = component::GarbageList<T>;

  /// abbreviation for simplicity.
  static constexpr std::memory_order mo_relax = std::memory_order_relaxed;

 private:
  /*################################################################################################
   * Internal structs
   *##############################################################################################*/

  /**
   * @brief A class of nodes for composing a linked list of gabage lists in each thread.
   *
   */
  struct GarbageNode {
    /*##############################################################################################
     * Public member variables
     *############################################################################################*/

    /// a pointer to a target garbage list.
    GarbageList_t* garbage_tail;

    /// a shared pointer for monitoring the lifetime of a target list.
    std::shared_ptr<size_t> reference;

    /// a pointer to a next node.
    GarbageNode* next;

    /*##############################################################################################
     * Public constructors/destructors
     *############################################################################################*/

    /**
     * @brief Construct a dummy instance.
     *
     */
    constexpr GarbageNode() : garbage_tail{nullptr}, reference{}, next{nullptr} {};

    /**
     * @brief Construct a new instance.
     *
     * @param garbage_tail a pointer to a target list.
     * @param reference an original pointer for monitoring the lifetime of a target list.
     * @param next a pointer to a next node.
     */
    GarbageNode(  //
        const GarbageList_t* garbage_tail,
        const std::shared_ptr<size_t>& reference,
        const GarbageNode* next)
        : garbage_tail{const_cast<GarbageList_t*>(garbage_tail)},
          reference{reference},
          next{const_cast<GarbageNode*>(next)} {};

    /**
     * @brief Destroy the instance.
     *
     */
    ~GarbageNode() { Delete(garbage_tail); }
  };

  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  /// an epoch manager.
  EpochManager epoch_manager_;

  /// the head of a linked list of garbage buffers.
  std::atomic<GarbageNode*> garbages_;

  /// the duration of garbage collection in micro seconds.
  const size_t gc_interval_micro_sec_;

  /// a thread to run garbage collection.
  std::thread gc_thread_;

  /// a flag to check whether garbage collection is running.
  std::atomic_bool gc_is_running_;

  /*################################################################################################
   * Internal utility functions
   *##############################################################################################*/

  /**
   * @brief Update current epoch values in registered garbage lists.
   *
   * @param current_epoch a current epoch value to update epochs in garbage lists.
   */
  void
  UpdateEpochsInGarbageLists(const size_t current_epoch)
  {
    auto current = garbages_.load(mo_relax);
    while (current != nullptr) {
      current->garbage_tail->SetCurrentEpoch(current_epoch);
      current = current->next;
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
    auto current = garbages_.load(mo_relax);
    GarbageNode* previous = nullptr;

    while (current != nullptr) {
      // delete freeable garbages
      current->garbage_tail = GarbageList_t::Clear(current->garbage_tail, protected_epoch);

      // unregister garbage lists of expired threads
      if (previous != nullptr  //
          && current->reference.use_count() <= 1 && current->garbage_tail->Size() == 0) {
        previous->next = current->next;
        Delete(current);
        current = previous->next;
      } else {
        previous = current;
        current = current->next;
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
    const auto interval = std::chrono::microseconds(gc_interval_micro_sec_);

    while (gc_is_running_.load(mo_relax)) {
      const auto sleep_time = std::chrono::high_resolution_clock::now() + interval;

      // forward a global epoch and update registered epochs/garbage lists
      const auto current_epoch = epoch_manager_.ForwardGlobalEpoch();
      UpdateEpochsInGarbageLists(current_epoch);
      const auto protected_epoch = epoch_manager_.UpdateRegisteredEpochs(current_epoch);
      DeleteGarbages(protected_epoch);

      // wait for garbages to be out of scope
      std::this_thread::sleep_until(sleep_time);
    }
  }

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  /**
   * @brief Construct a new instance.
   *
   * @param gc_interval_micro_sec the duration of interval for GC (default: 1e5us = 100ms).
   */
  constexpr explicit EpochBasedGC(const size_t gc_interval_micro_sec = 100000)
      : epoch_manager_{},
        garbages_{nullptr},
        gc_interval_micro_sec_{gc_interval_micro_sec},
        gc_is_running_{false}
  {
  }

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

    // wait until all guards are freed
    const auto current_epoch = epoch_manager_.GetCurrentEpoch();
    size_t protected_epoch;
    do {
      // wait for garbages to be out of scope
      std::this_thread::sleep_for(std::chrono::microseconds(gc_interval_micro_sec_));
      protected_epoch = epoch_manager_.UpdateRegisteredEpochs(current_epoch);
    } while (protected_epoch < current_epoch);

    // delete all garbages
    GarbageNode *current, *next;
    next = garbages_.load();
    while (next != nullptr) {
      current = next;
      current->garbage_tail = GarbageList_t::Clear(current->garbage_tail, protected_epoch);
      next = current->next;
      Delete(current);
    }
  }

  EpochBasedGC(const EpochBasedGC&) = delete;
  EpochBasedGC& operator=(const EpochBasedGC&) = delete;
  EpochBasedGC(EpochBasedGC&&) = delete;
  EpochBasedGC& operator=(EpochBasedGC&&) = delete;

  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  /**
   * @return size_t the total number of registered garbages.
   */
  size_t
  GetRegisteredGarbageSize() const
  {
    auto garbage_node = garbages_.load(mo_relax);
    size_t sum = 0;
    while (garbage_node != nullptr) {
      sum += garbage_node->garbage_tail->Size();
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
    thread_local auto epoch = std::make_shared<Epoch>();

    if (epoch.use_count() <= 1) {
      epoch->SetCurrentEpoch(epoch_manager_.GetCurrentEpoch());
      epoch_manager_.RegisterEpoch(epoch);
    }

    return EpochGuard{epoch.get()};
  }

  /**
   * @brief Add a new garbage instance.
   *
   * @param target_ptr a pointer to a target garbage.
   */
  void
  AddGarbage(const T* target_ptr)
  {
    thread_local auto garbage_keeper = std::make_shared<size_t>();
    thread_local GarbageList_t* garbage_head = nullptr;

    if (garbage_keeper.use_count() <= 1) {
      garbage_head = new GarbageList_t{epoch_manager_.GetCurrentEpoch()};

      // register this garbage list
      auto garbage_node = new GarbageNode{garbage_head, garbage_keeper, garbages_.load(mo_relax)};
      while (!garbages_.compare_exchange_weak(garbage_node->next, garbage_node, mo_relax)) {
        // continue until inserting succeeds
      }
    }

    const auto new_list = GarbageList_t::AddGarbage(garbage_head, target_ptr);
    if (new_list != nullptr) {
      garbage_head = new_list;
    }
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
    if (gc_is_running_.load(mo_relax)) {
      return false;
    } else {
      gc_is_running_.store(true, mo_relax);
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
    if (!gc_is_running_.load(mo_relax)) {
      return false;
    } else {
      gc_is_running_.store(false, mo_relax);
      gc_thread_.join();
      return true;
    }
  }
};

}  // namespace dbgroup::memory
