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

namespace dbgroup::memory::manager
{
using component::Epoch;
using component::EpochGuard;
using component::EpochManager;
using component::GarbageList;

template <class T>
class TLSBasedMemoryManager
{
 private:
  /*################################################################################################
   * Internal structs
   *##############################################################################################*/

  struct GarbageNode {
    GarbageList<T>* garbage_tail{nullptr};
    std::shared_ptr<std::atomic_bool> reference = std::make_shared<std::atomic_bool>(false);
    GarbageNode* next{nullptr};

    ~GarbageNode() { delete garbage_tail; }
  };

  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  EpochManager epoch_manager_;

  std::atomic<GarbageNode*> garbages_;

  const size_t gc_interval_micro_sec_;

  std::thread gc_thread_;

  std::atomic_bool gc_is_running_;

  /*################################################################################################
   * Internal utility functions
   *##############################################################################################*/

  constexpr void
  DeleteGarbages(  //
      const size_t current_epoch,
      const size_t protected_epoch)
  {
    GarbageNode *current, *next;
    auto head = garbages_.load(mo_relax);
    if (head == nullptr) {
      return;
    }

    current = head;
    while (current != nullptr) {
      current->garbage_tail->SetCurrentEpoch(current_epoch);
      current->garbage_tail = GarbageList<T>::Clear(current->garbage_tail, protected_epoch);
      current = current->next;
    }

    current = head;
    next = head->next;
    while (next != nullptr) {
      if (next->reference.use_count() == 1 && next->garbage_tail->Size() == 0) {
        current->next = next->next;
        delete next;
      } else {
        current = next;
      }
      next = current->next;
    }
  }

  constexpr void
  RunGC()
  {
    while (gc_is_running_) {
      // forward a global epoch and update registered epochs/garbage lists
      const auto current_epoch = epoch_manager_.ForwardGlobalEpoch();
      const auto protected_epoch = epoch_manager_.UpdateRegisteredEpochs();
      DeleteGarbages(current_epoch, protected_epoch);

      // wait for garbages to be out of scope
      std::this_thread::sleep_for(std::chrono::microseconds(gc_interval_micro_sec_));
    }
  }

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  constexpr explicit TLSBasedMemoryManager(const size_t gc_interval_micro_sec)
      : epoch_manager_{},
        garbages_{nullptr},
        gc_interval_micro_sec_{gc_interval_micro_sec},
        gc_is_running_{false}
  {
    StartGC();
  }

  ~TLSBasedMemoryManager()
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
      protected_epoch = epoch_manager_.UpdateRegisteredEpochs();
    } while (protected_epoch < current_epoch);

    // delete all garbages
    GarbageNode *current, *next;
    next = garbages_.load();
    while (next != nullptr) {
      current = next;
      current->garbage_tail = GarbageList<T>::Clear(current->garbage_tail, protected_epoch);
      if (current->reference.use_count() > 1) {
        current->reference->store(false);
      }
      next = current->next;
      delete current;
    }
  }

  TLSBasedMemoryManager(const TLSBasedMemoryManager&) = delete;
  TLSBasedMemoryManager& operator=(const TLSBasedMemoryManager&) = delete;
  TLSBasedMemoryManager(TLSBasedMemoryManager&&) = delete;
  TLSBasedMemoryManager& operator=(TLSBasedMemoryManager&&) = delete;

  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  constexpr size_t
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

  EpochGuard
  CreateEpochGuard()
  {
    thread_local auto epoch_keeper = std::make_shared<std::atomic_bool>(false);
    thread_local Epoch epoch{epoch_manager_.GetEpochReference()};

    if (!*epoch_keeper) {
      epoch_keeper->store(true);
      epoch_manager_.RegisterEpoch(&epoch, epoch_keeper);
    }

    return EpochGuard{&epoch};
  }

  void
  AddGarbage(const T* target_ptr)
  {
    thread_local auto garbage_keeper = std::make_shared<std::atomic_bool>(false);
    thread_local GarbageList<T>* garbage_head = nullptr;

    if (!*garbage_keeper) {
      garbage_keeper->store(true, mo_relax);
      garbage_head = new GarbageList<T>{epoch_manager_.GetCurrentEpoch()};
      auto garbage_node = new GarbageNode{garbage_head, garbage_keeper, garbages_.load(mo_relax)};
      while (!garbages_.compare_exchange_weak(garbage_node->next, garbage_node, mo_relax)) {
        // continue until inserting succeeds
      }
    }

    garbage_head = GarbageList<T>::AddGarbage(garbage_head, target_ptr);
  }

  /*################################################################################################
   * Public GC control functions
   *##############################################################################################*/

  constexpr bool
  StartGC()
  {
    if (gc_is_running_) {
      return false;
    } else {
      gc_is_running_ = true;
      gc_thread_ = std::thread{&TLSBasedMemoryManager::RunGC, this};
      return true;
    }
  }

  constexpr bool
  StopGC()
  {
    if (!gc_is_running_) {
      return false;
    } else {
      gc_is_running_ = false;
      gc_thread_.join();
      return true;
    }
  }
};

}  // namespace dbgroup::memory::manager
