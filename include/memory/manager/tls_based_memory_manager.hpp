// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

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
#include "component/memory_keeper.hpp"

namespace dbgroup::memory::manager
{
using component::Epoch;
using component::EpochGuard;
using component::EpochManager;
using component::GarbageList;
using component::MemoryKeeper;

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

  std::unique_ptr<MemoryKeeper> memory_keeper_;

  /*################################################################################################
   * Internal utility functions
   *##############################################################################################*/

  void
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

  void
  RunGC()
  {
    const auto interval = std::chrono::microseconds(gc_interval_micro_sec_);

    while (gc_is_running_) {
      const auto sleep_time = std::chrono::high_resolution_clock::now() + interval;

      // forward a global epoch and update registered epochs/garbage lists
      const auto current_epoch = epoch_manager_.ForwardGlobalEpoch();
      const auto protected_epoch = epoch_manager_.UpdateRegisteredEpochs();
      DeleteGarbages(current_epoch, protected_epoch);

      if (memory_keeper_ != nullptr) {
        // check a remaining memory capacity, and reserve memory if needed
        memory_keeper_->ReservePagesIfNeeded();
      }

      // wait for garbages to be out of scope
      std::this_thread::sleep_until(sleep_time);
    }
  }

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  TLSBasedMemoryManager(  //
      const size_t gc_interval_micro_sec,
      const bool reserve_memory = false,
      const size_t page_size = sizeof(T),
      const size_t page_num = 4096,
      const size_t partition_num = 8,
      const size_t page_alignment = 8)
      : epoch_manager_{},
        garbages_{nullptr},
        gc_interval_micro_sec_{gc_interval_micro_sec},
        gc_is_running_{false},
        memory_keeper_{nullptr}
  {
    if (reserve_memory) {
      memory_keeper_.reset(new MemoryKeeper{page_size, page_num, page_alignment, partition_num});
    }

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

  size_t
  GetAvailablePageSize() const
  {
    return memory_keeper_->GetCurrentCapacity();
  }

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  EpochGuard
  CreateEpochGuard()
  {
    thread_local auto epoch_keeper = std::make_shared<std::atomic_bool>(false);
    thread_local Epoch epoch{epoch_manager_.GetCurrentEpoch()};

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
      garbage_head = new GarbageList<T>{epoch_manager_.GetCurrentEpoch(), memory_keeper_.get()};
      auto garbage_node = new GarbageNode{garbage_head, garbage_keeper, garbages_.load(mo_relax)};
      while (!garbages_.compare_exchange_weak(garbage_node->next, garbage_node, mo_relax)) {
        // continue until inserting succeeds
      }
    }

    garbage_head = GarbageList<T>::AddGarbage(garbage_head, target_ptr);
  }

  void*
  GetPage()
  {
    assert(memory_keeper_ != nullptr);

    return memory_keeper_->GetPage();
  }

  /*################################################################################################
   * Public GC control functions
   *##############################################################################################*/

  bool
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

  bool
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
