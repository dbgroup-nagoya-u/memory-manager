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
    std::shared_ptr<GarbageList<T>> garbage_list;
    GarbageNode* next = nullptr;

    ~GarbageNode() { delete next; }
  };

  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  EpochManager epoch_manager_;

  const size_t garbage_list_size_;

  std::atomic<GarbageNode*> garbages_;

  const size_t gc_interval_micro_sec_;

  std::thread gc_thread_;

  std::atomic_bool gc_is_running_;

  std::unique_ptr<MemoryKeeper> memory_keeper_;

  /*################################################################################################
   * Internal utility functions
   *##############################################################################################*/

  void
  DeleteGarbages(const size_t protected_epoch)
  {
    // check the head of a garbage list
    auto previous = garbages_.load(mo_relax);
    previous->garbage_list->Clear(protected_epoch);
    auto current = previous->next;

    // check the other nodes of a garbage list
    while (current != nullptr) {
      current->garbage_list->Clear(protected_epoch);

      if (current->garbage_list.use_count() == 1 && current->garbage_list->Size() == 0) {
        // delete a garbage list
        previous->next = current->next;
        current->next = nullptr;
        delete current;
        current = previous->next;
      } else {
        // check a next garbage list
        previous = current;
        current = current->next;
      }
    }
  }

  void
  RunGC()
  {
    while (gc_is_running_) {
      const auto sleep_time = std::chrono::high_resolution_clock::now()
                              + std::chrono::microseconds(gc_interval_micro_sec_);

      // forward a global epoch and update registered epochs/garbage lists
      const auto current_epoch = epoch_manager_.ForwardGlobalEpoch();
      const auto protected_epoch = epoch_manager_.UpdateRegisteredEpochs();
      auto garbage_node = garbages_.load(mo_relax);
      while (garbage_node != nullptr) {
        if (garbage_node->garbage_list.use_count() > 1) {
          garbage_node->garbage_list->SetCurrentEpoch(current_epoch);
        }
        garbage_node = garbage_node->next;
      }

      // delete freeable garbages
      DeleteGarbages(protected_epoch);

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
      const size_t garbage_list_size,
      const size_t gc_interval_micro_sec,
      const bool reserve_memory = false,
      const size_t page_size = sizeof(T),
      const size_t page_num = 4096,
      const size_t partition_num = 8,
      const size_t page_alignment = 8)
      : epoch_manager_{},
        garbage_list_size_{garbage_list_size},
        garbages_{new GarbageNode{std::make_shared<GarbageList<T>>(), nullptr}},
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

    const auto current_epoch = epoch_manager_.GetCurrentEpoch();
    size_t protected_epoch;
    do {
      // wait for garbages to be out of scope
      std::this_thread::sleep_for(std::chrono::microseconds(gc_interval_micro_sec_));
      protected_epoch = epoch_manager_.UpdateRegisteredEpochs();
    } while (protected_epoch < current_epoch);

    delete garbages_;
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
    size_t size = 0;

    auto current = garbages_.load(mo_relax);
    while (current != nullptr) {
      size += current->garbage_list->Size();
      current = current->next;
    }

    return size;
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
    thread_local std::shared_ptr<GarbageList<T>> garbage_list = nullptr;

    if (garbage_list == nullptr) {
      garbage_list =
          std::make_shared<GarbageList<T>>(garbage_list_size_, epoch_manager_.GetCurrentEpoch(),
                                           gc_interval_micro_sec_, memory_keeper_.get());
      auto garbage_node = new GarbageNode{garbage_list, garbages_.load(mo_relax)};
      while (!garbages_.compare_exchange_weak(garbage_node->next, garbage_node, mo_relax)) {
        // continue until inserting succeeds
      }
    }

    garbage_list->AddGarbage(target_ptr);
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
