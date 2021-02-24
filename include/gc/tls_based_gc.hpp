// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <array>
#include <atomic>
#include <memory>
#include <thread>
#include <vector>

#include "tls_based/epoch_guard.hpp"
#include "tls_based/epoch_manager.hpp"
#include "tls_based/garbage_list.hpp"

namespace dbgroup::gc
{
using tls::Epoch;
using tls::EpochGuard;
using tls::EpochManager;
using tls::GarbageList;

template <class T>
class TLSBasedGC
{
 private:
  /*################################################################################################
   * Internal enum and constants
   *##############################################################################################*/

  static constexpr size_t kDefaultGCIntervalMicroSec = 1E6;

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

  std::atomic<GarbageNode*> garbages_;

  EpochManager epoch_manager_;

  const size_t gc_interval_micro_sec_;

  std::thread gc_thread_;

  std::atomic_bool gc_is_running_;

  /*################################################################################################
   * Internal utility functions
   *##############################################################################################*/

  void
  DeleteGarbages(const size_t protected_epoch)
  {
    // check the head of a garbage list
    auto previous = garbages_.load();
    if (previous->garbage_list.use_count() > 1) {
      previous->garbage_list->Clear(protected_epoch);
    }
    auto current = previous->next;

    // check the other nodes of a garbage list
    while (current != nullptr) {
      if (current->garbage_list.use_count() > 1) {
        // delete garbages and check the next one
        current->garbage_list->Clear(protected_epoch);
        previous = current;
        current = current->next;
      } else {
        // delete a garbage list
        previous->next = current->next;
        current->next = nullptr;
        delete current;
        current = previous->next;
      }
    }
  }

  void
  RunGC()
  {
    while (gc_is_running_) {
      // wait for garbages to be out of scope
      std::this_thread::sleep_for(std::chrono::microseconds(gc_interval_micro_sec_));

      // forward a global epoch and update registered epochs/garbage lists
      const auto current_epoch = epoch_manager_.ForwardGlobalEpoch();
      const auto protected_epoch = epoch_manager_.UpdateRegisteredEpochs();
      auto garbage_node = garbages_.load();
      while (garbage_node != nullptr) {
        if (garbage_node->garbage_list.use_count() > 1) {
          garbage_node->garbage_list->SetCurrentEpoch(current_epoch);
        }
        garbage_node = garbage_node->next;
      }

      // delete freeable garbages
      DeleteGarbages(protected_epoch);
    }
  }

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  explicit TLSBasedGC(  //
      const size_t gc_interval_micro_sec = kDefaultGCIntervalMicroSec,
      const bool start_gc = true)
      : garbages_{new GarbageNode{}},
        epoch_manager_{},
        gc_interval_micro_sec_{gc_interval_micro_sec},
        gc_is_running_{false}
  {
    if (start_gc) {
      StartGC();
    }
  }

  ~TLSBasedGC()
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

  TLSBasedGC(const TLSBasedGC&) = delete;
  TLSBasedGC& operator=(const TLSBasedGC&) = delete;
  TLSBasedGC(TLSBasedGC&&) = default;
  TLSBasedGC& operator=(TLSBasedGC&&) = default;

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  EpochGuard
  CreateEpochGuard()
  {
    thread_local std::shared_ptr<Epoch> epoch = nullptr;
    if (epoch == nullptr) {
      epoch = std::make_shared<Epoch>(epoch_manager_.GetCurrentEpoch());
      epoch_manager_.RegisterEpoch(epoch);
    }

    return EpochGuard{epoch.get()};
  }

  void
  AddGarbage(const T* target_ptr)
  {
    thread_local std::shared_ptr<GarbageList<T>> garbage_list = nullptr;
    if (garbage_list == nullptr) {
      garbage_list = std::make_shared<GarbageList<T>>(epoch_manager_.GetCurrentEpoch(),
                                                      gc_interval_micro_sec_);
      auto garbage_node = new GarbageNode{garbage_list, garbages_.load()};
      while (!garbages_.compare_exchange_weak(garbage_node->next, garbage_node)) {
        // continue until inserting succeeds
      }
    }

    garbage_list->AddGarbage(target_ptr);
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
      gc_thread_ = std::thread{&TLSBasedGC::RunGC, this};
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

}  // namespace dbgroup::gc
