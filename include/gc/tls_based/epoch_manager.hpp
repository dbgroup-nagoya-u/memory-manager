// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <limits>
#include <memory>
#include <utility>

#include "epoch_guard.hpp"

namespace dbgroup::gc::tls
{
class EpochManager
{
 private:
  /*################################################################################################
   * Internal structs
   *##############################################################################################*/

  struct EpochList {
    std::shared_ptr<Epoch> epoch;
    EpochList *next = nullptr;

    ~EpochList() { delete next; }
  };

  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  std::atomic_size_t current_epoch_;

  std::atomic<EpochList *> epochs_;

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  EpochManager() : current_epoch_{0}, epochs_{new EpochList{}} {}

  ~EpochManager() { delete epochs_; }

  EpochManager(const EpochManager &) = delete;
  EpochManager &operator=(const EpochManager &) = delete;
  EpochManager(EpochManager &&) = default;
  EpochManager &operator=(EpochManager &&) = default;

  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  size_t
  GetCurrentEpoch() const
  {
    return current_epoch_.load();
  }

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  void
  ForwardGlobalEpoch()
  {
    current_epoch_.fetch_add(1);
  }

  void
  RegisterEpoch(const std::shared_ptr<Epoch> epoch)
  {
    auto epoch_node = new EpochList{epoch, epochs_.load()};
    while (!epochs_.compare_exchange_weak(epoch_node->next, epoch_node)) {
      // continue until inserting succeeds
    }
  }

  size_t
  UpdateRegisteredEpochs()
  {
    const auto current_epoch = current_epoch_.load();
    auto min_protected_epoch = std::numeric_limits<size_t>::max();

    // update the head of an epoch list
    auto previous = epochs_.load();
    if (previous->epoch.use_count() > 1) {
      previous->epoch->SetCurrentEpoch(current_epoch);
      const size_t protected_epoch = previous->epoch->GetProtectedEpoch();
      if (protected_epoch < min_protected_epoch) {
        min_protected_epoch = protected_epoch;
      }
    }
    auto current = previous->next;

    // update the other nodes of an epoch list
    while (current != nullptr) {
      if (current->epoch.use_count() > 1) {
        // if an epoch remains, update epoch information
        current->epoch->SetCurrentEpoch(current_epoch);
        const size_t protected_epoch = current->epoch->GetProtectedEpoch();
        if (protected_epoch < min_protected_epoch) {
          min_protected_epoch = protected_epoch;
        }
        previous = current;
        current = current->next;
      } else {
        // if an epoch is deleted, delete this node from a list
        previous->next = current->next;
        current->next = nullptr;
        delete current;
        current = previous->next;
      }
    }

    return min_protected_epoch;
  }
};

}  // namespace dbgroup::gc::tls
