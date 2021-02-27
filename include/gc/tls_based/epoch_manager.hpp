// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <limits>
#include <memory>
#include <utility>

#include "epoch_guard.hpp"

namespace dbgroup::memory::tls_based
{
class EpochManager
{
 private:
  /*################################################################################################
   * Internal structs
   *##############################################################################################*/

  struct EpochList {
    Epoch *epoch;
    std::weak_ptr<uint64_t> reference;
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
    return current_epoch_.load(mo_relax);
  }

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  size_t
  ForwardGlobalEpoch()
  {
    return current_epoch_.fetch_add(1, mo_relax) + 1;
  }

  void
  RegisterEpoch(  //
      Epoch *epoch,
      const std::shared_ptr<uint64_t> reference)
  {
    auto epoch_node = new EpochList{epoch, reference, epochs_.load(mo_relax)};
    while (!epochs_.compare_exchange_weak(epoch_node->next, epoch_node, mo_relax)) {
      // continue until inserting succeeds
    }
  }

  size_t
  UpdateRegisteredEpochs()
  {
    const auto current_epoch = current_epoch_.load(mo_relax);
    auto min_protected_epoch = std::numeric_limits<size_t>::max();

    // update the head of an epoch list
    auto previous = epochs_.load(mo_relax);
    if (!previous->reference.expired()) {
      previous->epoch->SetCurrentEpoch(current_epoch);
      const auto protected_epoch = previous->epoch->GetProtectedEpoch();
      if (protected_epoch < min_protected_epoch) {
        min_protected_epoch = protected_epoch;
      }
    }
    auto current = previous->next;

    // update the other nodes of an epoch list
    while (current != nullptr) {
      if (!current->reference.expired()) {
        // if an epoch remains, update epoch information
        current->epoch->SetCurrentEpoch(current_epoch);
        const auto protected_epoch = current->epoch->GetProtectedEpoch();
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

}  // namespace dbgroup::memory::tls_based
