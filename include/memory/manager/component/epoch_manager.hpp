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

#include <atomic>
#include <limits>
#include <memory>
#include <utility>

#include "epoch_guard.hpp"

namespace dbgroup::memory::manager::component
{
class EpochManager
{
 private:
  /*################################################################################################
   * Internal structs
   *##############################################################################################*/

  struct EpochList {
    Epoch *epoch = nullptr;
    std::shared_ptr<std::atomic_bool> reference = std::make_shared<std::atomic_bool>(false);
    EpochList *next = nullptr;

    ~EpochList() { reference->store(false); }
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

  ~EpochManager()
  {
    auto next = epochs_.load();
    while (next != nullptr) {
      auto current = next;
      next = current->next;
      delete current;
    }
  }

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
      const std::shared_ptr<std::atomic_bool> reference)
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
    if (previous->reference.use_count() > 1) {
      previous->epoch->SetCurrentEpoch(current_epoch);
      const auto protected_epoch = previous->epoch->GetProtectedEpoch();
      if (protected_epoch < min_protected_epoch) {
        min_protected_epoch = protected_epoch;
      }
    }
    auto current = previous->next;

    // update the other nodes of an epoch list
    while (current != nullptr) {
      if (current->reference.use_count() > 1) {
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

}  // namespace dbgroup::memory::manager::component
