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

#include "../utility.hpp"
#include "epoch_guard.hpp"

namespace dbgroup::memory::manager::component
{
class EpochManager
{
 private:
  /*################################################################################################
   * Internal structs
   *##############################################################################################*/

  struct EpochNode {
    /*##############################################################################################
     * Public member variables
     *############################################################################################*/

    Epoch *epoch;

    const std::shared_ptr<std::atomic_bool> reference;

    EpochNode *next;

    /*##############################################################################################
     * Public constructors/destructors
     *############################################################################################*/

    constexpr EpochNode() : epoch{nullptr}, reference{}, next{nullptr} {}

    EpochNode(  //
        const Epoch *epoch,
        const std::shared_ptr<std::atomic_bool> reference,
        const EpochNode *next)
        : epoch{const_cast<Epoch *>(epoch)},
          reference{reference},
          next{const_cast<EpochNode *>(next)}
    {
    }

    ~EpochNode()
    {
      if (reference.use_count() > 0) {
        reference->store(false);
      }
    }
  };

  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  std::atomic_size_t current_epoch_;

  std::atomic<EpochNode *> epochs_;

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  EpochManager() : current_epoch_{0}, epochs_{New<EpochNode>()} {}

  ~EpochManager()
  {
    auto next = epochs_.load(mo_relax);
    while (next != nullptr) {
      auto current = next;
      next = current->next;
      Delete(std::move(current));
    }
  }

  EpochManager(const EpochManager &) = delete;
  EpochManager &operator=(const EpochManager &) = delete;
  EpochManager(EpochManager &&) = delete;
  EpochManager &operator=(EpochManager &&) = delete;

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
      const Epoch *epoch,
      const std::shared_ptr<std::atomic_bool> &reference)
  {
    // prepare a new epoch node
    auto epoch_node = New<EpochNode>(epoch, reference, epochs_.load(mo_relax));

    // insert a new epoch node into the epoch list
    while (!epochs_.compare_exchange_weak(epoch_node->next, epoch_node, mo_relax)) {
      // continue until inserting succeeds
    }
  }

  size_t
  UpdateRegisteredEpochs(const size_t current_epoch)
  {
    auto min_protected_epoch = std::numeric_limits<size_t>::max();

    // update the head of an epoch list
    auto previous = epochs_.load(mo_relax);
    if (previous->reference.use_count() > 1) {
      previous->epoch->SetCurrentEpoch(current_epoch);
      const auto protected_epoch = previous->epoch->GetProtectedEpoch();
      if (protected_epoch < min_protected_epoch) {
        min_protected_epoch = std::move(protected_epoch);
      }
    }
    auto current = previous->next;

    // update the tail nodes of an epoch list
    while (current != nullptr) {
      if (current->reference.use_count() > 1) {
        // if an epoch remains, update epoch information
        current->epoch->SetCurrentEpoch(current_epoch);
        const auto protected_epoch = current->epoch->GetProtectedEpoch();
        if (protected_epoch < min_protected_epoch) {
          min_protected_epoch = std::move(protected_epoch);
        }
        previous = current;
        current = current->next;
      } else {
        // if an epoch is deleted, delete this node from a list
        previous->next = current->next;
        Delete(current);
        current = previous->next;
      }
    }

    return min_protected_epoch;
  }
};

}  // namespace dbgroup::memory::manager::component
