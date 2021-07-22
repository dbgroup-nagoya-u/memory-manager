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

namespace dbgroup::memory::component
{
/**
 * @brief A class to manage epochs for epoch-based garbage collection.
 *
 */
class EpochManager
{
 private:
  /*################################################################################################
   * Internal structs
   *##############################################################################################*/

  /**
   * @brief A class of nodes for composing a linked list of epochs in each thread.
   *
   */
  struct EpochNode {
    /*##############################################################################################
     * Public member variables
     *############################################################################################*/

    /// a pointer to a target epoch.
    Epoch *epoch;

    /// a shared pointer for monitoring the lifetime of a target epoch.
    const std::shared_ptr<std::atomic_bool> reference;

    /// a pointer to a next node.
    EpochNode *next;

    /*##############################################################################################
     * Public constructors/destructors
     *############################################################################################*/

    /**
     * @brief Construct a dummy instance.
     *
     */
    constexpr EpochNode() : epoch{nullptr}, reference{}, next{nullptr} {}

    /**
     * @brief Construct a new instance.
     *
     * @param epoch a pointer to a target epoch.
     * @param reference an original pointer for monitoring the lifetime of a target epoch.
     * @param next a pointer to a next node.
     */
    EpochNode(  //
        const Epoch *epoch,
        const std::shared_ptr<std::atomic_bool> &reference,
        const EpochNode *next)
        : epoch{const_cast<Epoch *>(epoch)},
          reference{reference},
          next{const_cast<EpochNode *>(next)}
    {
    }

    /**
     * @brief Destroy the instance and set off a monitoring flag.
     *
     */
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

  /// an original epoch counter.
  std::atomic_size_t current_epoch_;

  /// the head pointer of a linked list of epochs.
  std::atomic<EpochNode *> epochs_;

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  /**
   * @brief Construct a new instance.
   *
   */
  constexpr EpochManager() : current_epoch_{0}, epochs_{nullptr} {}

  /**
   * @brief Destroy the instance.
   *
   */
  ~EpochManager()
  {
    auto next = epochs_.load(mo_relax);
    while (next != nullptr) {
      auto current = next;
      next = current->next;
      Delete(current);
    }
  }

  EpochManager(const EpochManager &) = delete;
  EpochManager &operator=(const EpochManager &) = delete;
  EpochManager(EpochManager &&) = delete;
  EpochManager &operator=(EpochManager &&) = delete;

  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  /**
   * @return size_t a current epoch counter.
   */
  size_t
  GetCurrentEpoch() const
  {
    return current_epoch_.load(mo_relax);
  }

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  /**
   * @brief Forward an original epoch counter.
   *
   * @return size_t a forwarded epoch value.
   */
  size_t
  ForwardGlobalEpoch()
  {
    return current_epoch_.fetch_add(1, mo_relax) + 1;
  }

  /**
   * @brief Register a new epoch with the manager.
   *
   * @param epoch an epoch to be registered.
   * @param reference a shared pointer for monitoring the lifetime of an epoch.
   */
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

  /**
   * @brief Update information of registered epochs.
   *
   * @param current_epoch a new epoch value to update registered epochs.
   * @return size_t  a protected epoch value.
   */
  size_t
  UpdateRegisteredEpochs(const size_t current_epoch)
  {
    // update the head of an epoch list
    auto previous = epochs_.load(mo_relax);
    if (previous == nullptr) return std::numeric_limits<size_t>::max();

    auto min_protected_epoch = std::numeric_limits<size_t>::max();
    if (previous->reference.use_count() > 1) {
      previous->epoch->SetCurrentEpoch(current_epoch);
      const auto protected_epoch = previous->epoch->GetProtectedEpoch();
      if (protected_epoch < min_protected_epoch) {
        min_protected_epoch = protected_epoch;
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
          min_protected_epoch = protected_epoch;
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

}  // namespace dbgroup::memory::component
