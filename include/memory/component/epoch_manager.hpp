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

#ifndef MEMORY_MANAGER_MEMORY_COMPONENT_EPOCH_MANAGER_H_
#define MEMORY_MANAGER_MEMORY_COMPONENT_EPOCH_MANAGER_H_

#include <atomic>
#include <memory>
#include <utility>

#include "epoch_guard.hpp"

namespace dbgroup::memory::component
{
/**
 * @brief A class to manage epochs for epoch-based garbage collection.
 *
 */
class EpochManager
{
 public:
  /*################################################################################################
   * Public constructors and assignment operators
   *##############################################################################################*/

  /**
   * @brief Construct a new instance.
   *
   */
  constexpr EpochManager() : global_epoch_{0}, epochs_{nullptr} {}

  EpochManager(const EpochManager &) = delete;
  EpochManager &operator=(const EpochManager &) = delete;
  EpochManager(EpochManager &&) = delete;
  EpochManager &operator=(EpochManager &&) = delete;

  /*################################################################################################
   * Public destructors
   *##############################################################################################*/

  /**
   * @brief Destroy the instance.
   *
   */
  ~EpochManager()
  {
    auto next = epochs_.load(kMORelax);
    while (next != nullptr) {
      auto current = next;
      next = current->next;
      delete current;
    }
  }

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  /**
   * @brief Forward an original epoch counter.
   *
   * @return a forwarded epoch value.
   */
  size_t
  ForwardGlobalEpoch()
  {
    return global_epoch_.fetch_add(1, kMORelax) + 1;
  }

  /**
   * @return a reference to the global epoch.
   */
  const std::atomic_size_t &
  GetGlobalEpochReference() const
  {
    return global_epoch_;
  }

  /**
   * @return an epoch to be kept in each thread
   */
  Epoch *
  GetEpoch()
  {
    thread_local std::shared_ptr<Epoch> epoch = CreateEpoch();

    return epoch.get();
  }

  /**
   * @brief Compute the minimum epoch value for epoch-based protection.
   *
   * This function also removes dead epochs from the internal list while computing.
   *
   * @return a protected epoch value.
   */
  size_t
  GetProtectedEpoch()
  {
    auto min_protected_epoch = global_epoch_.load(kMORelax);

    // check the head node of the epoch list
    auto previous = epochs_.load(kMORelax);
    if (previous == nullptr) {
      return min_protected_epoch;
    } else if (previous->IsAlive()) {
      previous->UpdateProtectedEpoch(min_protected_epoch);
    }

    // check the tail nodes of the epoch list
    auto current = previous->next;
    while (current != nullptr) {
      if (current->IsAlive()) {
        // if the epoch is alive, check the protected value
        current->UpdateProtectedEpoch(min_protected_epoch);
        previous = current;
        current = current->next;
      } else {
        // if the epoch is dead, delete this node from the list
        previous->next = current->next;
        delete current;
        current = previous->next;
      }
    }

    return min_protected_epoch;
  }

 private:
  /*################################################################################################
   * Internal structs
   *##############################################################################################*/

  /**
   * @brief A class of nodes for composing a linked list of epochs in each thread.
   *
   */
  class EpochNode
  {
   public:
    /*##############################################################################################
     * Public constructors/destructors
     *############################################################################################*/

    /**
     * @brief Construct a new instance.
     *
     * @param epoch a pointer to a target epoch.
     * @param next a pointer to a next node.
     */
    EpochNode(  //
        const std::shared_ptr<Epoch> &epoch,
        EpochNode *next)
        : epoch_{epoch}, next{next}
    {
    }

    /**
     * @brief Destroy the instance.
     *
     */
    ~EpochNode() = default;

    /*##############################################################################################
     * Public utility functions
     *############################################################################################*/

    /**
     * @retval true if the registered thread is still active.
     * @retval false if the registered thread has already left.
     */
    bool
    IsAlive() const
    {
      return epoch_.use_count() > 1;
    }

    /**
     * @brief Update the minimum epoch value.
     *
     * @param min_protected_epoch the current minimum value to be updated.
     */
    void
    UpdateProtectedEpoch(size_t &min_protected_epoch) const
    {
      const auto protected_epoch = epoch_->GetProtectedEpoch();
      if (protected_epoch < min_protected_epoch) {
        min_protected_epoch = protected_epoch;
      }
    }

    /*##############################################################################################
     * Public member variables
     *############################################################################################*/

    /// a pointer to the next node.
    EpochNode *next;

   private:
    /*##############################################################################################
     * Internal member variables
     *############################################################################################*/

    /// a shared pointer for monitoring the lifetime of a target epoch.
    const std::shared_ptr<Epoch> epoch_;
  };

  /*################################################################################################
   * Internal utility functions
   *##############################################################################################*/

  /**
   * @brief Create a new epoch and register it with the internal epoch list.
   *
   * @return a new epoch.
   */
  std::shared_ptr<Epoch>
  CreateEpoch()
  {
    // create a new epoch and its node
    auto epoch = std::make_shared<Epoch>(global_epoch_);
    auto epoch_node = new EpochNode{epoch, epochs_.load(kMORelax)};

    // insert a new epoch node into the epoch list
    while (!epochs_.compare_exchange_weak(epoch_node->next, epoch_node, kMORelax)) {
      // continue until inserting succeeds
    }

    return epoch;
  }

  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  /// an original epoch counter.
  std::atomic_size_t global_epoch_;

  /// the head pointer of a linked list of epochs.
  std::atomic<EpochNode *> epochs_;
};

}  // namespace dbgroup::memory::component

#endif  // MEMORY_MANAGER_MEMORY_COMPONENT_EPOCH_MANAGER_H_
