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

#ifndef MEMORY_COMPONENT_EPOCH_MANAGER_HPP
#define MEMORY_COMPONENT_EPOCH_MANAGER_HPP

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
  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  /**
   * @brief Construct a new instance.
   *
   */
  constexpr EpochManager() = default;

  EpochManager(const EpochManager &) = delete;
  auto operator=(const EpochManager &) -> EpochManager & = delete;
  EpochManager(EpochManager &&) = delete;
  auto operator=(EpochManager &&) -> EpochManager & = delete;

  /*####################################################################################
   * Public destructors
   *##################################################################################*/

  /**
   * @brief Destroy the instance.
   *
   */
  ~EpochManager()
  {
    auto *next = epochs_.load(std::memory_order_acquire);
    while (next != nullptr) {
      auto *current = next;
      next = current->next;
      delete current;
    }
  }

  /*####################################################################################
   * Public utility functions
   *##################################################################################*/

  /**
   * @brief Forward an original epoch counter.
   *
   * @return a forwarded epoch value.
   */
  void
  ForwardGlobalEpoch()
  {
    global_epoch_.fetch_add(1, std::memory_order_relaxed);
  }

  /**
   * @return a reference to the global epoch.
   */
  [[nodiscard]] auto
  GetGlobalEpochReference() const  //
      -> const std::atomic_size_t &
  {
    return global_epoch_;
  }

  /**
   * @return a current global epoch value.
   */
  [[nodiscard]] auto
  GetCurrentEpoch() const  //
      -> size_t
  {
    return global_epoch_.load(std::memory_order_relaxed);
  }

  /**
   * @return an epoch to be kept in each thread
   */
  auto
  GetEpoch()  //
      -> Epoch *
  {
    thread_local std::shared_ptr<Epoch> epoch = std::make_shared<Epoch>();

    if (epoch.use_count() <= 1) {
      epoch->SetGrobalEpoch(&global_epoch_);

      // insert a new epoch node into the epoch list
      auto *node = new EpochNode{epoch, epochs_.load(std::memory_order_relaxed)};
      while (!epochs_.compare_exchange_weak(node->next, node, std::memory_order_release)) {
        // continue until inserting succeeds
      }
    }

    return epoch.get();
  }

  /**
   * @brief Compute the minimum epoch value for epoch-based protection.
   *
   * This function also removes dead epochs from the internal list while computing.
   *
   * @return a protected epoch value.
   */
  auto
  GetProtectedEpoch()  //
      -> size_t
  {
    auto min_protected_epoch = global_epoch_.load(std::memory_order_relaxed);

    // check the head node of the epoch list
    auto *previous = epochs_.load(std::memory_order_acquire);
    if (previous == nullptr) return min_protected_epoch;

    if (previous->IsAlive()) {
      const auto protected_epoch = previous->GetProtectedEpoch();
      if (protected_epoch < min_protected_epoch) {
        min_protected_epoch = protected_epoch;
      }
    }

    // check the tail nodes of the epoch list
    auto *current = previous->next;
    while (current != nullptr) {
      if (current->IsAlive()) {
        // if the epoch is alive, check the protected value
        const auto protected_epoch = current->GetProtectedEpoch();
        if (protected_epoch < min_protected_epoch) {
          min_protected_epoch = protected_epoch;
        }
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
  /*####################################################################################
   * Internal structs and assignment operators
   *##################################################################################*/

  /**
   * @brief A class of nodes for composing a linked list of epochs in each thread.
   *
   */
  class EpochNode
  {
   public:
    /*##################################################################################
     * Public constructors/destructors
     *################################################################################*/

    /**
     * @brief Construct a new instance.
     *
     * @param epoch a pointer to a target epoch.
     * @param next a pointer to a next node.
     */
    EpochNode(  //
        std::shared_ptr<Epoch> epoch,
        EpochNode *next)
        : next{next}, epoch_{std::move(epoch)}
    {
    }

    EpochNode(const EpochNode &) = delete;
    auto operator=(const EpochNode &) -> EpochNode & = delete;
    EpochNode(EpochNode &&) = delete;
    auto operator=(EpochNode &&) -> EpochNode & = delete;

    /*##################################################################################
     * Public destructors
     *################################################################################*/

    /**
     * @brief Destroy the instance.
     *
     */
    ~EpochNode() = default;

    /*##################################################################################
     * Public utility functions
     *################################################################################*/

    /**
     * @retval true if the registered thread is still active.
     * @retval false if the registered thread has already left.
     */
    [[nodiscard]] auto
    IsAlive() const  //
        -> bool
    {
      return epoch_.use_count() > 1;
    }

    /**
     * @return the protected epoch value.
     */
    [[nodiscard]] auto
    GetProtectedEpoch() const  //
        -> size_t
    {
      return epoch_->GetProtectedEpoch();
    }

    /*##################################################################################
     * Public member variables
     *################################################################################*/

    /// a pointer to the next node.
    EpochNode *next{nullptr};  // NOLINT

   private:
    /*##################################################################################
     * Internal member variables
     *################################################################################*/

    /// a shared pointer for monitoring the lifetime of a target epoch.
    const std::shared_ptr<Epoch> epoch_{};
  };

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// an original epoch counter.
  std::atomic_size_t global_epoch_{0};

  /// the head pointer of a linked list of epochs.
  std::atomic<EpochNode *> epochs_{nullptr};
};

}  // namespace dbgroup::memory::component

#endif  // MEMORY_COMPONENT_EPOCH_MANAGER_HPP
