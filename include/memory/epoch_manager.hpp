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

#include <algorithm>
#include <atomic>
#include <chrono>
#include <memory>
#include <shared_mutex>
#include <thread>
#include <utility>
#include <vector>

#include "component/epoch_guard.hpp"

namespace dbgroup::memory
{
/**
 * @brief A class to manage epochs for epoch-based garbage collection.
 *
 */
class EpochManager
{
 public:
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using Epoch = component::Epoch;
  using EpochGuard = component::EpochGuard;

  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  /**
   * @brief Construct a new instance.
   *
   */
  EpochManager() = default;

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
   * Public getters
   *##################################################################################*/

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
   * @return the minimum protected epoch value.
   */
  [[nodiscard]] auto
  GetMinEpoch() const  //
      -> size_t
  {
    return min_epoch_.load(std::memory_order_relaxed);
  }

  /**
   * @brief Get protected epoch values as shared_ptr.
   *
   * Protected epoch values are sorted by descending order and include the current epoch
   * value. Note that the returned vector cannot be modified because it is referred from
   * multiple threads concurrently.
   *
   * @return protected epoch values.
   */
  [[nodiscard]] auto
  GetProtectedEpochs()  //
      -> std::shared_ptr<const std::vector<size_t>>
  {
    [[maybe_unused]] const std::shared_lock guard{epochs_mtx_};
    return std::shared_ptr<const std::vector<size_t>>{protected_epochs_};
  }

  /*####################################################################################
   * Public utility functions
   *##################################################################################*/

  /**
   * @brief Create a guard instance based on the scoped locking pattern.
   *
   * @return EpochGuard a created epoch guard.
   */
  [[nodiscard]] auto
  CreateEpochGuard()  //
      -> EpochGuard
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

    return EpochGuard{epoch.get()};
  }

  /**
   * @brief Increment a current epoch value.
   *
   * This function also updates protected epoch values.
   */
  void
  ForwardGlobalEpoch()
  {
    const auto next_epoch = global_epoch_.load(std::memory_order_relaxed) + 1;

    // update protected epoch values
    {
      auto *protected_epochs = CollectProtectedEpochs(next_epoch);
      [[maybe_unused]] const std::unique_lock guard{epochs_mtx_};
      protected_epochs_.reset(protected_epochs);
    }

    // store the max/min epoch values for efficiency
    global_epoch_.store(next_epoch, std::memory_order_relaxed);
    min_epoch_.store(protected_epochs_->back(), std::memory_order_relaxed);
  }

 private:
  /*####################################################################################
   * Internal constants
   *##################################################################################*/

  /*####################################################################################
   * Internal structs
   *##################################################################################*/

  /**
   * @brief A class of nodes for composing a linked list of epochs in each thread.
   *
   */
  class EpochNode
  {
   public:
    /*##################################################################################
     * Public constructors and assignment operators
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
   * Internal utility functions
   *##################################################################################*/

  /**
   * @brief Collect epoch value for epoch-based protection.
   *
   * This function also removes dead epochs from the internal list while computing.
   *
   * @param next_epoch the next global epoch value.
   * @return protected epoch values.
   */
  auto
  CollectProtectedEpochs(const size_t next_epoch)  //
      -> std::vector<size_t> *
  {
    auto &&protected_epochs = new std::vector<size_t>{next_epoch};
    protected_epochs->reserve(kExpectedThreadNum);

    // check the head node of the epoch list
    auto *previous = epochs_.load(std::memory_order_acquire);
    if (previous == nullptr) return protected_epochs;
    if (previous->IsAlive()) {
      protected_epochs->emplace_back(previous->GetProtectedEpoch());
    }

    // check the tail nodes of the epoch list
    auto *current = previous->next;
    while (current != nullptr) {
      if (current->IsAlive()) {
        // if the epoch is alive, get the protected epoch value
        protected_epochs->emplace_back(current->GetProtectedEpoch());
        previous = current;
        current = current->next;
      } else {
        // if the epoch is dead, delete this node from the list
        previous->next = current->next;
        delete current;
        current = previous->next;
      }
    }

    // remove duplicate values
    std::sort(protected_epochs->begin(), protected_epochs->end(), std::greater<size_t>{});
    auto &&end_iter = std::unique(protected_epochs->begin(), protected_epochs->end());
    protected_epochs->erase(end_iter, protected_epochs->end());

    return protected_epochs;
  }

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// a global epoch counter.
  std::atomic_size_t global_epoch_{0};

  /// the minimum protected ecpoch value.
  std::atomic_size_t min_epoch_{0};

  /// the head pointer of a linked list of epochs.
  std::atomic<EpochNode *> epochs_{nullptr};

  /// protected epoch values.
  std::shared_ptr<const std::vector<size_t>> protected_epochs_ =
      std::make_shared<const std::vector<size_t>>(0);

  /// a mutex for updating protected epochs.
  std::shared_mutex epochs_mtx_{};
};

}  // namespace dbgroup::memory

#endif  // MEMORY_COMPONENT_EPOCH_MANAGER_HPP
