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
   * Public constants
   *##################################################################################*/

  static constexpr size_t kInitialEpoch = 1;

  static constexpr size_t kMinEpoch = 0;

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
  EpochManager()
  {
    // initialize protected epochs
    std::vector<size_t> protected_epochs = {kInitialEpoch, kMinEpoch};
    auto *head = new ProtectedEpochsNode{std::move(protected_epochs), nullptr};
    protected_epoch_lists_.store(head, std::memory_order_release);
  }

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
    // remove the registered epochs
    auto *epoch_next = epochs_.load(std::memory_order_acquire);
    while (epoch_next != nullptr) {
      auto *current = epoch_next;
      epoch_next = current->next;
      delete current;
    }

    // remove the retained protected epochs
    auto *pro_next = protected_epoch_lists_.load(std::memory_order_acquire);
    while (pro_next != nullptr) {
      auto *current = pro_next;
      pro_next = current->next;
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
      -> std::pair<EpochGuard, const std::vector<size_t> &>
  {
    const auto *head = protected_epoch_lists_.load(std::memory_order_acquire);
    const auto &protected_epochs = head->GetProtectedEpochs();
    return {CreateEpochGuard(protected_epochs.front()), protected_epochs};
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
  CreateEpochGuard(std::optional<size_t> entered = std::nullopt)  //
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

    return EpochGuard{epoch.get(), entered};
  }

  /**
   * @brief Increment a current epoch value.
   *
   * This function also updates protected epoch values.
   */
  void
  ForwardGlobalEpoch()
  {
    const auto cur_epoch = global_epoch_.load(std::memory_order_relaxed);

    // update protected epoch values
    auto &&protected_epochs = CollectProtectedEpochs(cur_epoch);
    const auto &it_end = protected_epochs.cend();
    auto &&it = protected_epochs.cbegin();
    auto protected_epoch = (++(++it) != it_end) ? *it : kMinEpoch;

    // remove out-dated lists
    auto *const old_head = protected_epoch_lists_.load(std::memory_order_acquire);
    auto *current = old_head;
    while (current->next != nullptr) {
      auto *next = current->next;
      auto epoch = next->GetEpoch();
      if (epoch == protected_epoch) {
        // this node is still referred, so skip
        current = next;
        protected_epoch = (++it != it_end) ? *it : kMinEpoch;
        continue;
      }

      // remove the out-dated list
      next = next->next;
      delete current->next;
      current->next = next;
    }

    const auto min_epoch = protected_epochs.back();
    auto *head = new ProtectedEpochsNode{std::move(protected_epochs), old_head};
    protected_epoch_lists_.store(head, std::memory_order_release);

    // store the max/min epoch values for efficiency
    global_epoch_.fetch_add(1, std::memory_order_relaxed);
    min_epoch_.store(min_epoch, std::memory_order_relaxed);
  }

 private:
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

  /**
   * @brief A class of nodes for composing a linked list of epochs in each thread.
   *
   */
  class ProtectedEpochsNode
  {
   public:
    /*##################################################################################
     * Public constructors and assignment operators
     *################################################################################*/

    /**
     * @brief Construct a new instance.
     *
     * @param protected_epochs a moved pointer of protected epochs.
     * @param next a pointer to a next node.
     */
    ProtectedEpochsNode(  //
        std::vector<size_t> &&protected_epochs,
        ProtectedEpochsNode *next)
        : next{next}, protected_epochs_{protected_epochs}
    {
    }

    ProtectedEpochsNode(const ProtectedEpochsNode &) = delete;
    auto operator=(const ProtectedEpochsNode &) -> ProtectedEpochsNode & = delete;
    ProtectedEpochsNode(ProtectedEpochsNode &&) = delete;
    auto operator=(ProtectedEpochsNode &&) -> ProtectedEpochsNode & = delete;

    /*##################################################################################
     * Public destructors
     *################################################################################*/

    /**
     * @brief Destroy the instance.
     *
     */
    ~ProtectedEpochsNode() = default;

    /*##################################################################################
     * Public utility functions
     *################################################################################*/

    /**
     * @return the epoch value of this node.
     */
    [[nodiscard]] auto
    GetEpoch() const  //
        -> size_t
    {
      return protected_epochs_.front();
    }

    /**
     * @return the protected epochs.
     */
    [[nodiscard]] auto
    GetProtectedEpochs() const  //
        -> const std::vector<size_t> &
    {
      return protected_epochs_;
    }

    /*##################################################################################
     * Public member variables
     *################################################################################*/

    /// a pointer to the next node.
    ProtectedEpochsNode *next{nullptr};  // NOLINT

   private:
    /*##################################################################################
     * Internal member variables
     *################################################################################*/

    /// protected epochs in this epoch interval.
    const std::vector<size_t> protected_epochs_{};
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
  CollectProtectedEpochs(const size_t cur_epoch)  //
      -> std::vector<size_t>
  {
    std::vector<size_t> protected_epochs{};
    protected_epochs.reserve(kExpectedThreadNum);
    protected_epochs.emplace_back(cur_epoch + 1);  // reserve the next epoch
    protected_epochs.emplace_back(cur_epoch);

    // check the head node of the epoch list
    auto *previous = epochs_.load(std::memory_order_acquire);
    if (previous == nullptr) return protected_epochs;
    if (previous->IsAlive()) {
      const auto protected_epoch = previous->GetProtectedEpoch();
      if (protected_epoch < std::numeric_limits<size_t>::max()) {
        protected_epochs.emplace_back(protected_epoch);
      }
    }

    // check the tail nodes of the epoch list
    auto *current = previous->next;
    while (current != nullptr) {
      if (current->IsAlive()) {
        // if the epoch is alive, get the protected epoch value
        const auto protected_epoch = current->GetProtectedEpoch();
        if (protected_epoch < std::numeric_limits<size_t>::max()) {
          protected_epochs.emplace_back(protected_epoch);
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

    // remove duplicate values
    std::sort(protected_epochs.begin(), protected_epochs.end(), std::greater<size_t>{});
    auto &&end_iter = std::unique(protected_epochs.begin(), protected_epochs.end());
    protected_epochs.erase(end_iter, protected_epochs.end());

    return protected_epochs;
  }

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// a global epoch counter.
  std::atomic_size_t global_epoch_{kInitialEpoch};

  /// the minimum protected ecpoch value.
  std::atomic_size_t min_epoch_{kInitialEpoch};

  /// the head pointer of a linked list of epochs.
  std::atomic<EpochNode *> epochs_{nullptr};

  /// the head pointer of a linked list of epochs.
  std::atomic<ProtectedEpochsNode *> protected_epoch_lists_{nullptr};
};

}  // namespace dbgroup::memory

#endif  // MEMORY_COMPONENT_EPOCH_MANAGER_HPP
