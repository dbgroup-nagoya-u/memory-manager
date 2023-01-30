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
   * Public constants
   *##################################################################################*/

  /// @brief The capacity of nodes for protected epochs.
  static constexpr size_t kCapacity = 256;

  /// @brief The initial value of epochs.
  static constexpr size_t kInitialEpoch = kCapacity;

  /// @brief The minimum value of epochs.
  static constexpr size_t kMinEpoch = 0;

  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  /**
   * @brief Construct a new instance.
   *
   */
  EpochManager()
  {
    auto &protected_epochs = ProtectedNode::GetProtectedEpochs(kInitialEpoch, protected_lists_);
    protected_epochs.emplace_back(kInitialEpoch);
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
    [[maybe_unused]] const auto dummy = global_epoch_.load(std::memory_order_acquire);
    auto *pro_next = protected_lists_;
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
    auto &&guard = CreateEpochGuard();
    const auto e = guard.GetProtectedEpoch();
    const auto &protected_epochs = ProtectedNode::GetProtectedEpochs(e, protected_lists_);

    return {std::move(guard), protected_epochs};
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
    const auto cur_epoch = global_epoch_.load(std::memory_order_relaxed);
    const auto next_epoch = cur_epoch + 1;

    // create a new node if needed
    if ((next_epoch & kLowerMask) == 0UL) {
      protected_lists_ = new ProtectedNode{next_epoch, protected_lists_};
    }

    // update protected epoch values
    auto &protected_epochs = ProtectedNode::GetProtectedEpochs(next_epoch, protected_lists_);
    CollectProtectedEpochs(cur_epoch, protected_epochs);
    RemoveOutDatedLists(protected_epochs);

    // store the max/min epoch values for efficiency
    global_epoch_.store(next_epoch, std::memory_order_release);
    min_epoch_.store(protected_epochs.back(), std::memory_order_relaxed);
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
    EpochNode *next{nullptr};

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
  class alignas(kCashLineSize) ProtectedNode
  {
   public:
    /*##################################################################################
     * Public constructors and assignment operators
     *################################################################################*/

    /**
     * @brief Construct a new instance.
     *
     * @param epoch upper bits of epoch values to be retained in this node.
     * @param next a pointer to a next node.
     */
    ProtectedNode(  //
        const size_t epoch,
        ProtectedNode *next)
        : next{next}, upper_epoch_(epoch)
    {
    }

    ProtectedNode(const ProtectedNode &) = delete;
    auto operator=(const ProtectedNode &) -> ProtectedNode & = delete;
    ProtectedNode(ProtectedNode &&) = delete;
    auto operator=(ProtectedNode &&) -> ProtectedNode & = delete;

    /*##################################################################################
     * Public destructors
     *################################################################################*/

    /**
     * @brief Destroy the instance.
     *
     */
    ~ProtectedNode() = default;

    /*##################################################################################
     * Public utility functions
     *################################################################################*/

    /**
     * @return the protected epochs.
     */
    [[nodiscard]] static auto
    GetProtectedEpochs(  //
        const size_t epoch,
        ProtectedNode *node)  //
        -> std::vector<size_t> &
    {
      // go to the target node
      const auto upper_epoch = epoch & kUpperMask;
      while (node->upper_epoch_ > upper_epoch) {
        node = node->next;
      }

      return node->epoch_lists_.at(epoch & kLowerMask);
    }

    [[nodiscard]] constexpr auto
    GetUpperBits() const  //
        -> size_t
    {
      return upper_epoch_;
    }

    /*##################################################################################
     * Public member variables
     *################################################################################*/

    /// @brief A pointer to the next node.
    ProtectedNode *next{nullptr};  // NOLINT

   private:
    /*##################################################################################
     * Internal member variables
     *################################################################################*/

    /// @brief The upper bits of epoch values to be retained in this node.
    size_t upper_epoch_{};

    /// @brief The list of protected epochs.
    std::array<std::vector<size_t>, kCapacity> epoch_lists_{};
  };

  /*####################################################################################
   * Internal constants
   *##################################################################################*/

  /// @brief A bitmask for extracting lower bits from epochs.
  static constexpr size_t kLowerMask = kCapacity - 1UL;

  /// @brief A bitmask for extracting upper bits from epochs.
  static constexpr size_t kUpperMask = ~kLowerMask;

  /*####################################################################################
   * Internal utility functions
   *##################################################################################*/

  /**
   * @brief Collect epoch value for epoch-based protection.
   *
   * This function also removes dead epochs from the internal list while computing.
   *
   * @param next_epoch the next global epoch value.
   * @param protected_epochs protected epoch values.
   */
  void
  CollectProtectedEpochs(  //
      const size_t cur_epoch,
      std::vector<size_t> &protected_epochs)
  {
    protected_epochs.reserve(kExpectedThreadNum);
    protected_epochs.emplace_back(cur_epoch + 1);  // reserve the next epoch
    protected_epochs.emplace_back(cur_epoch);

    // check the head node of the epoch list
    auto *previous = epochs_.load(std::memory_order_acquire);
    if (previous == nullptr) return;
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
  }

  /**
   * @brief Remove unprotected epoch nodes from a linked-list.
   *
   * @param protected_epochs protected epoch values.
   */
  void
  RemoveOutDatedLists(std::vector<size_t> &protected_epochs)
  {
    const auto &it_end = protected_epochs.cend();
    auto &&it = protected_epochs.cbegin();
    auto protected_epoch = *it & kUpperMask;

    // remove out-dated lists
    auto *prev = protected_lists_;
    auto *current = protected_lists_;
    while (current->next != nullptr) {
      const auto upper_bits = current->GetUpperBits();
      if (protected_epoch == upper_bits) {
        // this node is still referred, so skip
        prev = current;
        current = current->next;

        // search the next protected epoch
        do {
          if (++it == it_end) {
            protected_epoch = kMinEpoch;
            break;
          }
          protected_epoch = *it & kUpperMask;
        } while (protected_epoch == upper_bits);
        continue;
      }

      // remove the out-dated list
      prev->next = current->next;
      delete current;
      current = prev->next;
    }
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
  ProtectedNode *protected_lists_{new ProtectedNode{kInitialEpoch, nullptr}};
};

}  // namespace dbgroup::memory

#endif  // MEMORY_COMPONENT_EPOCH_MANAGER_HPP
