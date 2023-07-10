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

// C++ standard libraries
#include <algorithm>
#include <atomic>
#include <chrono>
#include <memory>
#include <shared_mutex>
#include <thread>
#include <utility>
#include <vector>

// external sources
#include "thread/id_manager.hpp"

// local sources
#include "memory/component/epoch_guard.hpp"

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

  using IDManager = ::dbgroup::thread::IDManager;
  using Epoch = component::Epoch;
  using EpochGuard = component::EpochGuard;

  /*####################################################################################
   * Public constants
   *##################################################################################*/

  /// The capacity of nodes for protected epochs.
  static constexpr size_t kCapacity = 256;

  /// The initial value of epochs.
  static constexpr size_t kInitialEpoch = kCapacity;

  /// The minimum value of epochs.
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
    auto &tls = tls_fields_[IDManager::GetThreadID()];
    if (tls.heartbeat.expired()) {
      tls.epoch.SetGrobalEpoch(&global_epoch_);
      tls.heartbeat = IDManager::GetHeartBeat();
    }

    return EpochGuard{&(tls.epoch)};
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
   * @brief A class for representing thread local epoch storages.
   *
   */
  struct alignas(kCashLineSize) TLSEpoch {
    /// An epoch object for each thread.
    Epoch epoch{};

    /// A flag for indicating the corresponding thread has exited.
    std::weak_ptr<size_t> heartbeat{};
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
     * @brief Get protected epoch values based on a given epoch.
     *
     * @param epoch a target epoch value.
     * @param node the head pointer of a linked list.
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

    /**
     * @return the upper bits of the current epoch.
     */
    [[nodiscard]] constexpr auto
    GetUpperBits() const  //
        -> size_t
    {
      return upper_epoch_;
    }

    /*##################################################################################
     * Public member variables
     *################################################################################*/

    /// A pointer to the next node.
    ProtectedNode *next{nullptr};  // NOLINT

   private:
    /*##################################################################################
     * Internal member variables
     *################################################################################*/

    /// The upper bits of epoch values to be retained in this node.
    size_t upper_epoch_{};

    /// The list of protected epochs.
    std::array<std::vector<size_t>, kCapacity> epoch_lists_{};
  };

  /*####################################################################################
   * Internal constants
   *##################################################################################*/

  /// The expected maximum number of threads.
  static constexpr size_t kMaxThreadNum = ::dbgroup::thread::kMaxThreadNum;

  /// A bitmask for extracting lower bits from epochs.
  static constexpr size_t kLowerMask = kCapacity - 1UL;

  /// A bitmask for extracting upper bits from epochs.
  static constexpr size_t kUpperMask = ~kLowerMask;

  /*####################################################################################
   * Internal utility functions
   *##################################################################################*/

  /**
   * @brief Collect epoch value for epoch-based protection.
   *
   * This function also removes dead epochs from the internal list while computing.
   *
   * @param cur_epoch the current global epoch value.
   * @param protected_epochs protected epoch values.
   */
  void
  CollectProtectedEpochs(  //
      const size_t cur_epoch,
      std::vector<size_t> &protected_epochs)
  {
    protected_epochs.reserve(kMaxThreadNum);
    protected_epochs.emplace_back(cur_epoch + 1);  // reserve the next epoch
    protected_epochs.emplace_back(cur_epoch);

    for (size_t i = 0; i < kMaxThreadNum; ++i) {
      auto &tls = tls_fields_[i];
      if (tls.heartbeat.expired()) continue;

      const auto protected_epoch = tls.epoch.GetProtectedEpoch();
      if (protected_epoch < std::numeric_limits<size_t>::max()) {
        protected_epochs.emplace_back(protected_epoch);
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

  /// A global epoch counter.
  std::atomic_size_t global_epoch_{kInitialEpoch};

  /// The minimum protected ecpoch value.
  std::atomic_size_t min_epoch_{kInitialEpoch};

  /// The head pointer of a linked list of epochs.
  ProtectedNode *protected_lists_{new ProtectedNode{kInitialEpoch, nullptr}};

  /// The array of epochs to use as thread local storages.
  TLSEpoch tls_fields_[kMaxThreadNum]{};
};

}  // namespace dbgroup::memory

#endif  // MEMORY_COMPONENT_EPOCH_MANAGER_HPP
