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

#ifndef MEMORY_COMPONENT_GARBAGE_LIST_ON_PMEM_HPP
#define MEMORY_COMPONENT_GARBAGE_LIST_ON_PMEM_HPP

// C++ standard libraries
#include <array>
#include <atomic>
#include <iostream>
#include <limits>
#include <tuple>
#include <utility>

// external system libraries
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/mutex.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/utils.hpp>

// local sources
#include "common.hpp"

namespace dbgroup::memory::component
{
/**
 * @brief A class to represent a buffer of garbage instances.
 *
 * @tparam Target a target class of garbage collection.
 */
template <class Target>
class GarbageListOnPMEM
{
 public:
  /*####################################################################################
   * Public classes
   *##################################################################################*/

  /**
   * @brief A class to represent the pair of an epoch value and a registered garbage.
   *
   */
  struct Garbage {
    /// an epoch value when the garbage is registered.
    size_t epoch{};

    /// a pointer to the registered garbage.
    PMEMoid ptr{};
  };

  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using T = typename Target::T;
  using GarbageList_p = ::pmem::obj::persistent_ptr<GarbageListOnPMEM>;
  using Target_p = ::pmem::obj::persistent_ptr<T>;

  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  /**
   * @brief Construct a new instance.
   *
   */
  constexpr GarbageListOnPMEM() = default;

  GarbageListOnPMEM(const GarbageListOnPMEM &) = delete;
  auto operator=(const GarbageListOnPMEM &) -> GarbageListOnPMEM & = delete;
  GarbageListOnPMEM(GarbageListOnPMEM &&) = delete;
  auto operator=(GarbageListOnPMEM &&) -> GarbageListOnPMEM & = delete;

  /*####################################################################################
   * Public destructors
   *##################################################################################*/

  /**
   * @brief Destroy the instance.
   *
   */
  ~GarbageListOnPMEM() = default;

  /*####################################################################################
   * Public getters/setters
   *##################################################################################*/

  /**
   * @retval true if this list is empty.
   * @retval false otherwise
   */
  [[nodiscard]] auto
  Empty() const  //
      -> bool
  {
    const auto cur_idx = end_idx_atom.load(std::memory_order_acquire);
    const auto size = cur_idx - begin_idx.get_ro();

    if (size > 0) return false;
    if (cur_idx < kGarbageBufferSize) return true;
    return next->Empty();
  }

  /*####################################################################################
   * Public utility functions
   *##################################################################################*/

  /**
   * @brief Add a new garbage instance to a specified buffer.
   *
   * If the buffer becomes full, create a new garbage buffer and link them.
   *
   * @param buffer a garbage buffer to be added.
   * @param epoch an epoch value when a garbage is added.
   * @param garbage a new garbage instance.
   * @return a pointer to a current tail garbage buffer.
   */
  template <class PMEMPool>
  static auto
  AddGarbage(  //
      GarbageList_p buffer,
      const size_t epoch,
      Target_p &garbage,
      PMEMPool &pool)  //
      -> GarbageList_p
  {
    const auto pos = buffer->end_idx;
    auto return_buf = buffer;

    try {
      ::pmem::obj::flat_transaction::run(pool, [&] {
        // insert a new garbage
        buffer->epochs[pos] = epoch;
        buffer->garbages[pos] = garbage;

        // check whether the list is full
        if (pos >= kGarbageBufferSize - 1) {
          buffer->next = ::pmem::obj::make_persistent<GarbageListOnPMEM>();
          return_buf = buffer->next;
        }

        // increment the end position
        buffer->end_idx = pos + 1;
      });
    } catch (const std::exception &e) {
      std::cerr << e.what() << std::endl;
      std::terminate();
    }

    // increment the end position with a fence
    buffer->end_idx_atom.fetch_add(1, std::memory_order_release);

    return return_buf;
  }

  /**
   * @brief Release garbage where their epoch is less than a protected one.
   *
   * @param buffer a target barbage buffer.
   * @param protected_epoch a protected epoch.
   * @return GarbageListOnPMEM* a head of garbage buffers.
   */
  template <class PMEMPool>
  static auto
  Clear(  //
      GarbageList_p buffer,
      const size_t protected_epoch,
      PMEMPool &pool)  //
      -> GarbageList_p
  {
    size_t pos{};

    try {
      while (true) {
        const auto end_pos = buffer->end_idx_atom.load(std::memory_order_acquire);

        // release unprotected garbage
        ::pmem::obj::flat_transaction::run(pool, [&] {
          pos = buffer->begin_idx.get_ro();
          for (; pos < end_pos; ++pos) {
            if (buffer->epochs[pos] >= protected_epoch) break;

            ::pmem::obj::delete_persistent<T>(buffer->garbages[pos]);
            buffer->garbages[pos] = nullptr;
          }
          buffer->begin_idx = pos;
        });

        if (pos < kGarbageBufferSize) {
          // the buffer has unreleased garbage
          return buffer;
        }

        // release the next buffer recursively
        auto next = buffer->next;
        ::pmem::obj::flat_transaction::run(pool, [&] {
          ::pmem::obj::delete_persistent<GarbageListOnPMEM>(buffer);
          buffer = nullptr;
        });
        buffer = std::move(next);
      }
    } catch (const std::exception &e) {
      std::cerr << e.what() << std::endl;
      std::terminate();
    }
  }

  /*####################################################################################
   * Public member variables // they must be public to access via persistent_ptr
   *##################################################################################*/

  /// the index to represent a head position.
  ::pmem::obj::p<size_t> begin_idx{0};

  /// the index to represent a tail position.
  ::pmem::obj::p<size_t> end_idx{0};

  /// the index to notify cleaner threads of a tail position.
  std::atomic_size_t end_idx_atom{0};

  /// a pointer to a next garbage buffer.
  GarbageList_p next{nullptr};

  /// a buffer of garbage instances with added epochs.
  ::pmem::obj::p<size_t> epochs[kGarbageBufferSize]{};

  Target_p garbages[kGarbageBufferSize]{};

 private:
  /*####################################################################################
   * Internal classes
   *##################################################################################*/
};

/**
 * @brief A class for representing linked-list nodes.
 *
 * @tparam Target a target class of garbage collection.
 */
template <class Target>
class alignas(kCashLineSize) GarbageNodeOnPMEM
{
 public:
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using GarbageList_t = GarbageListOnPMEM<Target>;
  using GarbageList_p = ::pmem::obj::persistent_ptr<GarbageList_t>;
  using GarbageNode_p = ::pmem::obj::persistent_ptr<GarbageNodeOnPMEM>;

  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  /**
   * @brief Construct a new GarbageNodeOnPMEM object.
   *
   * @param list an initial garbage list.
   * @param next the next garbage node.
   */
  GarbageNodeOnPMEM(  //
      GarbageList_p list,
      GarbageNode_p next)
      : head{std::move(list)}, next{std::move(next)}
  {
  }

  GarbageNodeOnPMEM(const GarbageNodeOnPMEM &) = delete;
  GarbageNodeOnPMEM(GarbageNodeOnPMEM &&) = delete;

  auto operator=(const GarbageNodeOnPMEM &) -> GarbageNodeOnPMEM & = delete;
  auto operator=(GarbageNodeOnPMEM &&) -> GarbageNodeOnPMEM & = delete;

  /*####################################################################################
   * Public destructors
   *##################################################################################*/

  ~GarbageNodeOnPMEM() = default;

  /*####################################################################################
   * Public setters
   *##################################################################################*/

  /**
   * @brief Set the expired flag for destruction.
   *
   */
  void
  Expire()
  {
    expired.store(true, std::memory_order_relaxed);
  }

  /*####################################################################################
   * Public utility functions for worker threads
   *##################################################################################*/

  /**
   * @brief Reuse a released memory page if it exists in the list.
   *
   * @retval nullptr if the list does not have reusable pages.
   * @retval a memory page otherwise.
   */
  auto
  GetPageIfPossible()  //
      -> void *
  {
    return nullptr;
  }

  /*####################################################################################
   * Public utility functions for GC threads
   *##################################################################################*/

  /**
   * @brief Release registered garbage if possible.
   *
   * @param protected_epoch an epoch value to check whether garbage can be freed.
   * @param prev_mtx the previous mutex object.
   * @param next_on_prev_node the memory address of `next` on the previous node.
   * @retval true if all the node are removed.
   * @retval false otherwise.
   */
  static auto
  ClearGarbage(  //
      const size_t protected_epoch,
      std::mutex *prev_mtx,
      GarbageNode_p *next_on_prev_node)  //
      -> bool
  {
    prev_mtx->lock();

    // check all the nodes are removed
    auto node = *next_on_prev_node;
    if (node == nullptr) {
      prev_mtx->unlock();
      return true;
    }

    // release garbage or remove expired nodes
    auto &&pool = ::pmem::obj::pool_by_pptr(node);
    while (node != nullptr) {
      if (!node->list_mtx.try_lock()) {
        // the other thread is modifying this node, so go to the next node
        node->next_mtx.lock();
        prev_mtx->unlock();
        prev_mtx = &(node->next_mtx);
        next_on_prev_node = &(node->next);
        node = node->next;
        continue;
      }

      try {
        if (node->expired.load(std::memory_order_relaxed)) {
          // this node can be removed
          if (node->head == nullptr) {
            // remove this node
            node->next_mtx.lock();  // wait for other threads
            ::pmem::obj::flat_transaction::run(pool, [&] {
              *next_on_prev_node = node->next;
              ::pmem::obj::delete_persistent<GarbageNodeOnPMEM>(node);
              node = nullptr;
            });

            node = *next_on_prev_node;
            continue;
          }

          // release remaining garbage
          prev_mtx->unlock();
          node->head = GarbageList_t::Clear(node->head, protected_epoch, pool);
          if (node->head->Empty()) {
            ::pmem::obj::flat_transaction::run(pool, [&] {
              ::pmem::obj::delete_persistent<GarbageList_t>(node->head);
              node->head = nullptr;
            });
          }
        } else {
          // release/destruct garbage
          prev_mtx->unlock();
          auto &&head = GarbageList_t::Clear(node->head, protected_epoch, pool);
          ::pmem::obj::flat_transaction::run(pool, [&] { node->head = std::move(head); });
        }
      } catch (const std::exception &e) {
        std::cerr << e.what() << std::endl;
        std::terminate();
      }

      // go to the next node
      node->next_mtx.lock();
      node->list_mtx.unlock();
      prev_mtx = &(node->next_mtx);
      next_on_prev_node = &(node->next);
      node = node->next;
    }

    prev_mtx->unlock();
    return false;
  }

  /*####################################################################################
   * Public member variables // they must be public to access via persistent_ptr
   *##################################################################################*/

  /// @brief A mutex object for locking a garbage list.
  std::mutex list_mtx{};

  /// @brief A garbage list that has not destructed pages.
  GarbageList_p head{nullptr};

  /// @brief A flag for indicating the corresponding thread has exited.
  std::atomic_bool expired{false};

  /// @brief A mutex object for locking the pointer to the next node.
  std::mutex next_mtx{};

  /// @brief The next node.
  GarbageNode_p next{nullptr};
};

}  // namespace dbgroup::memory::component

#endif  // MEMORY_COMPONENT_GARBAGE_LIST_ON_PMEM_HPP
