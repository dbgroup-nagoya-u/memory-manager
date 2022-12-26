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
#include <libpmemobj++/detail/common.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/mutex.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pexceptions.hpp>
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
    PMEMoid oid{OID_NULL};
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
   * @param pool a pool object for managing persistent memory.
   * @return a pointer to a current tail garbage buffer.
   */
  template <class PMEMPool>
  static auto
  AddGarbage(  //
      GarbageList_p buffer,
      const size_t epoch,
      PMEMoid *garbage,
      PMEMPool &pool)  //
      -> GarbageList_p
  {
    const auto pos = buffer->end_idx;
    auto return_buf = buffer;

    auto &elem = buffer->garbage_arr[pos];
    elem.epoch = epoch;  // epoch can be outside of a transaction
    try {
      ::pmem::obj::flat_transaction::run(pool, [&] {
        // insert a new garbage
        pmemobj_tx_add_range_direct(&(elem.oid), sizeof(PMEMoid));
        elem.oid = *garbage;
        pmemobj_tx_add_range_direct(garbage, sizeof(PMEMoid));
        *garbage = OID_NULL;

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
   * @brief Reuse a garbage-collected memory page.
   *
   * @param buffer a garbage buffer to be reused.
   * @param out_page a persistent pointer to be stored a reusable page.
   * @param pool a pool object for managing persistent memory.
   * @return the current head buffer.
   */
  template <class PMEMPool>
  static auto
  ReusePage(  //
      GarbageList_p buffer,
      PMEMoid *out_page,
      PMEMPool &pool)  //
      -> GarbageList_p
  {
    const auto pos = buffer->begin_idx_atom.load(std::memory_order_relaxed);
    const auto mid_pos = buffer->mid_idx_atom.load(std::memory_order_acquire);

    // check whether there are released garbage
    if (pos >= mid_pos) return buffer;

    // get a released page
    auto &oid = buffer->garbage_arr[pos].oid;
    try {
      ::pmem::obj::flat_transaction::run(pool, [&] {
        pmemobj_tx_add_range_direct(out_page, sizeof(PMEMoid));
        *out_page = oid;
        pmemobj_tx_add_range_direct(&oid, sizeof(PMEMoid));
        oid = OID_NULL;
        buffer->begin_idx = pos + 1;
      });
    } catch (const std::exception &e) {
      std::cerr << e.what() << std::endl;
      std::terminate();
    }
    buffer->begin_idx_atom.fetch_add(1, std::memory_order_relaxed);

    // check whether all the pages in the list are reused
    if (pos >= kGarbageBufferSize - 1) {
      // the list has become empty, so delete it
      buffer = buffer->next;
    }
    return buffer;
  }

  /**
   * @brief Destruct garbage where their epoch is less than a protected one.
   *
   * @param buffer a target barbage buffer.
   * @param protected_epoch a protected epoch.
   * @param pool a pool object for managing persistent memory.
   * @return a head of garbage buffers.
   */
  template <class PMEMPool>
  static auto
  Destruct(  //
      GarbageList_p buffer,
      const size_t protected_epoch,
      PMEMPool &pool)  //
      -> GarbageList_p
  {
    size_t pos{};

    while (true) {
      try {
        const auto end_pos = buffer->end_idx_atom.load(std::memory_order_acquire);

        // release unprotected garbage
        pos = buffer->mid_idx.get_ro();
        ::pmem::obj::flat_transaction::run(pool, [&] {
          for (; pos < end_pos; ++pos) {
            auto &elem = buffer->garbage_arr[pos];
            if (elem.epoch >= protected_epoch) break;
            if constexpr (!std::is_same_v<T, void>) {
              auto *garbage = static_cast<T *>(pmemobj_direct(elem.oid));
              pmemobj_tx_add_range_direct(garbage, sizeof(T));
              garbage->~T();
            }
          }
          buffer->mid_idx = pos;
        });
      } catch (const std::exception &e) {
        std::cerr << e.what() << std::endl;
        std::terminate();
      }

      buffer->mid_idx_atom.store(pos, std::memory_order_release);
      if (pos < kGarbageBufferSize) return buffer;
      // go to the next buffer
      buffer = buffer->next;
    }
  }

  /**
   * @brief Release garbage where their epoch is less than a protected one.
   *
   * @param buffer a target barbage buffer.
   * @param protected_epoch a protected epoch.
   * @param pool a pool object for managing persistent memory.
   * @return a head of garbage buffers.
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
      const auto mid_pos = buffer->mid_idx.get_ro();
      const auto end_pos = buffer->end_idx_atom.load(std::memory_order_acquire);

      // release unprotected garbage
      pos = buffer->begin_idx_atom.load(std::memory_order_relaxed);
      ::pmem::obj::flat_transaction::run(pool, [&] {
        for (; pos < mid_pos; ++pos) {
          if (buffer->garbage_arr[pos].epoch >= protected_epoch) break;
          auto &oid = buffer->garbage_arr[pos].oid;
          pmemobj_tx_add_range_direct(&oid, sizeof(PMEMoid));
          if (pmemobj_tx_free(oid) != 0) {
            throw ::pmem::transaction_free_error{"failed to delete persistent memory object"};
          }
          oid = OID_NULL;
        }
        for (; pos < end_pos; ++pos) {
          if (buffer->garbage_arr[pos].epoch >= protected_epoch) break;
          auto &oid = buffer->garbage_arr[pos].oid;
          if constexpr (!std::is_same_v<T, void>) {
            auto *garbage = static_cast<T *>(pmemobj_direct(oid));
            pmemobj_tx_add_range_direct(garbage, sizeof(T));
            garbage->~T();
          }
          pmemobj_tx_add_range_direct(&oid, sizeof(PMEMoid));
          if (pmemobj_tx_free(oid) != 0) {
            throw ::pmem::transaction_free_error{"failed to delete persistent memory object"};
          }
          oid = OID_NULL;
        }
        buffer->begin_idx = pos;
      });
    } catch (const std::exception &e) {
      std::cerr << e.what() << std::endl;
      std::terminate();
    }

    buffer->begin_idx_atom.store(pos, std::memory_order_relaxed);
    if (pos >= kGarbageBufferSize) {
      // the buffer should be released
      buffer = buffer->next;
    }
    return buffer;
  }

  /**
   * @brief Release garbage where their epoch is less than a protected one.
   *
   * @param buffer a target barbage buffer.
   * @param pool a pool object for managing persistent memory.
   * @return a head of garbage buffers.
   */
  template <class PMEMPool>
  static auto
  Release(  //
      GarbageList_p buffer,
      PMEMPool &pool)  //
      -> GarbageList_p
  {
    size_t pos{};

    try {
      const auto mid_pos = buffer->mid_idx.get_ro();
      const auto end_pos = buffer->end_idx.get_ro();

      // release unprotected garbage
      pos = buffer->begin_idx.get_ro();
      ::pmem::obj::flat_transaction::run(pool, [&] {
        for (; pos < mid_pos; ++pos) {
          auto &oid = buffer->garbage_arr[pos].oid;
          pmemobj_tx_add_range_direct(&oid, sizeof(PMEMoid));
          if (pmemobj_tx_free(oid) != 0) {
            throw ::pmem::transaction_free_error{"failed to delete persistent memory object"};
          }
          oid = OID_NULL;
        }
        for (; pos < end_pos; ++pos) {
          auto &oid = buffer->garbage_arr[pos].oid;
          if constexpr (!std::is_same_v<T, void>) {
            auto *garbage = static_cast<T *>(pmemobj_direct(oid));
            pmemobj_tx_add_range_direct(garbage, sizeof(T));
            garbage->~T();
          }
          pmemobj_tx_add_range_direct(&oid, sizeof(PMEMoid));
          if (pmemobj_tx_free(oid) != 0) {
            throw ::pmem::transaction_free_error{"failed to delete persistent memory object"};
          }
          oid = OID_NULL;
        }
        buffer->begin_idx = pos;
      });
    } catch (const std::exception &e) {
      std::cerr << e.what() << std::endl;
      std::terminate();
    }

    if (pos >= kGarbageBufferSize) {
      // the buffer should be released
      buffer = buffer->next;
    }
    return buffer;
  }

  /*####################################################################################
   * Public member variables // they must be public to access via persistent_ptr
   *##################################################################################*/

  /// @brief the position of the head of destructed garbage.
  ::pmem::obj::p<size_t> begin_idx{0};

  /// @brief the position of the head of unreleased garbage.
  ::pmem::obj::p<size_t> mid_idx{0};

  /// @brief the position of the tail of unreleased garbage.
  ::pmem::obj::p<size_t> end_idx{0};

  /// @brief the atomic instance to notify cleaner threads of the begin position.
  std::atomic_size_t begin_idx_atom{0};

  /// @brief the atomic instance to notify cleaner threads of the middle position.
  std::atomic_size_t mid_idx_atom{0};

  /// @brief the atomic instance to notify cleaner threads of the tail position.
  std::atomic_size_t end_idx_atom{0};

  /// @brief a pointer to a next garbage buffer.
  GarbageList_p next{nullptr};

  /// @brief a buffer of garbage instances with added epochs.
  Garbage garbage_arr[kGarbageBufferSize]{};
};

/**
 * @brief A class for representing linked-list nodes.
 *
 * @tparam Target a target class of garbage collection.
 */
template <class Target>
class GarbageNodeOnPMEM
{
 public:
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using T = typename Target::T;
  using GarbageList_t = GarbageListOnPMEM<Target>;
  using GarbageList_p = ::pmem::obj::persistent_ptr<GarbageList_t>;
  using GarbageNode_p = ::pmem::obj::persistent_ptr<GarbageNodeOnPMEM>;
  using Target_p = ::pmem::obj::persistent_ptr<T>;

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
      : head{list}, mid{std::move(list)}, next{std::move(next)}
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
  template <class PMEMPool>
  void
  GetPageIfPossible(  //
      PMEMoid *out_page,
      PMEMPool &pool)
  {
    auto &&next = GarbageList_t::ReusePage(head, out_page, pool);
    if (next == head) return;

    // if the current buffer has become empty, release it
    ::pmem::obj::flat_transaction::run(pool, [&] {
      ::pmem::obj::delete_persistent<GarbageList_t>(head);
      head = std::move(next);
    });
    expired.store(false, std::memory_order_release);
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
        if (node->expired.load(std::memory_order_acquire)) {
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
          node->ClearGarbageInBuffer(protected_epoch, pool);
          if (node->head->Empty()) {
            ::pmem::obj::flat_transaction::run(pool, [&] {
              ::pmem::obj::delete_persistent<GarbageList_t>(node->head);
              node->head = nullptr;
            });
          }
        } else {
          // release/destruct garbage
          prev_mtx->unlock();
          if (Target::kReusePages) {
            node->mid = GarbageList_t::Destruct(node->mid, protected_epoch, pool);
          } else {
            node->ClearGarbageInBuffer(protected_epoch, pool);
          }
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

  /**
   * @brief Release all the registered garbage for recovery.
   *
   * @param next_on_prev_node the memory address of `next` on the previous node.
   */
  static void
  ReleaseAllGarbage(GarbageNode_p *next_on_prev_node)
  {
    // check all the nodes are removed
    auto node = *next_on_prev_node;
    if (node == nullptr) {
      return;
    }

    // release garbage or remove expired nodes
    auto &&pool = ::pmem::obj::pool_by_pptr(node);
    while (node != nullptr) {
      try {
        if (node->head != nullptr) {
          // release all the registered garbage
          while (true) {
            auto &&next = GarbageList_t::Release(node->head, pool);
            if (next == node->head) break;
            ::pmem::obj::flat_transaction::run(pool, [&] {
              ::pmem::obj::delete_persistent<GarbageList_t>(node->head);
              node->head = std::move(next);
            });
          }
          ::pmem::obj::flat_transaction::run(pool, [&] {
            ::pmem::obj::delete_persistent<GarbageList_t>(node->head);
            node->head = nullptr;
          });
        }

        // remove this node
        ::pmem::obj::flat_transaction::run(pool, [&] {
          *next_on_prev_node = node->next;
          ::pmem::obj::delete_persistent<GarbageNodeOnPMEM>(node);
          node = nullptr;
        });
      } catch (const std::exception &e) {
        std::cerr << e.what() << std::endl;
        std::terminate();
      }

      // go to the next node
      node = *next_on_prev_node;
    }
  }

  /*####################################################################################
   * Public member variables // they must be public to access via persistent_ptr
   *##################################################################################*/

  /// @brief A mutex object for locking a garbage list.
  std::mutex list_mtx{};

  /// @brief A garbage list that has destructed pages.
  GarbageList_p head{nullptr};

  /// @brief A garbage list that has not destructed pages.
  GarbageList_p mid{nullptr};

  /// @brief A flag for indicating the corresponding thread has exited.
  std::atomic_bool expired{false};

  /// @brief A mutex object for locking the pointer to the next node.
  std::mutex next_mtx{};

  /// @brief The next node.
  GarbageNode_p next{nullptr};

 private:
  /*####################################################################################
   * Internal utility functions
   *##################################################################################*/

  /**
   * @brief Clear garbage in linked-buffers.
   *
   * @tparam PMEMPool a class of a persistent pool.
   * @param protected_epoch an epoch value to check whether garbage can be freed.
   * @param pool a pool object for managing persistent memory.
   */
  template <class PMEMPool>
  void
  ClearGarbageInBuffer(  //
      const size_t protected_epoch,
      PMEMPool &pool)
  {
    while (true) {
      auto &&next = GarbageList_t::Clear(head, protected_epoch, pool);
      if (next == head) break;
      ::pmem::obj::flat_transaction::run(pool, [&] {
        ::pmem::obj::delete_persistent<GarbageList_t>(head);
        head = std::move(next);
      });
    }
  }
};

}  // namespace dbgroup::memory::component

#endif  // MEMORY_COMPONENT_GARBAGE_LIST_ON_PMEM_HPP
