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

#ifndef MEMORY_COMPONENT_GARBAGE_LIST_HPP
#define MEMORY_COMPONENT_GARBAGE_LIST_HPP

#include <array>
#include <atomic>
#include <limits>
#include <memory>
#include <mutex>
#include <tuple>
#include <utility>

#include "common.hpp"

namespace dbgroup::memory::component
{
/**
 * @brief A class to represent a buffer of garbage instances.
 *
 * @tparam Target a target class of garbage collection.
 */
template <class Target>
class GarbageList
{
 public:
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using T = typename Target::T;

  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  /**
   * @brief Construct a new instance.
   *
   */
  constexpr GarbageList() = default;

  GarbageList(const GarbageList &) = delete;
  auto operator=(const GarbageList &) -> GarbageList & = delete;
  GarbageList(GarbageList &&) = delete;
  auto operator=(GarbageList &&) -> GarbageList & = delete;

  /*####################################################################################
   * Public destructors
   *##################################################################################*/

  /**
   * @brief Destroy the instance.
   *
   */
  ~GarbageList()
  {
    const auto destructed_idx = destructed_idx_.load(std::memory_order_relaxed);
    const auto end_idx = end_idx_.load(std::memory_order_acquire);

    // release unprotected garbage
    auto idx = begin_idx_.load(std::memory_order_relaxed);
    for (; idx < destructed_idx; ++idx) {
      // the garbage has been already destructed
      Target::deleter(garbage_.at(idx).ptr);
    }
    for (; idx < end_idx; ++idx) {
      auto *ptr = garbage_.at(idx).ptr;
      if constexpr (!std::is_same_v<T, void>) {
        ptr->~T();
      }
      Target::deleter(ptr);
    }
  }

  /*####################################################################################
   * Public getters/setters
   *##################################################################################*/

  /**
   * @return the number of unreleased garbases in entire lists.
   */
  [[nodiscard]] auto
  Size() const  //
      -> size_t
  {
    const auto end_idx = end_idx_.load(std::memory_order_acquire);
    const auto size = end_idx - destructed_idx_.load(std::memory_order_relaxed);

    if (end_idx < kGarbageBufferSize) return size;
    return next_->Size() + size;
  }

  /**
   * @retval true if this list is empty.
   * @retval false otherwise
   */
  [[nodiscard]] auto
  Empty() const  //
      -> bool
  {
    const auto end_idx = end_idx_.load(std::memory_order_acquire);
    const auto size = end_idx - destructed_idx_.load(std::memory_order_relaxed);

    if (size > 0) return false;
    if (end_idx < kGarbageBufferSize) return true;
    return next_->Empty();
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
  static auto
  AddGarbage(  //
      GarbageList *buffer,
      const size_t epoch,
      T *garbage)  //
      -> GarbageList *
  {
    const auto end_idx = buffer->end_idx_.load(std::memory_order_relaxed);

    // insert a new garbage
    buffer->garbage_.at(end_idx).epoch = epoch;
    buffer->garbage_.at(end_idx).ptr = garbage;

    // check whether the list is full
    GarbageList *return_buf = buffer;
    if (end_idx >= kGarbageBufferSize - 1) {
      return_buf = new GarbageList{};
      buffer->next_ = return_buf;
    }

    // increment the end position
    buffer->end_idx_.fetch_add(1, std::memory_order_release);

    return return_buf;
  }

  /**
   * @brief Reuse a released memory page.
   *
   * @param buffer a garbage buffer to be reused.
   * @return the pair of a memory page and the current head buffer.
   */
  static auto
  ReusePage(GarbageList *buffer)  //
      -> std::pair<void *, GarbageList *>
  {
    const auto idx = buffer->begin_idx_.load(std::memory_order_relaxed);
    const auto destructed_idx = buffer->destructed_idx_.load(std::memory_order_acquire);

    // check whether there are released garbage
    if (idx >= destructed_idx) return {nullptr, buffer};

    // get a released page
    buffer->begin_idx_.fetch_add(1, std::memory_order_relaxed);
    auto *page = buffer->garbage_.at(idx).ptr;

    // check whether all the pages in the list are reused
    if (idx >= kGarbageBufferSize - 1) {
      // the list has become empty, so delete it
      auto *empty_buf = buffer;
      buffer = empty_buf->next_;
      delete empty_buf;
    }

    return {page, buffer};
  }

  /**
   * @brief Destruct garbage where their epoch is less than a protected one.
   *
   * @param buffer a target barbage buffer.
   * @param protected_epoch a protected epoch.
   * @return GarbageList* the head of a garbage buffer that has protected gabages.
   */
  static auto
  Destruct(  //
      GarbageList *buffer,
      const size_t protected_epoch)  //
      -> GarbageList *
  {
    // release unprotected garbage
    const auto end_idx = buffer->end_idx_.load(std::memory_order_acquire);
    auto idx = buffer->destructed_idx_.load(std::memory_order_relaxed);
    for (; idx < end_idx; ++idx) {
      if (buffer->garbage_.at(idx).epoch >= protected_epoch) break;

      // only call destructor to reuse pages
      if constexpr (!std::is_same_v<T, void>) {
        buffer->garbage_.at(idx).ptr->~T();
      }
    }

    // update the position to make visible destructed garbage
    auto *next_buf = (idx < kGarbageBufferSize) ? buffer : buffer->next_;
    buffer->destructed_idx_.store(idx, std::memory_order_release);

    if (next_buf != buffer) {
      // release the next buffer recursively
      next_buf = GarbageList::Destruct(next_buf, protected_epoch);
    }

    return next_buf;
  }

  /**
   * @brief Release garbage where their epoch is less than a protected one.
   *
   * @param buffer a target barbage buffer.
   * @param protected_epoch a protected epoch.
   * @return GarbageList* a head of garbage buffers.
   */
  static auto
  Clear(  //
      GarbageList *buffer,
      const size_t protected_epoch)  //
      -> GarbageList *
  {
    const auto destructed_idx = buffer->destructed_idx_.load(std::memory_order_relaxed);
    const auto end_idx = buffer->end_idx_.load(std::memory_order_acquire);

    // release unprotected garbage
    auto idx = buffer->begin_idx_.load(std::memory_order_relaxed);
    for (; idx < destructed_idx; ++idx) {
      // the garbage has been already destructed
      Target::deleter(buffer->garbage_.at(idx).ptr);
    }
    for (; idx < end_idx; ++idx) {
      if (buffer->garbage_.at(idx).epoch >= protected_epoch) break;

      auto *ptr = buffer->garbage_.at(idx).ptr;
      if constexpr (!std::is_same_v<T, void>) {
        ptr->~T();
      }
      Target::deleter(ptr);
    }
    buffer->begin_idx_.store(idx, std::memory_order_relaxed);
    buffer->destructed_idx_.store(idx, std::memory_order_relaxed);

    if (idx < kGarbageBufferSize) {
      // the buffer has unreleased garbage
      return buffer;
    }

    // release the next buffer recursively
    auto *next = buffer->next_;
    delete buffer;
    return GarbageList::Clear(next, protected_epoch);
  }

 private:
  /*####################################################################################
   * Internal classes
   *##################################################################################*/

  /**
   * @brief A class to represent the pair of an epoch value and a registered garbage.
   *
   */
  struct Garbage {
    /// an epoch value when the garbage is registered.
    size_t epoch{};

    /// a pointer to the registered garbage.
    T *ptr{};
  };

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// the index to represent a head position.
  std::atomic_size_t begin_idx_{0};

  /// the end of released indexes
  std::atomic_size_t destructed_idx_{0};

  /// the index to represent a tail position.
  std::atomic_size_t end_idx_{0};

  /// a pointer to a next garbage buffer.
  GarbageList *next_{nullptr};

  /// a buffer of garbage instances with added epochs.
  std::array<Garbage, kGarbageBufferSize> garbage_{};
};

/**
 * @brief A class for representing linked-list nodes.
 *
 * @tparam Target a target class of garbage collection.
 */
template <class Target>
struct alignas(kCashLineSize) GarbageNode {
 public:
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using GarbageList_t = GarbageList<Target>;
  using GarbageList_p = GarbageList_t *;
  using GarbageNode_p = GarbageNode *;

  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  /**
   * @brief Construct a new GarbageNode object.
   *
   * @param list an initial garbage list.
   * @param next the next garbage node.
   */
  GarbageNode(  //
      GarbageList_t *list,
      GarbageNode *next)
      : mid_{list}, head_{list}, next_{next}
  {
  }

  GarbageNode(const GarbageNode &) = delete;
  GarbageNode(GarbageNode &&) = delete;

  auto operator=(const GarbageNode &) -> GarbageNode & = delete;
  auto operator=(GarbageNode &&) -> GarbageNode & = delete;

  /*####################################################################################
   * Public destructors
   *##################################################################################*/

  ~GarbageNode() = default;

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
    expired_.store(true, std::memory_order_relaxed);
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
    auto [page, head] = GarbageList_t::ReusePage(head_.load(std::memory_order_relaxed));
    head_.store(head, std::memory_order_relaxed);

    return page;
  }

  /*####################################################################################
   * Public utility functions for GC threads
   *##################################################################################*/

  /**
   * @brief Release registered garbage if possible.
   *
   * @param protected_epoch an epoch value to check whether garbage can be freed.
   * @param prev_mtx the previous mutex object.
   * @param next_on_prev_node the memory address of `next_` on the previous node.
   * @retval true if all the node are removed.
   * @retval false otherwise.
   */
  static auto
  ClearGarbage(  //
      const size_t protected_epoch,
      std::mutex *prev_mtx,
      GarbageNode **next_on_prev_node)  //
      -> bool
  {
    prev_mtx->lock();

    // check all the nodes are removed
    auto *node = *next_on_prev_node;
    if (node == nullptr) {
      prev_mtx->unlock();
      return true;
    }

    // release garbage or remove expired nodes
    while (node != nullptr) {
      if (!node->list_mtx_.try_lock()) {
        // the other thread is modifying this node, so go to the next node
        node->next_mtx_.lock();
        prev_mtx->unlock();
        prev_mtx = &(node->next_mtx_);
        next_on_prev_node = &(node->next_);
        node = node->next_;
        continue;
      }

      if (node->expired_.load(std::memory_order_relaxed)) {
        // this node can be removed
        auto *list = node->head_.load(std::memory_order_relaxed);
        if (list == nullptr) {
          // remove this node
          node->next_mtx_.lock();  // wait for other threads
          *next_on_prev_node = node->next_;
          delete node;
          node = *next_on_prev_node;
          continue;
        }

        // release remaining garbage
        prev_mtx->unlock();
        list = GarbageList_t::Clear(list, protected_epoch);
        if (list->Empty()) {
          delete list;
          list = nullptr;
        }
        node->head_.store(list, std::memory_order_relaxed);
      } else {
        // release/destruct garbage
        prev_mtx->unlock();
        if (Target::kReusePages) {
          node->mid_ = GarbageList_t::Destruct(node->mid_, protected_epoch);
        } else {
          node->mid_ = GarbageList_t::Clear(node->mid_, protected_epoch);
          node->head_.store(node->mid_, std::memory_order_relaxed);
        }
      }

      // go to the next node
      node->next_mtx_.lock();
      node->list_mtx_.unlock();
      prev_mtx = &(node->next_mtx_);
      next_on_prev_node = &(node->next_);
      node = node->next_;
    }

    prev_mtx->unlock();
    return false;
  }

 private:
  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// @brief A mutex object for locking a garbage list.
  std::mutex list_mtx_{};

  /// @brief A garbage list that has not destructed pages.
  GarbageList_t *mid_{nullptr};

  /// @brief A flag for indicating the corresponding thread has exited.
  std::atomic_bool expired_{false};

  /// @brief A garbage list that has destructed pages.
  std::atomic<GarbageList_t *> head_{nullptr};

  /// @brief A mutex object for locking the pointer to the next node.
  std::mutex next_mtx_{};

  /// @brief The next node.
  GarbageNode *next_{nullptr};
};

}  // namespace dbgroup::memory::component

#endif  // MEMORY_COMPONENT_GARBAGE_LIST_HPP
