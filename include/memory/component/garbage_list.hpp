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
   * @brief Create a new GarbageList object.
   *
   */
  GarbageList() : tail_{new GarbageBuffer{}}, head_{tail_}, inter_{tail_} {}

  GarbageList(const GarbageList &) = delete;
  auto operator=(const GarbageList &) -> GarbageList & = delete;
  GarbageList(GarbageList &&) = delete;
  auto operator=(GarbageList &&) -> GarbageList & = delete;

  /*####################################################################################
   * Public destructor
   *##################################################################################*/

  /**
   * @brief Destroy the GarbageList object.
   *
   */
  ~GarbageList()
  {
    auto *current = head_.load(std::memory_order_relaxed);
    while (current != nullptr && !Empty()) {
      ClearGarbage(std::numeric_limits<size_t>::max());
    }
    delete tail_;
  }

  /*####################################################################################
   * Public getters/setters
   *##################################################################################*/

  /**
   * @retval true if the list is empty.
   * @retval false otherwise.
   */
  [[nodiscard]] auto
  Empty() const  //
      -> bool
  {
    return head_.load(std::memory_order_relaxed)->Empty();
  }

  /**
   * @return the number of non-destructed garbases in the list.
   */
  [[nodiscard]] auto
  Size() const  //
      -> size_t
  {
    return inter_.load(std::memory_order_relaxed)->Size();
  }

  /*####################################################################################
   * Public utility functions
   *##################################################################################*/

  /**
   * @brief Add a new garbage instance.
   *
   * @param epoch an epoch value when a garbage is added.
   * @param garbage_ptr a pointer to a target garbage.
   */
  void
  AddGarbage(  //
      const size_t epoch,
      T *garbage_ptr)
  {
    tail_ = GarbageBuffer::AddGarbage(tail_, epoch, garbage_ptr);
  }

  /**
   * @brief Reuse a released memory page if it exists in the list.
   *
   * @retval nullptr if the list does not have reusable pages.
   * @retval a memory page.
   */
  auto
  GetPageIfPossible()  //
      -> void *
  {
    auto [page, head] = GarbageBuffer::ReusePage(head_.load(std::memory_order_relaxed));
    head_.store(head, std::memory_order_relaxed);

    return page;
  }

  /**
   * @brief Destruct unprotected garbage for reusing.
   *
   * @param protected_epoch an epoch value to check epoch protection.
   */
  void
  DestructGarbage(const size_t protected_epoch)
  {
    auto *inter = GarbageBuffer::Destruct(inter_.load(std::memory_order_relaxed), protected_epoch);
    inter_.store(inter, std::memory_order_relaxed);
  }

  /**
   * @brief Release unprotected garbage.
   *
   * @param protected_epoch an epoch value to check epoch protection.
   */
  void
  ClearGarbage(const size_t protected_epoch)
  {
    auto *head = GarbageBuffer::Clear(head_.load(std::memory_order_relaxed), protected_epoch);
    head_.store(head, std::memory_order_relaxed);
    inter_.store(head, std::memory_order_relaxed);
  }

 private:
  /*####################################################################################
   * Internal classes
   *##################################################################################*/

  /**
   * @brief A class to represent internal buffers for garbage lists.
   *
   */
  class GarbageBuffer
  {
   public:
    /*##################################################################################
     * Public constructors and assignment operators
     *################################################################################*/

    /**
     * @brief Construct a new instance.
     *
     */
    constexpr GarbageBuffer() = default;

    GarbageBuffer(const GarbageBuffer &) = delete;
    auto operator=(const GarbageBuffer &) -> GarbageBuffer & = delete;
    GarbageBuffer(GarbageBuffer &&) = delete;
    auto operator=(GarbageBuffer &&) -> GarbageBuffer & = delete;

    /*##################################################################################
     * Public destructors
     *################################################################################*/

    /**
     * @brief Destroy the instance.
     *
     */
    ~GarbageBuffer()
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

    /*##################################################################################
     * Public getters/setters
     *################################################################################*/

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

    /*##################################################################################
     * Public utility functions
     *################################################################################*/

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
        GarbageBuffer *buffer,
        const size_t epoch,
        T *garbage)  //
        -> GarbageBuffer *
    {
      const auto end_idx = buffer->end_idx_.load(std::memory_order_relaxed);

      // insert a new garbage
      buffer->garbage_.at(end_idx).epoch = epoch;
      buffer->garbage_.at(end_idx).ptr = garbage;

      // check whether the list is full
      GarbageBuffer *return_buf = buffer;
      if (end_idx >= kGarbageBufferSize - 1) {
        return_buf = new GarbageBuffer{};
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
    ReusePage(GarbageBuffer *buffer)  //
        -> std::pair<void *, GarbageBuffer *>
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
     * @return GarbageBuffer* the head of a garbage buffer that has protected gabages.
     */
    static auto
    Destruct(  //
        GarbageBuffer *buffer,
        const size_t protected_epoch)  //
        -> GarbageBuffer *
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
        next_buf = GarbageBuffer::Destruct(next_buf, protected_epoch);
      }

      return next_buf;
    }

    /**
     * @brief Release garbage where their epoch is less than a protected one.
     *
     * @param buffer a target barbage buffer.
     * @param protected_epoch a protected epoch.
     * @return GarbageBuffer* a head of garbage buffers.
     */
    static auto
    Clear(  //
        GarbageBuffer *buffer,
        const size_t protected_epoch)  //
        -> GarbageBuffer *
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
      return GarbageBuffer::Clear(next, protected_epoch);
    }

   private:
    /*##################################################################################
     * Internal classes
     *################################################################################*/

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

    /*##################################################################################
     * Internal member variables
     *################################################################################*/

    /// a buffer of garbage instances with added epochs.
    std::array<Garbage, kGarbageBufferSize> garbage_{};

    /// the index to represent a head position.
    std::atomic_size_t begin_idx_{0};

    /// the end of released indexes
    std::atomic_size_t destructed_idx_{0};

    /// the index to represent a tail position.
    std::atomic_size_t end_idx_{0};

    /// a pointer to a next garbage buffer.
    GarbageBuffer *next_{nullptr};
  };

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// a pointer to the tail a target garbage list.
  GarbageBuffer *tail_{nullptr};

  /// a pointer to the head of a target garbage list.
  std::atomic<GarbageBuffer *> head_{};

  /// a pointer to the internal node of a target garbage list.
  std::atomic<GarbageBuffer *> inter_{};
};

}  // namespace dbgroup::memory::component

#endif  // MEMORY_COMPONENT_GARBAGE_LIST_HPP
