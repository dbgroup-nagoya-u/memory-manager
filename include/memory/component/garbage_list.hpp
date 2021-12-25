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
 * @tparam T a target class of garbage collection.
 */
template <class T>
class GarbageList
{
 public:
  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  /**
   * @brief Create a new GarbageList object.
   *
   */
  explicit GarbageList(const std::atomic_size_t &global_epoch)
      : tail_{new GarbageBuffer{global_epoch}}, head_{tail_}, inter_{tail_}
  {
  }

  GarbageList(const GarbageList &) = delete;
  GarbageList &operator=(const GarbageList &) = delete;
  GarbageList(GarbageList &&) = delete;
  GarbageList &operator=(GarbageList &&) = delete;

  /*####################################################################################
   * Public destructor
   *##################################################################################*/

  /**
   * @brief Destroy the GarbageList object.
   *
   */
  ~GarbageList()
  {
    auto current = head_.load(std::memory_order_relaxed);
    while (current != nullptr) {
      auto previous = current;
      current = current->GetNext();
      delete previous;
    }
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
   * @param garbage_ptr a pointer to a target garbage.
   */
  void
  AddGarbage(T *garbage_ptr)
  {
    tail_ = GarbageBuffer::AddGarbage(tail_, garbage_ptr);
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
   * @brief Destruct unprotected garbages for reusing.
   *
   * @param protected_epoch an epoch value to check epoch protection.
   */
  void
  DestructGarbages(const size_t protected_epoch)
  {
    auto inter = GarbageBuffer::Destruct(inter_.load(std::memory_order_relaxed), protected_epoch);
    inter_.store(inter, std::memory_order_relaxed);
  }

  /**
   * @brief Release unprotected garbages.
   *
   * @param protected_epoch an epoch value to check epoch protection.
   */
  void
  ClearGarbages(const size_t protected_epoch)
  {
    auto head = GarbageBuffer::Clear(head_.load(std::memory_order_relaxed), protected_epoch);
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
     * @param global_epoch a reference to the global epoch.
     */
    constexpr explicit GarbageBuffer(const std::atomic_size_t &global_epoch)
        : global_epoch_{global_epoch}
    {
    }

    GarbageBuffer(const GarbageBuffer &) = delete;
    GarbageBuffer &operator=(const GarbageBuffer &) = delete;
    GarbageBuffer(GarbageBuffer &&) = delete;
    GarbageBuffer &operator=(GarbageBuffer &&) = delete;

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

      // release unprotected garbages
      auto idx = begin_idx_.load(std::memory_order_relaxed);
      for (; idx < destructed_idx; ++idx) {
        // the garbage has been already destructed
        operator delete(garbages_[idx].ptr);
      }
      for (; idx < end_idx; ++idx) {
        delete garbages_[idx].ptr;
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
      const auto size = end_idx_.load(std::memory_order_relaxed)
                        - destructed_idx_.load(std::memory_order_relaxed);
      const auto next = next_.load(std::memory_order_relaxed);
      if (next == nullptr) {
        return size;
      }
      return next->Size() + size;
    }

    /**
     * @retval true if this list is empty.
     * @retval false otherwise
     */
    [[nodiscard]] auto
    Empty() const  //
        -> bool
    {
      const auto size = end_idx_.load(std::memory_order_relaxed)
                        - destructed_idx_.load(std::memory_order_relaxed);
      if (size > 0) return false;

      const auto next = next_.load(std::memory_order_relaxed);
      if (next == nullptr) return true;
      return next->Empty();
    }

    /**
     * @return a pointer to the next garbage buffer.
     */
    [[nodiscard]] auto
    GetNext() const  //
        -> GarbageBuffer *
    {
      return next_.load(std::memory_order_relaxed);
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
     * @param garbage a new garbage instance.
     * @return a pointer to a current tail garbage buffer.
     */
    static auto
    AddGarbage(  //
        GarbageBuffer *buffer,
        T *garbage)  //
        -> GarbageBuffer *
    {
      const auto end_idx = buffer->end_idx_.load(std::memory_order_relaxed);
      const auto epoch = buffer->global_epoch_.load(std::memory_order_relaxed);

      // insert a new garbage
      buffer->garbages_[end_idx].epoch = epoch;
      buffer->garbages_[end_idx].ptr = garbage;

      // increment the end position
      buffer->end_idx_.fetch_add(1, std::memory_order_release);

      // check whether the list is full
      if (end_idx >= kGarbageBufferSize - 1) {
        auto full_list = buffer;
        buffer = new GarbageBuffer{buffer->global_epoch_};
        full_list->next_.store(buffer, std::memory_order_relaxed);
      }

      return buffer;
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

      // check whether there are released garbages
      if (idx >= destructed_idx) return {nullptr, buffer};

      // get a released page
      buffer->begin_idx_.fetch_add(1, std::memory_order_relaxed);
      auto *page = buffer->garbages_[idx].ptr;

      // check whether all the pages in the list are reused
      if (idx >= kGarbageBufferSize - 1) {
        // the list has become empty, so delete it
        auto empty_buf = buffer;
        do {  // if the garbage buffer is empty but does not have a next buffer, wait insertion
          buffer = empty_buf->GetNext();
        } while (buffer == nullptr);

        while (empty_buf->cleaner_ref_this_.load(std::memory_order_relaxed)) {
          // wait for a cleaner thread to leave this list
        }
        delete empty_buf;
      }

      return {page, buffer};
    }

    /**
     * @brief Destruct garbages where their epoch is less than a protected one.
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
      // set a flag as single-counter based GC
      buffer->cleaner_ref_this_.store(true, std::memory_order_relaxed);

      // release unprotected garbages
      const auto end_idx = buffer->end_idx_.load(std::memory_order_acquire);
      auto idx = buffer->destructed_idx_.load(std::memory_order_relaxed);
      for (; idx < end_idx; ++idx) {
        if (buffer->garbages_[idx].epoch >= protected_epoch) break;

        // only call destructor to reuse pages
        buffer->garbages_[idx].ptr->~T();
      }
      buffer->destructed_idx_.store(idx, std::memory_order_release);

      if (idx < kGarbageBufferSize) {
        // the buffer has unreleased garbages
        buffer->cleaner_ref_this_.store(false, std::memory_order_relaxed);
        return buffer;
      }

      // release the next buffer recursively
      auto *next = buffer->GetNext();
      while (next == nullptr) {  // if the list does not have a next buffer, wait insertion
        next = buffer->GetNext();
      }

      buffer->cleaner_ref_this_.store(false, std::memory_order_relaxed);
      return GarbageBuffer::Destruct(next, protected_epoch);
    }

    /**
     * @brief Release garbages where their epoch is less than a protected one.
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

      // release unprotected garbages
      auto idx = buffer->begin_idx_.load(std::memory_order_relaxed);
      for (; idx < destructed_idx; ++idx) {
        // the garbage has been already destructed
        operator delete(buffer->garbages_[idx].ptr);
      }
      for (; idx < end_idx; ++idx) {
        if (buffer->garbages_[idx].epoch >= protected_epoch) break;

        delete buffer->garbages_[idx].ptr;
      }
      buffer->begin_idx_.store(idx, std::memory_order_relaxed);
      buffer->destructed_idx_.store(idx, std::memory_order_relaxed);

      if (idx < kGarbageBufferSize) {
        // the buffer has unreleased garbages
        return buffer;
      }

      // release the next buffer recursively
      auto *next = buffer->GetNext();
      while (next == nullptr) {  // if the list does not have a next buffer, wait insertion
        next = buffer->GetNext();
      }
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
    Garbage garbages_[kGarbageBufferSize];

    /// the index to represent a head position.
    std::atomic_size_t begin_idx_{0};

    /// the end of released indexes
    std::atomic_size_t destructed_idx_{0};

    /// the index to represent a tail position.
    std::atomic_size_t end_idx_{0};

    /// a current epoch. Note: this is maintained individually to improve performance.
    const std::atomic_size_t &global_epoch_;

    /// a flag to indicate a cleaner thread is modifying the list
    std::atomic_bool cleaner_ref_this_{false};

    /// a pointer to a next garbage buffer.
    std::atomic<GarbageBuffer *> next_{nullptr};
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
