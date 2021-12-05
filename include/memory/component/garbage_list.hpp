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
  /*##############################################################################################
   * Public constructors and assignment operators
   *############################################################################################*/

  /**
   * @brief Create a new GarbageList object.
   *
   */
  explicit GarbageList(const std::atomic_size_t& global_epoch)
      : tail_{new GarbageBuffer{global_epoch}}
  {
    head_.store(tail_, std::memory_order_release);
    inter_.store(tail_, std::memory_order_release);
  }

  GarbageList(const GarbageList&) = delete;
  GarbageList& operator=(const GarbageList&) = delete;
  GarbageList(GarbageList&&) = delete;
  GarbageList& operator=(GarbageList&&) = delete;

  /*##############################################################################################
   * Public destructor
   *############################################################################################*/

  /**
   * @brief Destroy the GarbageList object.
   *
   */
  ~GarbageList()
  {
    auto current = head_.load(std::memory_order_acquire);
    while (current != nullptr) {
      auto previous = current;
      current = current->GetNext();
      delete previous;
    }
  }

  /*##############################################################################################
   * Public getters/setters
   *############################################################################################*/

  /**
   * @retval true if the list is empty.
   * @retval false otherwise.
   */
  [[nodiscard]] auto
  Empty() const  //
      -> bool
  {
    return head_.load(std::memory_order_acquire)->Empty();
  }

  /**
   * @return the number of non-destructed garbases in the list.
   */
  [[nodiscard]] auto
  Size() const  //
      -> size_t
  {
    return inter_.load(std::memory_order_acquire)->Size();
  }

  /*##############################################################################################
   * Public utility functions
   *############################################################################################*/

  /**
   * @brief Add a new garbage instance.
   *
   * @param garbage_ptr a pointer to a target garbage.
   */
  void
  AddGarbage(T* garbage_ptr)
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
      -> void*
  {
    auto [page, head] = GarbageBuffer::ReusePage(head_.load(std::memory_order_acquire));
    head_.store(head, std::memory_order_release);

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
    auto inter = GarbageBuffer::Destruct(inter_.load(std::memory_order_acquire), protected_epoch);
    inter_.store(inter, std::memory_order_release);
  }

  /**
   * @brief Release unprotected garbages.
   *
   * @param protected_epoch an epoch value to check epoch protection.
   */
  void
  ClearGarbages(const size_t protected_epoch)
  {
    auto head = GarbageBuffer::Clear(head_.load(std::memory_order_acquire), protected_epoch);
    head_.store(head, std::memory_order_release);
  }

 private:
  /*################################################################################################
   * Internal classes
   *##############################################################################################*/

  /**
   * @brief A class to represent internal buffers for garbage lists.
   *
   */
  class GarbageBuffer
  {
   public:
    /*##############################################################################################
     * Public constructors and assignment operators
     *############################################################################################*/

    /**
     * @brief Construct a new instance.
     *
     * @param global_epoch a reference to the global epoch.
     */
    constexpr explicit GarbageBuffer(const std::atomic_size_t& global_epoch)
        : current_epoch_{global_epoch}
    {
    }

    GarbageBuffer(const GarbageBuffer&) = delete;
    GarbageBuffer& operator=(const GarbageBuffer&) = delete;
    GarbageBuffer(GarbageBuffer&&) = delete;
    GarbageBuffer& operator=(GarbageBuffer&&) = delete;

    /*##############################################################################################
     * Public destructors
     *############################################################################################*/

    /**
     * @brief Destroy the instance.
     *
     */
    ~GarbageBuffer()
    {
      const auto destructed_idx = GetDestructedIdx();
      const auto end_idx = GetEndIdx();

      // release unprotected garbages
      auto idx = GetBeginIdx();
      for (; idx < destructed_idx; ++idx) {
        // the garbage has been already destructed
        operator delete(garbages_[idx].GetGarbage());
      }
      for (; idx < end_idx; ++idx) {
        delete garbages_[idx].GetGarbage();
      }
    }

    /*##############################################################################################
     * Public getters/setters
     *############################################################################################*/

    /**
     * @return the number of unreleased garbases in entire lists.
     */
    [[nodiscard]] auto
    Size() const  //
        -> size_t
    {
      const auto size = GetEndIdx() - GetDestructedIdx();
      const auto next = GetNext();
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
      if (GetEndIdx() - GetBeginIdx() > 0) return false;

      const auto next = GetNext();
      if (next == nullptr) return true;
      return next->Empty();
    }

    /**
     * @return a pointer to the next garbage buffer.
     */
    [[nodiscard]] auto
    GetNext() const  //
        -> GarbageBuffer*
    {
      return next_.load(std::memory_order_acquire);
    }

    /*##############################################################################################
     * Public utility functions
     *############################################################################################*/

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
        GarbageBuffer* buffer,
        T* garbage)  //
        -> GarbageBuffer*
    {
      const auto end_idx = buffer->GetEndIdx();
      const auto epoch = buffer->current_epoch_.load(std::memory_order_acquire);

      // insert a new garbage
      buffer->garbages_[end_idx].SetEpoch(epoch);
      buffer->garbages_[end_idx].SetGarbage(garbage);

      // increment the end position
      buffer->end_idx_.fetch_add(1, std::memory_order_acq_rel);

      // check whether the list is full
      if (end_idx >= kGarbageBufferSize - 1) {
        auto full_list = buffer;
        buffer = new GarbageBuffer{buffer->current_epoch_};
        full_list->next_.store(buffer, std::memory_order_release);
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
    ReusePage(GarbageBuffer* buffer)  //
        -> std::pair<void*, GarbageBuffer*>
    {
      const auto idx = buffer->GetBeginIdx();
      const auto destructed_idx = buffer->GetDestructedIdx();

      // check whether there are released garbages
      if (idx >= destructed_idx) return {nullptr, buffer};

      // get a released page
      buffer->begin_idx_.fetch_add(1, std::memory_order_acq_rel);
      auto* page = buffer->garbages_[idx].GetGarbage();

      // check whether all the pages in the list are reused
      if (idx >= kGarbageBufferSize - 1) {
        // the list has become empty, so delete it
        auto empty_buf = buffer;
        do {  // if the garbage buffer is empty but does not have a next buffer, wait insertion
          buffer = empty_buf->GetNext();
        } while (buffer == nullptr);

        while (empty_buf->cleaner_ref_this_.load(std::memory_order_acquire)) {
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
        GarbageBuffer* buffer,
        const size_t protected_epoch)  //
        -> GarbageBuffer*
    {
      // set a flag as single-counter based GC
      buffer->cleaner_ref_this_.store(true, std::memory_order_release);

      // release unprotected garbages
      const auto end_idx = buffer->GetEndIdx();
      auto idx = buffer->GetDestructedIdx();
      for (; idx < end_idx; ++idx) {
        if (buffer->garbages_[idx].GetEpoch() >= protected_epoch) break;

        // only call destructor to reuse pages
        buffer->garbages_[idx].GetGarbage()->~T();
      }
      buffer->destructed_idx_.store(idx, std::memory_order_release);

      if (idx < kGarbageBufferSize) {
        // the buffer has unreleased garbages
        buffer->cleaner_ref_this_.store(false, std::memory_order_release);
        return buffer;
      }

      // release the next buffer recursively
      auto* next = buffer->GetNext();
      while (next == nullptr) {  // if the list does not have a next buffer, wait insertion
        next = buffer->GetNext();
      }

      buffer->cleaner_ref_this_.store(false, std::memory_order_release);
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
        GarbageBuffer* buffer,
        const size_t protected_epoch)  //
        -> GarbageBuffer*
    {
      const auto destructed_idx = buffer->GetDestructedIdx();
      const auto end_idx = buffer->GetEndIdx();

      // release unprotected garbages
      auto idx = buffer->GetBeginIdx();
      for (; idx < destructed_idx; ++idx) {
        // the garbage has been already destructed
        operator delete(buffer->garbages_[idx].GetGarbage());
      }
      for (; idx < end_idx; ++idx) {
        if (buffer->garbages_[idx].GetEpoch() >= protected_epoch) break;

        delete buffer->garbages_[idx].GetGarbage();
      }
      buffer->begin_idx_.store(idx, std::memory_order_release);

      if (idx < kGarbageBufferSize) {
        // the buffer has unreleased garbages
        return buffer;
      }

      // release the next buffer recursively
      auto* next = buffer->GetNext();
      while (next == nullptr) {  // if the list does not have a next buffer, wait insertion
        next = buffer->GetNext();
      }
      delete buffer;

      return GarbageBuffer::Clear(next, protected_epoch);
    }

   private:
    /*##############################################################################################
     * Internal classes
     *############################################################################################*/

    /**
     * @brief A class to represent the pair of an epoch value and a registered garbage.
     *
     */
    class Garbage
    {
     public:
      /*############################################################################################
       * Public constructors and assignment operators
       *##########################################################################################*/

      /**
       * @brief Create a new garbage object.
       *
       */
      constexpr Garbage() = default;

      Garbage(const Garbage&) = delete;
      Garbage& operator=(const Garbage&) = delete;
      Garbage(Garbage&&) = delete;
      Garbage& operator=(Garbage&&) = delete;

      /*############################################################################################
       * Public destructors
       *##########################################################################################*/

      /**
       * @brief Destroy the Garbage object.
       *
       */
      ~Garbage() = default;

      /*############################################################################################
       * Public getters/setters
       *##########################################################################################*/

      /**
       * @param epoch an epoch value to be set.
       */
      void
      SetEpoch(const size_t epoch)
      {
        epoch_.store(epoch, std::memory_order_release);
      }

      /**
       * @param ptr a pointer to a garbage to be registered.
       */
      void
      SetGarbage(T* ptr)
      {
        ptr_.store(ptr, std::memory_order_release);
      }

      /**
       * @return the epoch value when the garbage is registered.
       */
      [[nodiscard]] auto
      GetEpoch() const  //
          -> size_t
      {
        return epoch_.load(std::memory_order_acquire);
      }

      /**
       * @return the pointer to the registered garbage.
       */
      [[nodiscard]] auto
      GetGarbage() const  //
          -> T*
      {
        return ptr_.load(std::memory_order_acquire);
      }

     private:
      /*############################################################################################
       * Internal member variables
       *##########################################################################################*/

      /// an epoch value when the garbage is registered.
      std::atomic_size_t epoch_{};

      /// a pointer to the registered garbage.
      std::atomic<T*> ptr_{};
    };

    /*##############################################################################################
     * Internal setters/getters
     *############################################################################################*/

    /**
     * @return the begin index of this buffer.
     *
     * Note that the value is read with std::memory_order_acquire.
     */
    [[nodiscard]] auto
    GetBeginIdx() const  //
        -> size_t
    {
      return begin_idx_.load(std::memory_order_acquire);
    }

    /**
     * @return an index that has an available page.
     *
     * Note that the value is read with std::memory_order_acquire.
     */
    [[nodiscard]] auto
    GetDestructedIdx() const  //
        -> size_t
    {
      return destructed_idx_.load(std::memory_order_acquire);
    }

    /**
     * @return the end index of this buffer.
     *
     * Note that the value is read with std::memory_order_acquire.
     */
    [[nodiscard]] auto
    GetEndIdx() const  //
        -> size_t
    {
      return end_idx_.load(std::memory_order_acquire);
    }

    /*##############################################################################################
     * Internal member variables
     *############################################################################################*/

    /// a buffer of garbage instances with added epochs.
    Garbage garbages_[kGarbageBufferSize];

    /// the index to represent a head position.
    std::atomic_size_t begin_idx_{0};

    /// the end of released indexes
    std::atomic_size_t destructed_idx_{0};

    /// the index to represent a tail position.
    std::atomic_size_t end_idx_{0};

    /// a current epoch. Note: this is maintained individually to improve performance.
    const std::atomic_size_t& current_epoch_;

    /// a flag to indicate a cleaner thread is modifying the list
    std::atomic_bool cleaner_ref_this_{false};

    /// a pointer to a next garbage buffer.
    std::atomic<GarbageBuffer*> next_{nullptr};
  };

  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  /// a pointer to the head of a target garbage list.
  std::atomic<GarbageBuffer*> head_{};

  /// a pointer to the internal node of a target garbage list.
  std::atomic<GarbageBuffer*> inter_{};

  /// a pointer to the tail a target garbage list.
  GarbageBuffer* tail_{nullptr};
};

}  // namespace dbgroup::memory::component

#endif  // MEMORY_COMPONENT_GARBAGE_LIST_HPP
