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

#ifndef MEMORY_MANAGER_MEMORY_COMPONENT_GARBAGE_LIST_H_
#define MEMORY_MANAGER_MEMORY_COMPONENT_GARBAGE_LIST_H_

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
  constexpr GarbageList() : head_{nullptr}, reuse_{nullptr}, tail_{nullptr} {}

  /*##############################################################################################
   * Public destructor
   *############################################################################################*/

  /**
   * @brief Destroy the GarbageList object.
   *
   */
  ~GarbageList()
  {
    auto current = head_;
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
  bool
  Empty() const
  {
    return head_->Empty();
  }

  /**
   * @return the number of unreleased garbases in the list.
   */
  size_t
  Size() const
  {
    return head_->Size();
  }

  /*##############################################################################################
   * Public utility functions
   *############################################################################################*/

  /**
   * @brief Set a reference to a global epoch.
   *
   * Note that this function expires the old internal list and creates a new list.
   *
   * @param global_epoch a reference to a global epoch.
   */
  void
  SetEpoch(const std::atomic_size_t& global_epoch)
  {
    delete head_;

    head_ = new GarbageBuffer{global_epoch};
    reuse_ = head_;
    tail_ = head_;
  }

  /**
   * @brief Add a new garbage instance.
   *
   * @param garbage_ptr a pointer to a target garbage.
   */
  void
  AddGarbage(const T* garbage_ptr)
  {
    tail_ = GarbageBuffer::AddGarbage(tail_, garbage_ptr);
  }

  /**
   * @brief Reuse a released memory page if it exists in the list.
   *
   * @retval nullptr if the list does not have reusable pages.
   * @retval a memory page.
   */
  void*
  GetPageIfPossible()
  {
    void* page;
    std::tie(page, reuse_) = GarbageBuffer::ReusePage(reuse_);

    return page;
  }

  /**
   * @brief Release unprotected garbages.
   *
   * @param protected_epoch an epoch value to check epoch protection.
   */
  void
  ClearGarbages(const size_t protected_epoch)
  {
    head_ = GarbageBuffer::Clear(head_, protected_epoch);
  }

 private:
  /*################################################################################################
   * Internal member variables
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
        : begin_idx_{0}, end_idx_{0}, released_idx_{0}, current_epoch_{global_epoch}, next_{nullptr}
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
      // if the list has garbages, release them before deleting oneself
      const auto released_idx = released_idx_.load(kMORelax);
      const auto end_idx = end_idx_.load(kMORelax);
      for (size_t i = begin_idx_; i < released_idx; ++i) {
        operator delete(garbages_[i].GetGarbage());
      }
      for (size_t i = released_idx; i < end_idx; ++i) {
        delete garbages_[i].GetGarbage();
      }
    }

    /*##############################################################################################
     * Public getters/setters
     *############################################################################################*/

    /**
     * @return the number of unreleased garbases in entire lists.
     */
    size_t
    Size() const
    {
      const auto size = end_idx_.load(kMORelax) - released_idx_.load(kMORelax);
      const auto next = next_.load(kMORelax);
      if (next == nullptr) {
        return size;
      } else {
        return next->Size() + size;
      }
    }

    /**
     * @retval true if this list is empty.
     * @retval false otherwise
     */
    bool
    Empty() const
    {
      if (end_idx_.load(kMORelax) - released_idx_.load(kMORelax) > 0) return false;

      const auto next = next_.load(kMORelax);
      if (next == nullptr) return true;
      return next->Empty();
    }

    /**
     * @return a pointer to the next garbage buffer.
     */
    GarbageBuffer*
    GetNext() const
    {
      return next_.load(kMORelax);
    }

    /*##############################################################################################
     * Public utility functions
     *############################################################################################*/

    /**
     * @brief Add a new garbage instance to a specified buffer.
     *
     * If the buffer becomes full, create a new garbage buffer and link them.
     *
     * @param garbage_buf a garbage buffer to be added.
     * @param garbage a new garbage instance.
     * @return a pointer to a current tail garbage buffer.
     */
    static GarbageBuffer*
    AddGarbage(  //
        GarbageBuffer* garbage_buf,
        const T* garbage)
    {
      const auto end_idx = garbage_buf->end_idx_.load(kMORelax);
      const auto current_epoch = garbage_buf->current_epoch_.load(kMORelax);

      // insert a new garbage
      garbage_buf->garbages_[end_idx].SetEpoch(current_epoch);
      garbage_buf->garbages_[end_idx].SetGarbage(garbage);
      garbage_buf->end_idx_.fetch_add(1, kMORelax);

      // check whether the list is full
      if (end_idx >= kGarbageBufferSize - 1) {
        auto full_list = garbage_buf;
        garbage_buf = new GarbageBuffer{garbage_buf->current_epoch_};
        full_list->next_.store(garbage_buf, kMORelax);
      }

      return garbage_buf;
    }

    /**
     * @brief Reuse a released memory page.
     *
     * @param garbage_buf a garbage buffer to be reused.
     * @return the pair of a memory page and the current head buffer.
     */
    static std::pair<void*, GarbageBuffer*>
    ReusePage(GarbageBuffer* garbage_buf)
    {
      const auto released_idx = garbage_buf->released_idx_.load(kMORelax);
      auto cur_idx = garbage_buf->begin_idx_;

      // check whether there are released garbages
      if (cur_idx >= released_idx) return {nullptr, garbage_buf};

      // get a released page
      void* page = garbage_buf->garbages_[cur_idx].GetGarbage();
      garbage_buf->begin_idx_ = ++cur_idx;
      if (cur_idx >= kGarbageBufferSize) {
        // the list has become empty, so delete it
        auto empty_buf = garbage_buf;
        do {  // if the garbage buffer is empty but does not have a next buffer, wait insertion
          garbage_buf = empty_buf->next_.load(kMORelax);
        } while (garbage_buf == nullptr);
        delete empty_buf;
      }

      return {page, garbage_buf};
    }

    /**
     * @brief Clear garbages where their epoch is less than a protected one.
     *
     * @param garbage_buf a target barbage buffer.
     * @param protected_epoch a protected epoch.
     * @return GarbageBuffer* a head of garbage buffers.
     */
    static GarbageBuffer*
    Clear(  //
        GarbageBuffer* garbage_buf,
        const size_t protected_epoch)
    {
      // release unprotected garbages
      const auto end_idx = garbage_buf->end_idx_.load(kMORelax);
      auto idx = garbage_buf->released_idx_.load(kMORelax);
      for (; idx < end_idx; ++idx) {
        if (garbage_buf->garbages_[idx].GetEpoch() >= protected_epoch) break;

        // only call destructor to reuse pages
        garbage_buf->garbages_[idx].GetGarbage()->~T();
      }
      garbage_buf->released_idx_.store(idx, kMORelax);

      // check whether there is space in this list
      if (idx < kGarbageBufferSize) return garbage_buf;

      // release the next list recursively
      GarbageBuffer* next;
      do {  // if the garbage buffer is empty but does not have a next buffer, wait insertion
        next = garbage_buf->next_.load(kMORelax);
      } while (next == nullptr);
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
      constexpr Garbage() : epoch_{}, ptr_{} {}

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
        epoch_.store(epoch, kMORelax);
      }

      /**
       * @param ptr a pointer to a garbage to be registered.
       */
      void
      SetGarbage(const T* ptr)
      {
        ptr_.store(const_cast<T*>(ptr), kMORelax);
      }

      /**
       * @return the epoch value when the garbage is registered.
       */
      size_t
      GetEpoch() const
      {
        return epoch_.load(kMORelax);
      }

      /**
       * @return the pointer to the registered garbage.
       */
      T*
      GetGarbage() const
      {
        return ptr_.load(kMORelax);
      }

     private:
      /*############################################################################################
       * Internal member variables
       *##########################################################################################*/

      /// an epoch value when the garbage is registered.
      std::atomic_size_t epoch_;

      /// a pointer to the registered garbage.
      std::atomic<T*> ptr_;
    };

    /*##############################################################################################
     * Internal member variables
     *############################################################################################*/

    /// a buffer of garbage instances with added epochs.
    std::array<Garbage, kGarbageBufferSize> garbages_;

    /// the index to represent a head position.
    size_t begin_idx_;

    /// the index to represent a tail position.
    std::atomic_size_t end_idx_;

    /// the end of released indexes
    std::atomic_size_t released_idx_;

    /// a current epoch. Note: this is maintained individually to improve performance.
    const std::atomic_size_t& current_epoch_;

    /// a pointer to a next garbage buffer.
    std::atomic<GarbageBuffer*> next_;
  };

  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  /// a pointer to a target garbage list.
  GarbageBuffer* head_;

  /// a pointer to a target garbage list.
  GarbageBuffer* reuse_;

  /// a pointer to a target garbage list.
  GarbageBuffer* tail_;
};

}  // namespace dbgroup::memory::component

#endif  // MEMORY_MANAGER_MEMORY_COMPONENT_GARBAGE_LIST_H_
