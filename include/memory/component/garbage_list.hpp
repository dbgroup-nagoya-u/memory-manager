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

// C++ standard libraries
#include <array>
#include <atomic>
#include <limits>
#include <memory>
#include <mutex>
#include <tuple>
#include <utility>

// external sources
#include "thread/id_manager.hpp"

// local sources
#include "memory/component/common.hpp"

namespace dbgroup::memory::component
{
/**
 * @brief A class for representing garbage lists.
 *
 * @tparam Target a target class of garbage collection.
 */
template <class Target>
class alignas(kCashLineSize) GarbageList
{
 public:
  /*####################################################################################
   * Public global constants
   *##################################################################################*/

  /// The size of buffers for retaining garbages.
  static constexpr size_t kBufferSize = (kVMPageSize - 4 * kWordSize) / (2 * kWordSize);

  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using T = typename Target::T;
  using IDManager = ::dbgroup::thread::IDManager;

  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  /**
   * @brief Construct a new GarbageList object.
   *
   */
  GarbageList() = default;

  GarbageList(const GarbageList &) = delete;
  GarbageList(GarbageList &&) = delete;

  auto operator=(const GarbageList &) -> GarbageList & = delete;
  auto operator=(GarbageList &&) -> GarbageList & = delete;

  /*####################################################################################
   * Public destructors
   *##################################################################################*/

  /**
   * @brief Destroy the GarbageList object.
   *
   * If the list contains unreleased garbage, the destructor will forcibly release it.
   */
  ~GarbageList()
  {
    auto *head = Target::kReusePages ? head_.load(std::memory_order_relaxed) : mid_;
    if (head != nullptr) {
      GarbageBuffer::Clear(&head, std::numeric_limits<size_t>::max());
      delete head;
    }
  }

  /*####################################################################################
   * Public utility functions for worker threads
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
    AssignCurrentThreadIfNeeded();
    GarbageBuffer::AddGarbage(&tail_, epoch, garbage_ptr);
  }

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
    AssignCurrentThreadIfNeeded();
    return GarbageBuffer::ReusePage(&head_);
  }

  /*####################################################################################
   * Public utility functions for GC threads
   *##################################################################################*/

  /**
   * @brief Release registered garbage if possible.
   *
   * @param protected_epoch an epoch value to check whether garbage can be freed.
   */
  void
  ClearGarbage(const size_t protected_epoch)
  {
    std::unique_lock guard{mtx_, std::defer_lock};
    if (!guard.try_lock() || mid_ == nullptr) return;

    // destruct or release garbages
    if constexpr (!Target::kReusePages) {
      GarbageBuffer::Clear(&mid_, protected_epoch);
    } else {
      if (!heartbeat_.expired()) {
        GarbageBuffer::Destruct(&mid_, protected_epoch);
        return;
      }

      auto *head = head_.load(std::memory_order_relaxed);
      if (head != nullptr) {
        mid_ = head;
      }
      GarbageBuffer::Clear(&mid_, protected_epoch);
      head_.store(mid_, std::memory_order_relaxed);
    }

    // check this list is alive
    if (!heartbeat_.expired() || !mid_->Empty()) return;

    // release buffers if the thread has exitted
    delete mid_;
    head_.store(nullptr, std::memory_order_relaxed);
    mid_ = nullptr;
    tail_ = nullptr;
  }

 private:
  /*####################################################################################
   * Internal classes
   *##################################################################################*/

  /**
   * @brief A class to represent a buffer of garbage instances.
   *
   */
  class alignas(kVMPageSize) GarbageBuffer
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
    ~GarbageBuffer() = default;

    /*##################################################################################
     * Public getters/setters
     *################################################################################*/

    /**
     * @retval true if this list is empty.
     * @retval false otherwise
     */
    [[nodiscard]] auto
    Empty() const  //
        -> bool
    {
      const auto end_pos = end_pos_.load(std::memory_order_acquire);
      const auto size = end_pos - begin_pos_.load(std::memory_order_relaxed);

      return (size == 0) && (end_pos < kBufferSize);
    }

    /*##################################################################################
     * Public utility functions
     *################################################################################*/

    /**
     * @brief Add a new garbage instance to a specified buffer.
     *
     * If the buffer becomes full, create a new garbage buffer and link them.
     *
     * @param buf_addr the address of the pointer of a target buffer.
     * @param epoch an epoch value when a garbage is added.
     * @param garbage a new garbage instance.
     */
    static void
    AddGarbage(  //
        GarbageBuffer **buf_addr,
        const size_t epoch,
        T *garbage)
    {
      auto *buf = *buf_addr;
      const auto pos = buf->end_pos_.load(std::memory_order_relaxed);

      // insert a new garbage
      buf->garbage_.at(pos).epoch = epoch;
      buf->garbage_.at(pos).ptr = garbage;

      // check whether the list is full
      if (pos >= kBufferSize - 1) {
        auto *new_tail = new GarbageBuffer{};
        buf->next_ = new_tail;
        *buf_addr = new_tail;
      }

      // increment the end position
      buf->end_pos_.fetch_add(1, std::memory_order_release);
    }

    /**
     * @brief Reuse a released memory page.
     *
     * @param buf_addr the address of the pointer of a target buffer.
     * @retval a memory page if exist.
     * @retval nullptr otherwise.
     */
    static auto
    ReusePage(std::atomic<GarbageBuffer *> *buf_addr)  //
        -> void *
    {
      auto *buf = buf_addr->load(std::memory_order_relaxed);
      const auto pos = buf->begin_pos_.load(std::memory_order_relaxed);
      const auto mid_pos = buf->mid_pos_.load(std::memory_order_acquire);

      // check whether there are released garbage
      if (pos >= mid_pos) return nullptr;

      // get a released page
      buf->begin_pos_.fetch_add(1, std::memory_order_relaxed);
      auto *page = buf->garbage_.at(pos).ptr;

      // check whether all the pages in the list are reused
      if (pos >= kBufferSize - 1) {
        buf_addr->store(buf->next_, std::memory_order_relaxed);
        delete buf;
      }

      return page;
    }

    /**
     * @brief Destruct garbage where their epoch is less than a protected one.
     *
     * @param buf_addr the address of the pointer of a target buffer.
     * @param protected_epoch a protected epoch.
     */
    static void
    Destruct(  //
        GarbageBuffer **buf_addr,
        const size_t protected_epoch)
    {
      while (true) {
        // release unprotected garbage
        auto *buf = *buf_addr;
        const auto end_pos = buf->end_pos_.load(std::memory_order_acquire);
        auto pos = buf->mid_pos_.load(std::memory_order_relaxed);
        for (; pos < end_pos; ++pos) {
          if (buf->garbage_.at(pos).epoch >= protected_epoch) break;

          // only call destructor to reuse pages
          if constexpr (!std::is_same_v<T, void>) {
            buf->garbage_.at(pos).ptr->~T();
          }
        }

        // update the position to make visible destructed garbage
        buf->mid_pos_.store(pos, std::memory_order_release);
        if (pos < kBufferSize) return;

        // release the next buffer recursively
        *buf_addr = buf->next_;
      }
    }

    /**
     * @brief Release garbage where their epoch is less than a protected one.
     *
     * @param buf_addr the address of the pointer of a target buffer.
     * @param protected_epoch a protected epoch.
     */
    static void
    Clear(  //
        GarbageBuffer **buf_addr,
        const size_t protected_epoch)
    {
      while (true) {
        auto *buf = *buf_addr;
        const auto mid_pos = buf->mid_pos_.load(std::memory_order_relaxed);
        const auto end_pos = buf->end_pos_.load(std::memory_order_acquire);

        // release unprotected garbage
        auto pos = buf->begin_pos_.load(std::memory_order_relaxed);
        for (; pos < mid_pos; ++pos) {
          // the garbage has been already destructed
          Target::deleter(buf->garbage_.at(pos).ptr);
        }
        for (; pos < end_pos; ++pos) {
          if (buf->garbage_.at(pos).epoch >= protected_epoch) break;

          auto *ptr = buf->garbage_.at(pos).ptr;
          if constexpr (!std::is_same_v<T, void>) {
            ptr->~T();
          }
          Target::deleter(ptr);
        }
        buf->begin_pos_.store(pos, std::memory_order_relaxed);
        buf->mid_pos_.store(pos, std::memory_order_relaxed);

        if (pos < kBufferSize) return;

        // release the next buffer recursively
        *buf_addr = buf->next_;
        delete buf;
      }
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
      /// An epoch value when the garbage is registered.
      size_t epoch{};

      /// A pointer to the registered garbage.
      T *ptr{};
    };

    /*##################################################################################
     * Internal member variables
     *################################################################################*/

    /// The index to represent a head position.
    std::atomic_size_t begin_pos_{0};

    /// The end of released indexes
    std::atomic_size_t mid_pos_{0};

    /// A buffer of garbage instances with added epochs.
    std::array<Garbage, kBufferSize> garbage_{};

    /// The index to represent a tail position.
    std::atomic_size_t end_pos_{0};

    /// A pointer to a next garbage buffer.
    GarbageBuffer *next_{nullptr};
  };

  /*####################################################################################
   * Internal utilities
   *##################################################################################*/

  /**
   * @brief Assign this list to the current thread.
   *
   */
  void
  AssignCurrentThreadIfNeeded()
  {
    if (!heartbeat_.expired()) return;

    std::lock_guard guard{mtx_};
    if (tail_ == nullptr) {
      tail_ = new GarbageBuffer{};
      mid_ = tail_;
      if constexpr (Target::kReusePages) {
        head_.store(tail_, std::memory_order_relaxed);
      }
    }
    heartbeat_ = IDManager::GetHeartBeat();
  }

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// A flag for indicating the corresponding thread has exited.
  std::weak_ptr<size_t> heartbeat_{};

  /// A garbage list that has destructed pages.
  std::atomic<GarbageBuffer *> head_{nullptr};

  /// A garbage list that has free space for new garbages.
  GarbageBuffer *tail_{nullptr};

  /// A dummy array for cache line alignments
  uint64_t padding_[4]{};

  /// A mutex instance for modifying buffer pointers.
  std::mutex mtx_{};

  /// A garbage list that has not destructed pages.
  GarbageBuffer *mid_{nullptr};
};

}  // namespace dbgroup::memory::component

#endif  // MEMORY_COMPONENT_GARBAGE_LIST_HPP
