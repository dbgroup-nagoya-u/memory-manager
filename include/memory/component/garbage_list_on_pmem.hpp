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
#include <memory>
#include <mutex>
#include <stdexcept>
#include <tuple>
#include <utility>

// external system libraries
#include <libpmem.h>
#include <libpmemobj.h>

// external sources
#include "thread/id_manager.hpp"

// local sources
#include "common.hpp"

namespace dbgroup::memory::component
{
/*######################################################################################
 * Global utility functions
 *####################################################################################*/

/**
 * @brief Allocate a region of persistent memory by using a given pool.
 *
 * Internally, this function uses pmemobj_zalloc, and so an allocated region is filled
 * with zeros.
 *
 * @param pop the pointer to a pmemobj pool instance.
 * @param oid the address of a PMEMoid instance to store an allocated region.
 * @param size the desired size of allocation.
 */
inline void
AllocatePmem(  //
    PMEMobjpool *pop,
    PMEMoid *oid,
    const size_t size)
{
  const auto rc = pmemobj_zalloc(pop, oid, size, kDefaultPMDKType);
  if (rc != 0) {
    std::cerr << pmemobj_errormsg() << std::endl;
    throw std::bad_alloc{};
  }
}

/*######################################################################################
 * Global classes
 *####################################################################################*/

/**
 * @brief A struct for holding thread-local PMEMoid instances.
 *
 */
struct TLSFields {
  /*####################################################################################
   * Public constants
   *##################################################################################*/

  /// The number of temporary fields per thread.
  static constexpr size_t kTmpFieldNum = 13;

  /*####################################################################################
   * Public member variables
   *##################################################################################*/

  /// The head pointer of garbage buffers.
  PMEMoid head{};

  /// A temporary field for swapping buffer heads.
  PMEMoid tmp_head{};

  /// Temporary fields to ensure fault tolerance of user-defined data structures.
  PMEMoid tmp_oids[kTmpFieldNum]{};

  /*####################################################################################
   * Public utilities
   *##################################################################################*/

  /**
   * @param oid a target PMEMoid instance.
   * @retval true if there is the same PMEMoid with the given one.
   * @retval false otherwise.
   */
  auto
  HasSamePMEMoid(const PMEMoid &oid)
  {
    for (size_t i = 0; i < kTmpFieldNum; ++i) {
      if (tmp_oids[i].pool_uuid_lo == oid.pool_uuid_lo && tmp_oids[i].off == oid.off) {
        return true;
      }
    }
    return false;
  }
};

/**
 * @brief A class for retaining unreleased garbages.
 *
 */
class PMEMoidBuffer
{
 public:
  /*####################################################################################
   * Public global constants
   *##################################################################################*/

  /// The size of buffers for retaining garbages.
  static constexpr size_t kBufferSize = (kPmemPageSize - sizeof(PMEMoid)) / sizeof(PMEMoid);

  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  /**
   * @brief Construct a new instance.
   *
   */
  constexpr PMEMoidBuffer() = default;

  PMEMoidBuffer(const PMEMoidBuffer &) = delete;
  auto operator=(const PMEMoidBuffer &) -> PMEMoidBuffer & = delete;
  PMEMoidBuffer(PMEMoidBuffer &&) = delete;
  auto operator=(PMEMoidBuffer &&) -> PMEMoidBuffer & = delete;

  /*####################################################################################
   * Public destructor
   *##################################################################################*/

  /**
   * @brief Destroy the PMEMoidBuffer object.
   *
   */
  ~PMEMoidBuffer() = default;

  /*####################################################################################
   * Public static utilities
   *##################################################################################*/

  /**
   * @brief Exchange the current head to the next buffer.
   *
   * @param buf the current head buffer.
   * @param head_addr the address of a head.
   * @param tmp_addr a temporary field for swapping.
   * @return PMEMoidBuffer*
   */
  static auto
  ExchangeHead(  //
      PMEMoidBuffer *buf,
      PMEMoid *head_addr,
      PMEMoid *tmp_addr)  //
      -> PMEMoidBuffer *
  {
    tmp_addr->pool_uuid_lo = head_addr->pool_uuid_lo;
    tmp_addr->off = head_addr->off;
    pmem_persist(tmp_addr, sizeof(PMEMoid));

    head_addr->off = buf->next_.off;
    pmem_persist(&(head_addr->off), kWordSize);

    pmemobj_free(tmp_addr);
    return reinterpret_cast<PMEMoidBuffer *>(pmemobj_direct(*head_addr));
  }

  /**
   * @brief Release all garbage for recovery.
   *
   * NOTE: This function does not perform any destruction on the remaining garbage.
   *
   * @param buf the head buffer.
   * @param tls the pointer to the thread-local fields.
   */
  static void
  ReleaseAllGarbages(  //
      PMEMoidBuffer *buf,
      TLSFields *tls)
  {
    while (true) {
      for (size_t i = 0; i < kBufferSize; ++i) {
        auto &oid = buf->garbages_[i];
        if (oid.pool_uuid_lo == 0 || oid.off == 0 || tls->HasSamePMEMoid(oid)) continue;

        pmemobj_free(&oid);
      }

      if (OID_IS_NULL(buf->next_)) break;
      buf = ExchangeHead(buf, &(tls->head), &(tls->tmp_head));
    }
  }

  /*####################################################################################
   * Public utilities
   *##################################################################################*/

  /**
   * @brief Add a given garbage PMEMoid into this buffer.
   *
   * If this function completes successfully, the specified PMEMoid becomes NULL.
   *
   * @param pos the position to be added.
   * @param[in,out] garbage the garbage pointer.
   */
  void
  AddGarbage(  //
      const size_t pos,
      PMEMoid *garbage)
  {
    garbages_[pos].pool_uuid_lo = garbage->pool_uuid_lo;
    garbages_[pos].off = garbage->off;
    pmem_persist(&garbages_[pos], sizeof(PMEMoid));

    garbage->off = kNullPtr;
    pmem_persist(&(garbage->off), kWordSize);
  }

  /**
   * @brief Reuse PMEMoid.
   *
   * @param pos the position of PMEMoid to be reused.
   * @param[out] out_page the destination of a reused PMEMoid.
   */
  void
  ReusePage(  //
      const size_t pos,
      PMEMoid *out_page)
  {
    out_page->pool_uuid_lo = garbages_[pos].pool_uuid_lo;
    out_page->off = garbages_[pos].off;
    pmem_persist(out_page, sizeof(PMEMoid));

    garbages_[pos].off = kNullPtr;
    pmem_persist(&(garbages_[pos].off), kWordSize);
  }

  /**
   * @brief Destruct a target PMEMoid.
   *
   * NOTE: This function only performs destruction and does not free it.
   *
   * @tparam T a target class for performing destruction.
   * @param pos the position of PMEMoid to be destructed.
   */
  template <class T>
  void
  DestructGarbage(const size_t pos)
  {
    auto *ptr = reinterpret_cast<T *>(pmemobj_direct(garbages_[pos]));
    ptr->~T();
  }

  /**
   * @brief Release a target PMEMoid.
   *
   * @param pos the position of PMEMoid to be released.
   */
  void
  ReleaseGarbage(const size_t pos)
  {
    pmemobj_free(&(garbages_[pos]));
  }

  /**
   * @brief Create the next buffer and link to this.
   *
   * @param pop pmemobj_pool instance for allocation.
   * @return the next buffer.
   */
  auto
  CreateNextBuffer(PMEMobjpool *pop)  //
      -> PMEMoidBuffer *
  {
    AllocatePmem(pop, &next_, kPmemPageSize);
    return reinterpret_cast<PMEMoidBuffer *>(pmemobj_direct(next_));
  }

 private:
  /*####################################################################################
   * Internal constants
   *##################################################################################*/

  /// The zero integer acts as nullptr.
  static constexpr uint64_t kNullPtr = 0;

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// Offset values of garbage PMEMoids.
  PMEMoid garbages_[kBufferSize]{};

  /// The next garbage buffer if exist.
  PMEMoid next_{};
};

/**
 * @brief A class for representing garbage lists on persitent memory.
 *
 * @tparam Target a target class of garbage collection.
 */
template <class Target>
class alignas(kCashLineSize) GarbageListOnPMEM
{
 public:
  /*####################################################################################
   * Public global constants
   *##################################################################################*/

  /// The size of buffers for retaining garbages.
  static constexpr size_t kBufferSize = PMEMoidBuffer::kBufferSize;

  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using T = typename Target::T;
  using IDManager = ::dbgroup::thread::IDManager;

  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  /**
   * @brief Construct a new GarbageListOnPMEM object.
   *
   * @param pop a pmemobj_pool instance for allocation.
   * @param tls_oid the pointer to a PMEMoid for thread-local fields.
   */
  constexpr GarbageListOnPMEM(  //
      PMEMobjpool *pop,
      PMEMoid *tls_oid)
      : pop_{pop}, tls_oid_{tls_oid}
  {
  }

  GarbageListOnPMEM(const GarbageListOnPMEM &) = delete;
  GarbageListOnPMEM(GarbageListOnPMEM &&) = delete;

  auto operator=(const GarbageListOnPMEM &) -> GarbageListOnPMEM & = delete;
  auto operator=(GarbageListOnPMEM &&) -> GarbageListOnPMEM & = delete;

  /*####################################################################################
   * Public destructors
   *##################################################################################*/

  /**
   * @brief Destroy the GarbageList object.
   *
   * If the list contains unreleased garbage, the destructor will forcibly release it.
   */
  ~GarbageListOnPMEM()
  {
    auto *head = head_.load(std::memory_order_relaxed);
    if (head == nullptr) {
      head = mid_;
    }
    if (head != nullptr) {
      GarbageBuffer::Clear(&head, std::numeric_limits<size_t>::max(), tls_fields_);
      delete head;
    }
  }

  /*####################################################################################
   * Public utility functions for worker threads
   *##################################################################################*/

  /**
   * @brief Get the temporary field for memory allocation.
   *
   * @param i the position of fields (0 <= i <= 13).
   * @return the address of the specified temporary field.
   */
  auto
  GetTmpField(const size_t i)  //
      -> PMEMoid *
  {
    assert(i >= 0);                       // NOLINT
    assert(i < TLSFields::kTmpFieldNum);  // NOLINT

    AssignCurrentThreadIfNeeded();
    return &(tls_fields_->tmp_oids[i]);
  }

  /**
   * @brief Add a new garbage instance.
   *
   * @param epoch an epoch value when a garbage is added.
   * @param garbage_ptr a pointer to a target garbage.
   */
  void
  AddGarbage(  //
      const size_t epoch,
      PMEMoid *garbage_ptr)
  {
    AssignCurrentThreadIfNeeded();
    GarbageBuffer::AddGarbage(&tail_, epoch, garbage_ptr, pop_);
  }

  /**
   * @brief Reuse a released memory page if it exists in the list.
   *
   * @param out_page an address to be stored a reusable page.
   */
  void
  GetPageIfPossible(PMEMoid *out_page)
  {
    AssignCurrentThreadIfNeeded();
    GarbageBuffer::ReusePage(&head_, out_page, tls_fields_);
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
      GarbageBuffer::Clear(&mid_, protected_epoch, tls_fields_);
    } else {
      if (!heartbeat_.expired()) {
        GarbageBuffer::Destruct(&mid_, protected_epoch);
        return;
      }

      auto *head = head_.load(std::memory_order_relaxed);
      if (head != nullptr) {
        mid_ = head;
        head_.store(nullptr, std::memory_order_relaxed);
      }
      GarbageBuffer::Clear(&mid_, protected_epoch, tls_fields_);
    }

    // check this list is alive
    if (!heartbeat_.expired() || !mid_->Empty()) return;

    // release buffers if the thread has exitted
    pmemobj_free(&(tls_fields_->head));
    delete mid_;
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
   * @tparam Target a target class of garbage collection.
   */
  class alignas(kCashLineSize) GarbageBuffer
  {
   public:
    /*##################################################################################
     * Public constructors and assignment operators
     *################################################################################*/

    /**
     * @brief Construct a new instance.
     *
     */
    constexpr explicit GarbageBuffer(PMEMoidBuffer *oid_buf) : buffer_{oid_buf} {}

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
     * @param pop a pmemobj_pool instance for allocation.
     */
    static auto
    AddGarbage(  //
        GarbageBuffer **buf_addr,
        const size_t epoch,
        PMEMoid *garbage,
        PMEMobjpool *pop)
    {
      auto *buf = *buf_addr;
      const auto pos = buf->end_pos_.load(std::memory_order_relaxed);

      // insert a new garbage
      buf->epochs_[pos] = epoch;
      buf->buffer_->AddGarbage(pos, garbage);

      // check whether the list is full
      if (pos >= kBufferSize - 1) {
        auto *new_tail = new GarbageBuffer{buf->buffer_->CreateNextBuffer(pop)};
        buf->next_ = new_tail;
        *buf_addr = new_tail;
      }

      // increment the end position
      buf->end_pos_.fetch_add(1, std::memory_order_release);
    }

    /**
     * @brief Reuse a garbage-collected memory page.
     *
     * @param buf_addr the address of the pointer of a target buffer.
     * @param out_page a persistent pointer to be stored a reusable page.
     * @param tls the pointer to thread local PMEMoids.
     */
    static void
    ReusePage(  //
        std::atomic<GarbageBuffer *> *buf_addr,
        PMEMoid *out_page,
        TLSFields *tls)
    {
      auto *buf = buf_addr->load(std::memory_order_relaxed);
      const auto pos = buf->begin_pos_.load(std::memory_order_relaxed);
      const auto mid_pos = buf->mid_pos_.load(std::memory_order_acquire);

      // check whether there are released garbage
      if (pos >= mid_pos) return;

      // get a released page
      buf->begin_pos_.fetch_add(1, std::memory_order_relaxed);
      buf->buffer_->ReusePage(pos, out_page);

      // check whether all the pages in the list are reused
      if (pos >= kBufferSize - 1) {
        // the list has become empty, so delete it
        buf_addr->store(buf->next_, std::memory_order_relaxed);
        PMEMoidBuffer::ExchangeHead(buf->buffer_, &(tls->head), &(tls->tmp_head));
        delete buf;
      }
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
          if (buf->epochs_[pos] >= protected_epoch) break;

          // only call destructor to reuse pages
          if constexpr (!std::is_same_v<T, void>) {
            buf->buffer_->template DestructGarbage<T>(pos);
          }
        }

        // update the position to make visible destructed garbage
        buf->mid_pos_.store(pos, std::memory_order_release);
        if (pos < kBufferSize) break;

        // release the next buffer recursively
        *buf_addr = buf->next_;
      }
    }

    /**
     * @brief Release garbage where their epoch is less than a protected one.
     *
     * @param buf_addr the address of the pointer of a target buffer.
     * @param protected_epoch a protected epoch.
     * @param tls the pointer to thread local PMEMoids.
     */
    static void
    Clear(  //
        GarbageBuffer **buf_addr,
        const size_t protected_epoch,
        TLSFields *tls)
    {
      while (true) {
        auto *buf = *buf_addr;
        const auto mid_pos = buf->mid_pos_.load(std::memory_order_relaxed);
        const auto end_pos = buf->end_pos_.load(std::memory_order_acquire);

        // release unprotected garbage
        auto pos = buf->begin_pos_.load(std::memory_order_relaxed);
        for (; pos < mid_pos; ++pos) {
          // the garbage has been already destructed
          buf->buffer_->ReleaseGarbage(pos);
        }
        for (; pos < end_pos; ++pos) {
          if (buf->epochs_[pos] >= protected_epoch) break;

          if constexpr (!std::is_same_v<T, void>) {
            buf->buffer_->template DestructGarbage<T>(pos);
          }
          buf->buffer_->ReleaseGarbage(pos);
        }

        buf->begin_pos_.store(pos, std::memory_order_relaxed);
        buf->mid_pos_.store(pos, std::memory_order_relaxed);

        if (pos < kBufferSize) break;

        // release the next buffer recursively
        *buf_addr = buf->next_;
        PMEMoidBuffer::ExchangeHead(buf->buffer_, &(tls->head), &(tls->tmp_head));
        delete buf;
      }
    }

   private:
    /*##################################################################################
     * Internal member variables
     *################################################################################*/

    /// The atomic instance to notify cleaner threads of the begin position.
    std::atomic_size_t begin_pos_{0};

    /// The atomic instance to notify cleaner threads of the middle position.
    std::atomic_size_t mid_pos_{0};

    /// Actual garbages pointers on persistemt memory.
    PMEMoidBuffer *buffer_{nullptr};

    /// Epoch values when each garbage is registered.
    size_t epochs_[kBufferSize]{};

    /// The atomic instance to notify cleaner threads of the tail position.
    std::atomic_size_t end_pos_{0};

    /// A pointer to a next garbage buffer.
    GarbageBuffer *next_{nullptr};
  };

  /*####################################################################################
   * Internal utility functions
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
      AllocatePmem(pop_, tls_oid_, sizeof(TLSFields));
      tls_fields_ = reinterpret_cast<TLSFields *>(pmemobj_direct(*tls_oid_));

      AllocatePmem(pop_, &(tls_fields_->head), kPmemPageSize);
      auto *buf = reinterpret_cast<PMEMoidBuffer *>(pmemobj_direct(tls_fields_->head));

      tail_ = new GarbageBuffer{buf};
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

  /// The pointer to a pmemobj_pool object.
  PMEMobjpool *pop_{nullptr};

  /// The address of the original PMEMoid of thread local fields.
  PMEMoid *tls_oid_{nullptr};

  /// The pointer to the thread local fields.
  TLSFields *tls_fields_{nullptr};

  /// A dummy array for cache line alignments
  uint64_t padding_[1]{};

  /// A mutex instance for modifying buffer pointers.
  std::mutex mtx_{};

  /// A garbage list that has not destructed pages.
  GarbageBuffer *mid_{nullptr};
};

}  // namespace dbgroup::memory::component

#endif  // MEMORY_COMPONENT_GARBAGE_LIST_ON_PMEM_HPP
