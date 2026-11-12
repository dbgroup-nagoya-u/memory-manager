/*
 * Copyright 2024 Database Group, Nagoya University
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

#ifndef MEMORY_MANAGER_DBGROUP_MEMORY_MAPPING_TABLE_HPP_
#define MEMORY_MANAGER_DBGROUP_MEMORY_MAPPING_TABLE_HPP_

// C++ standard libraries
#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <new>
#include <stdexcept>
#include <utility>

// our libraries
#include <dbgroup/constants.hpp>

namespace dbgroup::memory
{
/**
 * @brief A class for managing page IDs and their logical pointers.
 *
 * @note If you delete page IDs by `DeletePageID`, destroy the given garbage
 * collector before this table because the collector reuses the IDs via this
 * table.
 */
class alignas(kVMPageSize) MappingTable
{
 public:
  /*##########################################################################*
   * Public constants
   *##########################################################################*/

  /// @brief The begin bit position of column IDs.
  static constexpr size_t kColShift = 1;

  /// @brief The begin bit position of row IDs.
  static constexpr size_t kRowShift = 16;

  /// @brief The begin bit position of sheet IDs.
  static constexpr size_t kSheetShift = 32;

  /// @brief A flag for indicating page IDs.
  static constexpr uint64_t kLPIDFlag = 1UL << 63UL;

  /// @brief The capacity of each array (rows and columns).
  static constexpr size_t kColNum = kVMPageSize / kWordSize;

  /// @brief The capacity of each array (rows and columns).
  static constexpr size_t kRowNum = kVMPageSize / kWordSize;

  /// @brief The capacity of each array (rows and columns).
  static constexpr size_t kSheetNum = (kVMPageSize - kCacheLineSize) / kWordSize;

  /// @brief The column ID for allocating the next row in advance.
  static constexpr size_t kPreallocColID = kColNum * 7 / 10;

  /*##########################################################################*
   * Public classes for garbage collection
   *##########################################################################*/

  /**
   * @brief A garbage instance for reusing a deleted page ID.
   *
   */
  struct PageIDGarbage {
    /*########################################################################*
     * Public constructors and assignment operators
     *########################################################################*/

    /**
     * @param table The table containing a deleted page ID.
     * @param pid A deleted page ID.
     */
    PageIDGarbage(  //
        MappingTable* table,
        uint64_t pid)
        : table{table}
        , pid{pid}
    {
    }

    PageIDGarbage(const PageIDGarbage&) = delete;
    PageIDGarbage(PageIDGarbage&&) = delete;

    auto operator=(const PageIDGarbage&) -> PageIDGarbage& = delete;
    auto operator=(PageIDGarbage&&) -> PageIDGarbage& = delete;

    /*########################################################################*
     * Public destructors
     *########################################################################*/

    /// @brief Reuse the page ID after garbage collection.
    ~PageIDGarbage();

    /*########################################################################*
     * Public member variables
     *########################################################################*/

    /// @brief The table containing the page ID.
    MappingTable* table;

    /// @brief The deleted page ID.
    uint64_t pid;
  };

  /**
   * @brief A target class of garbage collectors for deleted page IDs.
   *
   */
  struct PageIDTarget {
    /// @brief Call the destructor to reuse page IDs.
    using T = PageIDGarbage;

    /// @brief Release garbage instances immediately.
    static constexpr bool kReusePages = false;
  };

  /*##########################################################################*
   * Public constructors and assignment operators
   *##########################################################################*/

  /**
   * @brief Construct a new mapping table.
   *
   * @param page_size The size of pages stored in this table.
   * @note Stored pages must be allocated with the alignment of
   * `GetAlignValOnVirtualPages(page_size)` (e.g., `Allocate<Page4Ki>()`).
   */
  explicit MappingTable(  //
      size_t page_size);

  MappingTable(const MappingTable&) = delete;
  MappingTable(MappingTable&&) = delete;

  auto operator=(const MappingTable&) -> MappingTable& = delete;
  auto operator=(MappingTable&&) -> MappingTable& = delete;

  /*##########################################################################*
   * Public destructors
   *##########################################################################*/

  /**
   * @brief Destroy the MappingTable object.
   *
   * @note This destructor will release all the stored pages with the alignment
   * of `GetAlignValOnVirtualPages(page_size)`.
   */
  ~MappingTable();

  /*##########################################################################*
   * Public APIs
   *##########################################################################*/

  /**
   * @param pid A target page ID
   * @retval true if a given page ID is valid.
   * @retval false otherwise.
   */
  [[nodiscard]] static constexpr auto
  IsPageID(                //
      const uint64_t pid)  //
      -> bool
  {
    const auto sheet_id = (pid >> kSheetShift) & kIDMask;
    const auto row_id = (pid >> kRowShift) & kIDMask;
    const auto col_id = pid & kIDMask;
    return pid >= kLPIDFlag && sheet_id < kSheetNum && row_id < kRowNum && col_id < kColNum;
  }

  /**
   * @return A reserved page ID.
   * @note This function reuses a deleted page ID if possible, and the slot of
   * the returned page ID always contains `nullptr`.
   */
  [[nodiscard]] auto ReservePageID()  //
      -> uint64_t;

  /**
   * @brief Delete a page ID and reuse it after garbage collection.
   *
   * @tparam GC A class of garbage collectors containing `PageIDTarget`.
   * @param pid A page ID to be deleted.
   * @param gc A garbage collector to defer reusing the page ID.
   * @exception `std::runtime_error` if an invalid page ID is given.
   * @note This function does not release the page stored in the slot, so
   * threads detaching the page must release it. Threads must not modify the
   * slot after this function.
   */
  template <class GC>
  void
  DeletePageID(  //
      const uint64_t pid,
      GC& gc)
  {
    static_assert(GC::template HasTarget<PageIDTarget>(),
                  "Garbage collectors must contain `MappingTable::PageIDTarget`.");
    if (!IsPageID(pid) || pid >= cnt_.load(kAcquire)) {
      throw std::runtime_error{"The process tried to delete the invalid page ID."};
    }
    gc.template AddGarbage<PageIDTarget>(new PageIDGarbage{this, pid});
  }

  /**
   * @brief Compute the memory usage of this table (excluding stored pages).
   *
   * @return The pair of the actual and reserved usage in bytes, where the
   * actual usage excludes unused slots in reserved spaces.
   */
  [[nodiscard]] auto GetMemoryUsage() const  //
      -> std::pair<size_t, size_t>;

  /*##########################################################################*
   * Public APIs for modifying table contents
   *##########################################################################*/

  /**
   * @brief Load a page from the corresponding slot of a given page ID.
   *
   * @tparam T An optional class template to cast the output.
   * @param pid A target page ID.
   * @return The address of a stored page.
   * @note This function sets a acquire fence.
   */
  template <class T = void>
  [[nodiscard]] auto
  Load(                          //
      const uint64_t pid) const  //
      -> T*
  {
    const auto& cell = const_cast<MappingTable*>(this)->GetCell(pid);
    return std::bit_cast<T*>(cell.load(kAcquire));
  }

  /**
   * @brief Store a new page to the slot of a given page ID.
   *
   * @param pid A target page ID.
   * @param page A page to be stored.
   * @note This function sets a release fence.
   */
  void Store(  //
      uint64_t pid,
      const void* page);

  /**
   * @brief Compare-and-swap given pages on the slot of a given page ID.
   *
   * @param pid A target page ID.
   * @param[in,out] expected An contained page.
   * @param desired A page to be stored.
   * @retval true if CAS succeeds.
   * @retval false otherwise.
   * @note This function sets release/acquire fences when the CAS succeeds/fails.
   */
  auto CAS(  //
      uint64_t pid,
      void*& expected,
      const void* desired)  //
      -> bool;

  /**
   * @brief Compare-and-swap given pages on the slot of a given page ID.
   *
   * @param pid A target page ID.
   * @param[in,out] expected An contained page.
   * @param desired A page to be stored.
   * @retval true if CAS succeeds.
   * @retval false otherwise.
   * @note This function sets release/acquire fences when the CAS succeeds/fails.
   */
  auto CASStrong(  //
      uint64_t pid,
      void*& expected,
      const void* desired)  //
      -> bool;

 private:
  /*##########################################################################*
   * Internal classes
   *##########################################################################*/

  struct alignas(kVMPageSize) Row {
    std::atomic<void*> cols[kVMPageSize / kWordSize] = {};
  };

  struct alignas(kVMPageSize) Sheet {
    std::atomic<Row*> rows[kVMPageSize / kWordSize] = {};
  };

  /// @brief A list of reusable page IDs linked via their slots.
  struct FreeList {
    /// @brief A mutex for modifying this list.
    std::mutex mtx{};

    /// @brief The first reusable page ID (zero for an empty list).
    uint64_t head{};
  };

  /*##########################################################################*
   * Internal constants
   *##########################################################################*/

  /// @brief The unit value for incrementing column IDs.
  static constexpr uint64_t kColUnit = 1UL;

  /// @brief The unit value for incrementing row IDs.
  static constexpr uint64_t kRowUnit = 1UL << kRowShift;

  /// @brief The unit value for incrementing table IDs.
  static constexpr uint64_t kSheetUnit = 1UL << kSheetShift;

  /// @brief A bit mask for extracting IDs.
  static constexpr uint64_t kIDMask = 0xFFFFUL;

  /// @brief The number of yields before helping a thread allocating a row.
  static constexpr size_t kMaxYieldNum = 100;

  /*##########################################################################*
   * Internal utilities
   *##########################################################################*/

  /**
   * @param pid A target page ID.
   * @return The reference of a corresponding cell.
   * @exception `std::runtime_error` if an invalid page ID is given.
   */
  auto GetCell(      //
      uint64_t pid)  //
      -> std::atomic<void*>&;

  /**
   * @param pid A reserved page ID.
   * @return The reference of a corresponding cell.
   * @note This function does not validate a given page ID.
   */
  auto GetCellUnchecked(      //
      uint64_t pid) noexcept  //
      -> std::atomic<void*>&;

  /**
   * @param cur_id A page ID (or the counter value) in a row.
   * @return The first page ID in the next row.
   * @note The returned ID may exceed the capacity of this table.
   */
  [[nodiscard]]
  static constexpr auto
  GetNextRowID(               //
      const uint64_t cur_id)  //
      -> uint64_t
  {
    auto next_id = (cur_id + kRowUnit) & ~kIDMask;
    if (((next_id >> kRowShift) & kIDMask) >= kRowNum) {
      next_id = (next_id + kSheetUnit) & ~(kIDMask << kRowShift);
    }
    return next_id;
  }

  /**
   * @param cur_id A page ID (or the counter value) in a row.
   * @retval true if the next row has been allocated.
   * @retval false otherwise.
   */
  [[nodiscard]]
  auto HasNextRow(                     //
      uint64_t cur_id) const noexcept  //
      -> bool;

  /**
   * @brief Allocate the next row (and a new sheet if needed) if not exist.
   *
   * @param cur_id A page ID (or the counter value) in a row.
   * @retval true if the next row exists.
   * @retval false if this table has no space.
   */
  auto AllocateNextRow(  //
      uint64_t cur_id)   //
      -> bool;

  /**
   * @brief Move the overflowed counter to the first page ID in the next row.
   *
   * @param overflowed_id The overflowed counter value.
   * @retval true if the counter has been moved (by any thread).
   * @retval false if this table has no space.
   */
  auto AdvanceToNextRow(       //
      uint64_t overflowed_id)  //
      -> bool;

  /**
   * @brief Wait for a thread moving the overflowed counter to the next row.
   *
   * @param overflowed_id The overflowed counter value.
   * @return The current counter value.
   * @exception `std::runtime_error` if this table has no space.
   * @note If the counter does not change for a while, this function helps
   * moving it because the thread may be stalled.
   */
  auto WaitForNextRow(         //
      uint64_t overflowed_id)  //
      -> uint64_t;

  /**
   * @brief Register a deleted page ID for reuse.
   *
   * @param pid A deleted page ID.
   * @note The slot of the deleted page ID stores the next reusable page ID to
   * link reusable IDs without additional memory.
   */
  void RecyclePageID(  //
      uint64_t pid);

  /*##########################################################################*
   * Static assertions
   *##########################################################################*/

  // the members before sheets must fit in a cache line to fit a table in a page
  static_assert(sizeof(std::atomic_uint64_t) + sizeof(std::atomic_size_t) + sizeof(std::align_val_t)
                    + sizeof(std::unique_ptr<FreeList>)
                <= kCacheLineSize);

  /*##########################################################################*
   * Internal member variables
   *##########################################################################*/

  /// @brief An atomic counter for incrementing page IDs.
  std::atomic_uint64_t cnt_{};

  /// @brief The number of reusable page IDs.
  std::atomic_size_t free_num_{};

  /// @brief The alignment of stored pages.
  std::align_val_t page_align_{};

  /// @brief Reusable page IDs.
  std::unique_ptr<FreeList> free_list_{std::make_unique<FreeList>()};

  /// @brief Mapping tables.
  alignas(kCacheLineSize) std::atomic<Sheet*> sheets_[kSheetNum] = {};
};

}  // namespace dbgroup::memory

#endif  // MEMORY_MANAGER_DBGROUP_MEMORY_MAPPING_TABLE_HPP_
