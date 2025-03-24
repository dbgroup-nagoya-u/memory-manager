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
#include <cstddef>
#include <cstdint>
#include <functional>
#include <utility>

// external libraries
#include "dbgroup/constants.hpp"

// local sources
#include "dbgroup/memory/utility.hpp"

namespace dbgroup::memory
{
/**
 * @brief A class for managing page IDs and their logical pointers.
 *
 */
class alignas(kVMPageSize) MappingTable
{
 public:
  /*############################################################################
   * Public constants
   *##########################################################################*/

  /// @brief The begin bit position of column IDs.
  static constexpr size_t kColShift = 1;

  /// @brief The begin bit position of row IDs.
  static constexpr size_t kRowShift = 16;

  /// @brief The begin bit position of sheet IDs.
  static constexpr size_t kSheetShift = 32;

  /// @brief A flag for indicating page IDs.
  static constexpr uint64_t kLPIDFlag = 1UL << 63;

  /// @brief The capacity of each array (rows and columns).
  static constexpr size_t kColNum = kVMPageSize / kWordSize;

  /// @brief The capacity of each array (rows and columns).
  static constexpr size_t kRowNum = kVMPageSize / kWordSize;

  /// @brief The capacity of each array (rows and columns).
  static constexpr size_t kSheetNum = (kVMPageSize - kCacheLineSize) / kWordSize;

  /*############################################################################
   * Public constructors and assignment operators
   *##########################################################################*/

  /**
   * @brief Construct a new mapping table.
   *
   */
  explicit MappingTable(  //
      std::function<void(void *)> deleter = Release<void>);

  MappingTable(const MappingTable &) = delete;
  MappingTable(MappingTable &&) = delete;

  auto operator=(const MappingTable &) -> MappingTable & = delete;
  auto operator=(MappingTable &&) -> MappingTable & = delete;

  /*############################################################################
   * Public destructors
   *##########################################################################*/

  /**
   * @brief Destroy the MappingTable object.
   *
   * @note This destructor will release all the pages.
   */
  ~MappingTable();

  /*############################################################################
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
   */
  [[nodiscard]] auto ReservePageID()  //
      -> uint64_t;

  /**
   * @brief Compute memory usage.
   *
   * @retval 2nd: The actual usage in bytes.
   * @retval 3rd: The virtual usage in bytes.
   */
  [[nodiscard]] auto GetMemoryUsage() const  //
      -> std::pair<size_t, size_t>;

  /*############################################################################
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
      -> T *
  {
    const auto &cell = const_cast<MappingTable *>(this)->GetCell(pid);
    return reinterpret_cast<T *>(cell.load(kAcquire));
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
      const void *page);

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
      void *&expected,
      const void *desired)  //
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
      void *&expected,
      const void *desired)  //
      -> bool;

 private:
  /*############################################################################
   * Internal classes
   *##########################################################################*/

  struct alignas(kVMPageSize) Row {
    std::atomic<void *> cols[kVMPageSize / kWordSize] = {};
  };

  struct alignas(kVMPageSize) Sheet {
    std::atomic<Row *> rows[kVMPageSize / kWordSize] = {};
  };

  /*############################################################################
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

  /// @brief The maximum logical page ID.
  static constexpr uint64_t kMaxPID{((kSheetNum - 1) << kSheetShift)  //
                                    | ((kRowNum - 1) << kRowShift)    //
                                    | (kColNum - 1) | kLPIDFlag};

  /*############################################################################
   * Internal utilities
   *##########################################################################*/

  /**
   * @param pid A target page ID.
   * @return The reference of a corresponding cell.
   */
  auto GetCell(      //
      uint64_t pid)  //
      -> std::atomic<void *> &;

  /**
   * @brief Allocate a new row (and a new sheet if needed).
   *
   * @param cur_id The expected current page ID.
   * @return A new page ID.
   */
  auto AllocateNewSpace(  //
      uint64_t cur_id)    //
      -> uint64_t;

  /*############################################################################
   * Internal member variables
   *##########################################################################*/

  /// @brief An atomic counter for incrementing page IDs.
  std::atomic_uint64_t cnt_{};

  /// @brief A function to release stored pages.
  std::function<void(void *)> deleter_{};

  /// @brief Padding space for the cache line alignment.
  std::byte padding_[kCacheLineSize - 40] = {};

  /// @brief Mapping tables.
  std::atomic<Sheet *> sheets_[kSheetNum] = {};
};

}  // namespace dbgroup::memory

#endif  // MEMORY_MANAGER_DBGROUP_MEMORY_MAPPING_TABLE_HPP_
