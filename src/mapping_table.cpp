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

// the corresponding header
#include "dbgroup/memory/mapping_table.hpp"

// C++ standard libraries
#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <new>
#include <stdexcept>
#include <thread>
#include <utility>

// our libraries
#include <dbgroup/constants.hpp>
#include <dbgroup/lock/utility.hpp>

// local sources
#include "dbgroup/memory/utility.hpp"

namespace dbgroup::memory
{
/*############################################################################*
 * Public constructors destructors
 *############################################################################*/

MappingTable::MappingTable(  //
    const size_t page_size)
    : page_align_{GetAlignValOnVirtualPages(page_size)}
{
  auto* const row = new (Allocate<Row>()) Row{};
  auto* const sheet = new (Allocate<Sheet>()) Sheet{};
  sheet->rows[0].store(row, kRelaxed);
  sheets_[0].store(sheet, kRelaxed);
  cnt_.store(kLPIDFlag, kRelaxed);
}

MappingTable::~MappingTable()
{
  // clear the slots linking reusable page IDs
  for (auto pid = free_list_->head; pid;) {
    auto& cell = GetCellUnchecked(pid);
    pid = std::bit_cast<uint64_t>(cell.load(kRelaxed));
    cell.store(nullptr, kRelaxed);
  }

  for (size_t i = 0; i < kSheetNum; ++i) {
    auto* const sheet = sheets_[i].load(kRelaxed);
    if (!sheet) continue;
    for (size_t j = 0; j < kRowNum; ++j) {
      auto* const row = sheet->rows[j].load(kRelaxed);
      if (!row) continue;
      for (size_t k = 0; k < kColNum; ++k) {
        auto* const page = row->cols[k].load(kRelaxed);
        if (!page) continue;
        ::operator delete(page, page_align_);
      }
      Release<Row>(row);
    }
    Release<Sheet>(sheet);
  }
}

/*############################################################################*
 * Public APIs
 *############################################################################*/

auto
MappingTable::ReservePageID()  //
    -> uint64_t
{
  // reuse a deleted page ID without waiting for other threads
  if (free_num_.load(kRelaxed) > 0 && free_list_->mtx.try_lock()) {
    const std::lock_guard guard{free_list_->mtx, std::adopt_lock};
    const auto pid = free_list_->head;
    if (pid) {
      auto& cell = GetCellUnchecked(pid);
      free_list_->head = std::bit_cast<uint64_t>(cell.load(kRelaxed));
      cell.store(nullptr, kRelaxed);
      free_num_.store(free_num_.load(kRelaxed) - 1, kRelaxed);
      return pid;
    }
  }

  auto cur_id = cnt_.load(kRelaxed);
  while (true) {
    const auto col_id = cur_id & kIDMask;
    if (col_id >= kColNum) {
      cur_id = WaitForNextRow(cur_id);
      continue;
    }

    // the last column moves the counter to the next row if it has been allocated
    const auto is_last = col_id == kColNum - 1;
    const auto next_id = is_last && HasNextRow(cur_id) ? GetNextRowID(cur_id) : cur_id + kColUnit;
    if (!cnt_.compare_exchange_weak(cur_id, next_id, kAcqRel, kRelaxed)) {
      CPP_UTILITY_SPINLOCK_HINT
      continue;
    }

    if (col_id == kPreallocColID) {
      AllocateNextRow(cur_id);  // prepare the next row in advance
    } else if (is_last && next_id == cur_id + kColUnit) {
      // the next row has not been prepared in time (or this table becomes full)
      static_cast<void>(AdvanceToNextRow(next_id));
    }
    return cur_id;
  }
}

auto
MappingTable::GetMemoryUsage() const  //
    -> std::pair<size_t, size_t>
{
  const auto id = cnt_.load(kAcquire);
  const auto tab_id = (id >> kSheetShift) & kIDMask;
  const auto row_id = (id >> kRowShift) & kIDMask;
  const auto col_id = id & kIDMask;

  // count rows and sheets including those allocated in advance
  auto sheet_num = tab_id + 1;
  auto row_num = tab_id * kRowNum + row_id + 1;
  const auto next_tab_id = (GetNextRowID(id) >> kSheetShift) & kIDMask;
  if (next_tab_id != tab_id && next_tab_id < kSheetNum && sheets_[next_tab_id].load(kAcquire)) {
    ++sheet_num;
  }
  if (HasNextRow(id)) {
    ++row_num;
  }

  // the actual usage consists of a header and pointers to sheets, rows, and pages
  const auto id_num = (tab_id * kRowNum + row_id) * kColNum + col_id - free_num_.load(kRelaxed);
  const auto reserved = (1 + sheet_num + row_num) * kVMPageSize;
  const auto used = kCacheLineSize + (sheet_num + row_num + id_num) * kWordSize;

  return {used, reserved};
}

/*############################################################################*
 * Public APIs for modifying table contents
 *############################################################################*/

void
MappingTable::Store(  //
    const uint64_t pid,
    const void* const page)
{
  auto& cell = GetCell(pid);
  cell.store(const_cast<void*>(page), kRelease);
}

auto
MappingTable::CAS(  //
    const uint64_t pid,
    void*& expected,
    const void* const desired)  //
    -> bool
{
  auto& cell = GetCell(pid);
  return cell.compare_exchange_weak(expected, const_cast<void*>(desired), kRelease, kAcquire);
}

auto
MappingTable::CASStrong(  //
    const uint64_t pid,
    void*& expected,
    const void* const desired)  //
    -> bool
{
  auto& cell = GetCell(pid);
  return cell.compare_exchange_strong(expected, const_cast<void*>(desired), kRelease, kAcquire);
}

/*############################################################################*
 * Internal utilities
 *############################################################################*/

auto
MappingTable::GetCell(   //
    const uint64_t pid)  //
    -> std::atomic<void*>&
{
  const auto sheet_id = (pid >> kSheetShift) & kIDMask;
  const auto row_id = (pid >> kRowShift) & kIDMask;
  const auto col_id = pid & kIDMask;
  if ((pid & kLPIDFlag) == 0 || sheet_id >= kSheetNum || row_id >= kRowNum || col_id >= kColNum) {
    throw std::runtime_error{"The process accessed the invalid page ID."};
  }
  return GetCellUnchecked(pid);
}

auto
MappingTable::GetCellUnchecked(   //
    const uint64_t pid) noexcept  //
    -> std::atomic<void*>&
{
  const auto sheet_id = (pid >> kSheetShift) & kIDMask;
  const auto row_id = (pid >> kRowShift) & kIDMask;
  const auto col_id = pid & kIDMask;
  auto* const sheet = sheets_[sheet_id].load(kAcquire);
  auto* const row = sheet->rows[row_id].load(kAcquire);
  return row->cols[col_id];
}

auto
MappingTable::HasNextRow(                  //
    const uint64_t cur_id) const noexcept  //
    -> bool
{
  const auto next_id = GetNextRowID(cur_id);
  const auto tab_id = (next_id >> kSheetShift) & kIDMask;
  if (tab_id >= kSheetNum) return false;
  const auto* const sheet = sheets_[tab_id].load(kAcquire);
  return sheet && sheet->rows[(next_id >> kRowShift) & kIDMask].load(kAcquire);
}

auto
MappingTable::AllocateNextRow(  //
    const uint64_t cur_id)      //
    -> bool
{
  const auto next_id = GetNextRowID(cur_id);
  const auto tab_id = (next_id >> kSheetShift) & kIDMask;
  if (tab_id >= kSheetNum) return false;

  // create a new sheet if needed
  auto& sheet_ref = sheets_[tab_id];
  auto* sheet = sheet_ref.load(kAcquire);
  if (!sheet) {
    auto* const new_sheet = new (Allocate<Sheet>()) Sheet{};
    if (sheet_ref.compare_exchange_strong(sheet, new_sheet, kAcqRel, kAcquire)) {
      sheet = new_sheet;
    } else {
      Release<Sheet>(new_sheet);
    }
  }

  // create a new row if needed
  auto& row_ref = sheet->rows[(next_id >> kRowShift) & kIDMask];
  auto* row = row_ref.load(kAcquire);
  if (!row) {
    auto* const new_row = new (Allocate<Row>()) Row{};
    if (!row_ref.compare_exchange_strong(row, new_row, kRelease, kRelaxed)) {
      Release<Row>(new_row);
    }
  }
  return true;
}

auto
MappingTable::AdvanceToNextRow(    //
    const uint64_t overflowed_id)  //
    -> bool
{
  if (!AllocateNextRow(overflowed_id)) return false;
  auto expected = overflowed_id;
  cnt_.compare_exchange_strong(expected, GetNextRowID(overflowed_id), kAcqRel, kRelaxed);
  return true;
}

auto
MappingTable::WaitForNextRow(      //
    const uint64_t overflowed_id)  //
    -> uint64_t
{
  for (size_t i = 0; true; ++i) {
    if (i >= kMaxYieldNum) {
      // the thread moving the counter may be stalled
      if (!AdvanceToNextRow(overflowed_id)) {
        throw std::runtime_error{"The mapping table has no space."};
      }
      i = 0;
    }
    std::this_thread::yield();
    const auto cur_id = cnt_.load(kRelaxed);
    if (cur_id != overflowed_id) return cur_id;
  }
}

void
MappingTable::RecyclePageID(  //
    const uint64_t pid)
{
  auto& cell = GetCellUnchecked(pid);

  const std::lock_guard guard{free_list_->mtx};
  cell.store(std::bit_cast<void*>(free_list_->head), kRelaxed);
  free_list_->head = pid;
  free_num_.store(free_num_.load(kRelaxed) + 1, kRelaxed);
}

/*############################################################################*
 * Public classes for garbage collection
 *############################################################################*/

MappingTable::PageIDGarbage::~PageIDGarbage()
{  //
  table->RecyclePageID(pid);
}

}  // namespace dbgroup::memory
