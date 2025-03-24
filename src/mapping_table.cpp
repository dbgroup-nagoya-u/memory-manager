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
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <utility>

// external libraries
#include "dbgroup/constants.hpp"
#include "dbgroup/lock/common.hpp"

// local sources
#include "dbgroup/memory/utility.hpp"

namespace dbgroup::memory
{
/*############################################################################*
 * Public constructors destructors
 *############################################################################*/

MappingTable::MappingTable(  //
    std::function<void(void *)> deleter)
    : deleter_{std::move(deleter)}
{
  auto *row = new (Allocate<Row>()) Row{};
  auto *sheet = new (Allocate<Sheet>()) Sheet{};
  sheet->rows[0].store(row, kRelaxed);
  sheets_[0].store(sheet, kRelaxed);
  cnt_.store(kLPIDFlag, kRelaxed);
}

MappingTable::~MappingTable()
{
  try {
    for (size_t i = 0; i < kSheetNum; ++i) {
      auto *sheet = sheets_[i].load(kRelaxed);
      if (sheet == nullptr) continue;
      for (size_t j = 0; j < kRowNum; ++j) {
        auto *row = sheet->rows[j].load(kRelaxed);
        if (row == nullptr) continue;
        for (size_t k = 0; k < kColNum; ++k) {
          auto *page = row->cols[k].load(kRelaxed);
          if (page == nullptr) continue;
          deleter_(page);
        }
        Release<Row>(row);
      }
      Release<Sheet>(sheet);
    }
  } catch (const std::exception &e) {
    std::cerr << e.what() << '\n';
  }
}

/*############################################################################*
 * Public APIs
 *############################################################################*/

auto
MappingTable::ReservePageID()  //
    -> uint64_t
{
  auto cur_id = cnt_.load(kRelaxed);
  for (size_t i = 1; true; ++i) {
    while ((cur_id & kIDMask) < kColNum) {
      if (cur_id > kMaxPID) throw std::runtime_error{"The mapping table has no space."};
      if (cnt_.compare_exchange_weak(cur_id, cur_id + kColUnit, kRelaxed, kRelaxed)) {
        if ((cur_id & kIDMask) >= kColNum - 1) {
          AllocateNewSpace(cur_id);
        }
        return cur_id;
      }
      CPP_UTILITY_SPINLOCK_HINT
    }
    if (i <= ::dbgroup::lock::kRetryNum) {
      CPP_UTILITY_SPINLOCK_HINT
      cur_id = cnt_.load(kRelaxed);
      continue;
    }

    // insert back off
    i = 0;
    auto prev_id = cur_id;
    std::this_thread::sleep_for(::dbgroup::lock::kBackOffTime);
    cur_id = cnt_.load(kRelaxed);
    if (cur_id != prev_id) continue;

    // the last thread may be stalled
    cur_id = AllocateNewSpace(cur_id);
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

  const auto reserved = (tab_id * (kSheetNum + 1) + row_id + 3) * kVMPageSize;
  const auto empty_num = 2 * kSheetNum + kSheetNum - col_id - row_id - tab_id - 3;
  const auto used = reserved - empty_num * kWordSize;

  return {used, reserved};
}

/*############################################################################*
 * Public APIs for modifying table contents
 *############################################################################*/

void
MappingTable::Store(  //
    const uint64_t pid,
    const void *page)
{
  auto &cell = GetCell(pid);
  cell.store(const_cast<void *>(page), kRelease);
}

auto
MappingTable::CAS(  //
    const uint64_t pid,
    void *&expected,
    const void *desired)  //
    -> bool
{
  auto &cell = GetCell(pid);
  return cell.compare_exchange_weak(expected, const_cast<void *>(desired), kRelease, kAcquire);
}

auto
MappingTable::CASStrong(  //
    const uint64_t pid,
    void *&expected,
    const void *desired)  //
    -> bool
{
  auto &cell = GetCell(pid);
  return cell.compare_exchange_strong(expected, const_cast<void *>(desired), kRelease, kAcquire);
}

/*############################################################################*
 * Internal utilities
 *############################################################################*/

auto
MappingTable::GetCell(   //
    const uint64_t pid)  //
    -> std::atomic<void *> &
{
  const auto sheet_id = (pid >> kSheetShift) & kIDMask;
  const auto row_id = (pid >> kRowShift) & kIDMask;
  const auto col_id = pid & kIDMask;
  if ((pid & kLPIDFlag) == 0 || sheet_id >= kSheetNum || row_id >= kRowNum || col_id >= kColNum) {
    throw std::runtime_error{"The process accessed the invalid page ID."};
  }

  auto *sheet = sheets_[sheet_id].load(kRelaxed);
  auto *row = sheet->rows[row_id].load(kRelaxed);
  return row->cols[col_id];
}

auto
MappingTable::AllocateNewSpace(  //
    const uint64_t cur_id)       //
    -> uint64_t
{
  auto new_id = (cur_id + kRowUnit) & ~kIDMask;
  auto row_id = (new_id >> kRowShift) & kIDMask;
  Sheet *sheet;
  if (row_id < kRowNum) {
    sheet = sheets_[(new_id >> kSheetShift) & kIDMask].load(kRelaxed);
  } else {
    // create a new sheet
    row_id = 0;
    new_id = (new_id + kSheetUnit) & ~(kIDMask << kRowShift);
    auto &sheet_ref = sheets_[(new_id >> kSheetShift) & kIDMask];
    auto *expected = sheet_ref.load(kRelaxed);
    if (expected != nullptr) return cnt_.load(kRelaxed);
    sheet = new (Allocate<Sheet>()) Sheet{};
    if (!sheet_ref.compare_exchange_strong(expected, sheet, kRelaxed, kRelaxed)) {
      Release<Sheet>(sheet);
      return cnt_.load(kRelaxed);
    };
  }

  // create a new row
  auto &row_ref = sheet->rows[row_id];
  auto *expected = row_ref.load(kRelaxed);
  if (expected != nullptr) return cnt_.load(kRelaxed);
  auto *row = new (Allocate<Row>()) Row{};
  if (!row_ref.compare_exchange_strong(expected, row, kRelaxed, kRelaxed)) {
    Release<Row>(row);
    return cnt_.load(kRelaxed);
  }

  cnt_.store(new_id, kRelaxed);
  return new_id;
}
}  // namespace dbgroup::memory
