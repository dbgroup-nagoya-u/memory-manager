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
#include "dbgroup/memory/component/reuse_list.hpp"

// C++ standard libraries
#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>

// external libraries
#include "dbgroup/constants.hpp"
#include "dbgroup/lock/common.hpp"

// local sources
#include "dbgroup/memory/utility.hpp"

namespace dbgroup::memory::component
{
/*##############################################################################
 * Public APIs for clients
 *############################################################################*/

void
ReuseList::AddPages(  //
    std::atomic_uintptr_t *tail_addr,
    std::vector<void *> &pages,
    const size_t reuse_capacity)
{
  auto uptr = tail_addr->load(kAcquire);
  if (!tail_addr->compare_exchange_strong(uptr, uptr + kCntUnit, kRelaxed, kRelaxed)) return;

  auto *list = std::bit_cast<ReuseList *>(uptr & kPtrMask);
  auto tail = list->tail_.load(kRelaxed);
  while (!pages.empty() && tail < kReuseListCapacity
         && (tail - list->head_.load(kRelaxed)) < reuse_capacity) {
    // add reusable pages
    auto &dest = list->pages_[tail];
    auto *cur_page = dest.load(kRelaxed);
    if (cur_page == nullptr
        && dest.compare_exchange_strong(cur_page, pages.back(), kRelaxed, kRelaxed)) {
      pages.pop_back();
    }
    if (list->tail_.load(kRelaxed) != tail) goto end;

    // if the list has free space, only update header
    const auto next_tail = tail + 1;
    if (next_tail < kReuseListCapacity) {
      if (!list->tail_.compare_exchange_strong(tail, next_tail, kRelease)) goto end;
      tail = next_tail;
      continue;
    }

    // if the list has been full, go to the next list
    auto *next = list->next_.load(kAcquire);
    if (next == nullptr) {
      auto *new_next = new ReuseList{list};
      if (!list->next_.compare_exchange_strong(next, new_next, kRelease, kRelaxed)) {
        delete new_next;
        goto end;
      }
      next = new_next;
    }

    // update the tail list on a list holder
    for (uptr = tail_addr->load(kRelaxed); true;) {
      auto *cur = std::bit_cast<ReuseList *>(uptr & kPtrMask);
      if (cur != list) goto end;
      const auto next_ptr = std::bit_cast<uintptr_t>(next) + (uptr & ~kPtrMask);
      if (tail_addr->compare_exchange_weak(uptr, next_ptr, kRelease, kRelaxed)) break;
    }
    list->tail_.store(kReuseListCapacity, kRelease);

    // go to the next loop with the next list
    list = next;
    tail = next->tail_.load(kRelaxed);
  }

  // delete the head list if needed
  if ((tail_addr->load(kAcquire) >> kCntShift) == 1 && list->prev_ != nullptr) {
    auto *prev = list->prev_;
    while (prev->prev_ != nullptr) {
      list = prev;
      prev = prev->prev_;
    }
    if (prev->head_.load(kAcquire) < kReuseListCapacity) goto end;
    list->prev_ = nullptr;
    delete prev;
  }

end:
  tail_addr->fetch_sub(kCntUnit, kRelease);
}

auto
ReuseList::GetPage(                       //
    std::atomic<ReuseList *> *head_addr)  //
    -> void *
{
  void *page = nullptr;
  auto *list = head_addr->load(kRelaxed);
  auto head = list->head_.load(kRelaxed);
  if (head < list->tail_.load(kAcquire)) {
    page = list->pages_[head].load(kRelaxed);
    if (head == kReuseListCapacity - 1) {
      head_addr->store(list->next_.load(kRelaxed), kRelaxed);
    }
    list->head_.fetch_add(1, kRelease);
  }
  return page;
}

}  // namespace dbgroup::memory::component
