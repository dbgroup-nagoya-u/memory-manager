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
#include "memory/component/garbage_list.hpp"

// C++ standard libraries
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <utility>

// local sources
#include "memory/utility.hpp"

namespace dbgroup::memory::component
{
/*##############################################################################
 * Public APIs
 *############################################################################*/

auto
GarbageList::Empty() const  //
    -> bool
{
  const auto end_pos = end_pos_.load(kAcquire);
  const auto size = end_pos - begin_pos_.load(kRelaxed);
  return (size == 0) && (end_pos < kGarbageBufSize);
}

auto
GarbageList::GetNext(                      //
    std::atomic<GarbageList *> *buf_addr)  //
    -> std::pair<bool, GarbageList *>
{
  auto next = reinterpret_cast<uintptr_t>(buf_addr->load(kRelaxed));
  return {(next & kUsedFlag) > 0, reinterpret_cast<GarbageList *>(next & ~kUsedFlag)};
}

void
GarbageList::AddGarbage(  //
    std::atomic<GarbageList *> *buf_addr,
    const size_t epoch,
    void *garbage)
{
  auto *buf = buf_addr->load(kRelaxed);

  const auto pos = buf->end_pos_.load(kRelaxed);
  buf->garbage_.at(pos) = {epoch, garbage};
  if (pos >= kGarbageBufSize - 1) {
    auto *new_tail = new GarbageList{};
    buf->next_.store(new_tail, kRelaxed);
    buf_addr->store(new_tail, kRelaxed);
  }
  buf->end_pos_.fetch_add(1, kRelease);
}

auto
GarbageList::ReusePage(                    //
    std::atomic<GarbageList *> *buf_addr)  //
    -> void *
{
  auto *buf = buf_addr->load(kRelaxed);

  const auto pos = buf->begin_pos_.load(kRelaxed);
  const auto mid_pos = buf->mid_pos_.load(kAcquire);
  if (pos >= mid_pos) return nullptr;  // there is no reusable page

  // get a reusable page
  buf->begin_pos_.fetch_add(1, kRelaxed);
  auto *page = buf->garbage_.at(pos).ptr;

  // check whether all the pages in the list are reused
  if (pos >= kGarbageBufSize - 1) {
    auto *next = buf->next_.load(kRelaxed);
    while (!buf->next_.compare_exchange_weak(
        next, reinterpret_cast<GarbageList *>(reinterpret_cast<uintptr_t>(next) | kUsedFlag),
        kRelaxed, kRelaxed)) {
      // continue until the next list is reserved
    }
    buf_addr->store(next, kRelaxed);
  }

  return page;
}

}  // namespace dbgroup::memory::component
