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
  const auto end_pos = end_pos_.load(std::memory_order_acquire);
  const auto size = end_pos - begin_pos_.load(std::memory_order_relaxed);

  return (size == 0) && (end_pos < kGarbageBufSize);
}

void
GarbageList::AddGarbage(  //
    GarbageList **buf_addr,
    const size_t epoch,
    void *garbage)
{
  auto *buf = *buf_addr;
  const auto pos = buf->end_pos_.load(std::memory_order_relaxed);

  // insert a new garbage
  buf->garbage_.at(pos).epoch = epoch;
  buf->garbage_.at(pos).ptr = garbage;

  // check whether the list is full
  if (pos >= kGarbageBufSize - 1) {
    auto *new_tail = new GarbageList{};
    buf->next_ = new_tail;
    *buf_addr = new_tail;
  }

  // increment the end position
  buf->end_pos_.fetch_add(1, std::memory_order_release);
}

auto
GarbageList::ReusePage(                    //
    std::atomic<GarbageList *> *buf_addr)  //
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
  if (pos >= kGarbageBufSize - 1) {
    buf_addr->store(buf->next_, std::memory_order_relaxed);
    delete buf;
  }

  return page;
}

}  // namespace dbgroup::memory::component
