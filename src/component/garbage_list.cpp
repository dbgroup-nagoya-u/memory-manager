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
#include "dbgroup/memory/component/garbage_list.hpp"

// C++ standard libraries
#include <array>
#include <atomic>
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
GarbageList::AddGarbage(  //
    std::atomic<GarbageList *> *tail_addr,
    const size_t epoch,
    void *garbage)
{
  auto *list = tail_addr->load(kAcquire);
  const auto tail = list->tail_.load(kRelaxed);
  list->garbage_[tail] = {epoch, garbage};
  if (tail >= kGarbageListCapacity - 1) {
    auto *next = new GarbageList{};
    tail_addr->store(next, kRelease);
    list->next_ = next;
  }
  list->tail_.fetch_add(1, kRelease);
}

}  // namespace dbgroup::memory::component
