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

#ifndef MEMORY_UTILITY_HPP
#define MEMORY_UTILITY_HPP

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <type_traits>

namespace dbgroup::memory
{
/*######################################################################################
 * Global constants
 *####################################################################################*/

/// The default time interval for garbage collection [us].
constexpr size_t kDefaultGCTime = 10000;  // 10 ms

/// The default number of worker threads for garbage collection.
constexpr size_t kDefaultGCThreadNum = 1;

/*######################################################################################
 * Turning parameters
 *####################################################################################*/

/// The size of buffers for retaining garbages.
constexpr size_t kGarbageBufferSize = MEMORY_MANAGER_GARBAGE_BUFFER_SIZE;

/// The expected number of worker threads.
constexpr size_t kExpectedThreadNum = MEMORY_MANAGER_EXPECTED_THREAD_NUM;

/// The expected cache-line size.
constexpr size_t kCashLineSize = 64;

}  // namespace dbgroup::memory

#endif  // MEMORY_UTILITY_HPP
