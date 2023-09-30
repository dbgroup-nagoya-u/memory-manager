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

// C++ standard libraries
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

/// The default alignment size for dynamically allocated instances.
constexpr size_t kDefaultAlignment = __STDCPP_DEFAULT_NEW_ALIGNMENT__;

/*######################################################################################
 * Turning parameters
 *####################################################################################*/

/// The page size of virtual memory addresses.
constexpr size_t kVMPageSize = 4096;

/// The size of words.
constexpr size_t kWordSize = 8;

/// The expected cache-line size.
constexpr size_t kCashLineSize = 64;

#ifdef MEMORY_MANAGER_USE_PERSISTENT_MEMORY
/*######################################################################################
 * Constants for persistent memory
 *####################################################################################*/

/// In PMDK, the memblock header use 16 bytes
constexpr size_t kPmemPageSize = kVMPageSize - 16;
#endif

/*######################################################################################
 * Utility functions
 *####################################################################################*/

/**
 * @brief Allocate a memory region with alignments.
 *
 * @tparam T A target class.
 * @param size The size of target class.
 * @return The address of an allocated one.
 */
template <class T>
inline auto
Allocate(size_t size = sizeof(T))  //
    -> T *
{
  if constexpr (alignof(T) <= kDefaultAlignment) {
    return reinterpret_cast<T *>(::operator new(size));
  } else {
    return reinterpret_cast<T *>(::operator new(size, static_cast<std::align_val_t>(alignof(T))));
  }
}

/**
 * @brief A deleter function to release aligned pages.
 *
 * @tparam T A target class.
 * @param ptr The address of allocations to be released.
 */
template <class T>
inline void
Release(void *ptr)
{
  if constexpr (alignof(T) <= kDefaultAlignment) {
    ::operator delete(ptr);
  } else {
    ::operator delete(ptr, static_cast<std::align_val_t>(alignof(T)));
  }
}

}  // namespace dbgroup::memory

#endif  // MEMORY_UTILITY_HPP
