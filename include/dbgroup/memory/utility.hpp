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

#ifndef MEMORY_MANAGER_DBGROUP_MEMORY_UTILITY_HPP_
#define MEMORY_MANAGER_DBGROUP_MEMORY_UTILITY_HPP_

// C++ standard libraries
#include <atomic>
#include <bit>
#include <cstddef>
#include <cstring>
#include <new>

// external libraries
#include "dbgroup/constants.hpp"

namespace dbgroup::memory
{
/*############################################################################*
 * Global constants
 *############################################################################*/

/// @brief The default time interval for garbage collection (10 ms).
constexpr size_t kDefaultGCTime = 10000;

/// @brief The default number of worker threads for garbage collection.
constexpr size_t kDefaultGCThreadNum = 1;

/// @brief The default number of worker threads for garbage collection.
constexpr size_t kDefaultReusePageCapacity = 32;

/// @brief The default alignment size for dynamically allocated instances.
constexpr size_t kDefaultAlignment = __STDCPP_DEFAULT_NEW_ALIGNMENT__;

/*############################################################################*
 * Utility classes
 *############################################################################*/

/**
 * @brief A default GC information.
 *
 */
struct DefaultTarget {
  /// @brief Use the void type to avoid calling a destructor.
  using T = void;

  /// @brief Do not reuse pages after GC (release immediately).
  static constexpr bool kReusePages = false;
};

/**
 * @brief A dummy struct for representing fixed-length pages.
 *
 * @tparam kPageSize A target page size.
 */
template <size_t kPageSize>
struct alignas(kPageSize < kVMPageSize ? kPageSize : kVMPageSize) PageTarget {
  /// @brief Call a self destructor (zero filling).
  using T = PageTarget;

  /// @brief Reuse pages after GC.
  static constexpr bool kReusePages = true;

  constexpr PageTarget() noexcept = default;

  constexpr PageTarget(const PageTarget &) noexcept = default;
  constexpr PageTarget(PageTarget &&) noexcept = default;

  constexpr auto operator=(const PageTarget &) noexcept -> PageTarget & = default;
  constexpr auto operator=(PageTarget &&) noexcept -> PageTarget & = default;

  /// @brief Fill this page with zeros.
  ~PageTarget() { std::memset(std::bit_cast<void *>(this), 0, kPageSize); }

  /// @brief A data buffer.
  std::byte buf[kPageSize]{};
};

/*############################################################################*
 * Utility functions
 *############################################################################*/

/**
 * @brief Allocate a memory region with alignments.
 *
 * @tparam T A target class.
 * @param size The size of target class.
 * @return The address of an allocated one.
 */
template <class T>
inline auto
Allocate(                     //
    size_t size = sizeof(T))  //
    -> T *
{
  if constexpr (alignof(T) <= kDefaultAlignment) {
    return std::bit_cast<T *>(::operator new(size));
  } else {
    return std::bit_cast<T *>(::operator new(size, static_cast<std::align_val_t>(alignof(T))));
  }
}

/**
 * @brief A deleter function to release aligned pages.
 *
 * @tparam T A target class.
 * @param ptr The address of allocations to be released.
 */
template <class T = void>
inline void
Release(  //
    void *ptr)
{
  if constexpr (std::is_same_v<T, void>) {
    ::operator delete(ptr);
  } else {
    if constexpr (alignof(T) <= kDefaultAlignment) {
      ::operator delete(ptr);
    } else {
      ::operator delete(ptr, static_cast<std::align_val_t>(alignof(T)));
    }
  }
}

}  // namespace dbgroup::memory

#endif  // MEMORY_MANAGER_DBGROUP_MEMORY_UTILITY_HPP_
