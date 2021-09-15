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

#pragma once

#include <memory_resource>
#include <utility>

#include "component/common.hpp"

#if MEMORY_MANAGER_USE_MIMALLOC
#include <mimalloc.h>
#elif MEMORY_MANAGER_USE_JEMALLOC
#include <jemalloc/jemalloc_without_override.h>
#include <string.h>
#else
#include <stdlib.h>
#endif

namespace dbgroup::memory
{
/**
 * @brief A wrapper function to create an instance dynamically.
 *
 * @tparam T a class to be created.
 * @tparam Args variadic templates.
 * @param args arguments for a constructor.
 * @return T* a pointer to a created instance.
 */
template <class T, class... Args>
T*
New(Args&&... args)
{
  return new T{std::forward<Args>(args)...};
}

/**
 * @brief A wrapper function to create an instance dynamically.
 *
 * This function is equivalent with "malloc + new".
 *
 * @tparam T a class to be created.
 * @tparam Args variadic templates.
 * @param size the size to be allocated.
 * @param args arguments for a constructor.
 * @return T* a pointer to a created instance.
 */
template <class T, class... Args>
T*
MallocNew(  //
    const size_t size,
    Args&&... args)
{
  return new (malloc(size)) T{std::forward<Args>(args)...};
}

/**
 * @brief A wrapper function to create an instance dynamically.
 *
 * This function is equivalent with "calloc + new".
 *
 * @tparam T a class to be created.
 * @tparam Args variadic templates.
 * @param size the size to be allocated.
 * @param args arguments for a constructor.
 * @return T* a pointer to a created instance.
 */
template <class T, class... Args>
T*
CallocNew(  //
    const size_t size,
    Args&&... args)
{
  return new (calloc(1, size)) T{std::forward<Args>(args)...};
}

/**
 * @brief A wrapper function to delete an dynamically created instance.
 *
 * This function is equivalent with "delete".

 * @tparam T a class to be deleted.
 * @param obj a target instance.
 */
template <class T>
void
Delete(T* obj)
{
  delete obj;
}

/**
 * @brief An alias of allocators for container types.
 *
 * @tparam T a class to be contained.
 */
template <class T>
using STLAlloc = std::pmr::polymorphic_allocator<T>;

/**
 * @brief A wrapper of a deleter class for unique_ptr/shared_ptr.
 *
 * @tparam T a class to be deleted by this deleter.
 */
template <class T>
struct Deleter {
  constexpr Deleter() noexcept = default;

  template <class Up, typename = typename std::enable_if_t<std::is_convertible_v<Up*, T*>>>
  Deleter(const Deleter<Up>&) noexcept
  {
  }

  void
  operator()(T* ptr) const
  {
    static_assert(!std::is_void_v<T>, "can't delete pointer to incomplete type");
    static_assert(sizeof(T) > 0, "can't delete pointer to incomplete type");

    Delete(ptr);
  }
};

}  // namespace dbgroup::memory
