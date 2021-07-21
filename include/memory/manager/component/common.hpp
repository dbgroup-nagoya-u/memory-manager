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

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <utility>

#ifdef MEMORY_MANAGER_USE_MIMALLOC
#include <mimalloc.h>
#endif

namespace dbgroup::memory::manager
{
constexpr std::memory_order mo_relax = std::memory_order_relaxed;

constexpr size_t kCacheLineSize = 64;

constexpr size_t kGarbageBufferSize = 1024;

#ifdef MEMORY_MANAGER_USE_MIMALLOC

template <class T, class... Args>
T*
Create(Args&&... args)
{
  auto page = mi_malloc_aligned(sizeof(T), alignof(T));
  return new (std::move(page)) T{args...};
}

template <class T>
void
Delete(T* obj)
{
  obj->~T();
  mi_free_aligned(obj, alignof(T));
}
#else

template <class T, class... Args>
T*
Create(Args&&... args)
{
  return new T{args...};
}

template <class T>
void
Delete(T* obj)
{
  delete obj;
}
#endif

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

}  // namespace dbgroup::memory::manager
