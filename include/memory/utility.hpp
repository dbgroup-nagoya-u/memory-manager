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
#if MEMORY_MANAGER_USE_MIMALLOC

/**
 * @brief A wrapper function to create an instance dynamically.
 *
 * This function is equivalent with "new".
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
  return new (mi_malloc_aligned(sizeof(T), alignof(T))) T{std::forward<Args>(args)...};
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
  return new (mi_malloc_aligned(size, alignof(T))) T{std::forward<Args>(args)...};
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
  return new (mi_zalloc_aligned(size, alignof(T))) T{std::forward<Args>(args)...};
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
  obj->~T();
  mi_free(obj);
}

/**
 * @brief An alias of allocators for container types.
 *
 * @tparam T a class to be contained.
 */
template <class T>
using STLAlloc = mi_stl_allocator<T>;

#elif MEMORY_MANAGER_USE_JEMALLOC

/**
 * @brief A wrapper function to create an instance dynamically.
 *
 * This function is equivalent with "new".
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
  return new (je_aligned_alloc(alignof(T), sizeof(T))) T{std::forward<Args>(args)...};
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
  return new (je_aligned_alloc(alignof(T), size)) T{std::forward<Args>(args)...};
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
  auto page = je_aligned_alloc(alignof(T), size);
  memset(page, 0, size);
  return new (page) T{std::forward<Args>(args)...};
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
  obj->~T();
  je_free(obj);
}

template <class T>
struct je_stl_allocator {
  typedef T value_type;
  typedef std::size_t size_type;
  typedef std::ptrdiff_t difference_type;
  typedef value_type& reference;
  typedef value_type const& const_reference;
  typedef value_type* pointer;
  typedef value_type const* const_pointer;
  template <class U>
  struct rebind {
    typedef je_stl_allocator<U> other;
  };

  je_stl_allocator() noexcept = default;

  je_stl_allocator(const je_stl_allocator&) noexcept = default;

  template <class U>
  je_stl_allocator(const je_stl_allocator<U>&) noexcept
  {
  }

  je_stl_allocator
  select_on_container_copy_construction() const
  {
    return *this;
  }

  void
  deallocate(T* p, size_type)
  {
    je_free(p);
  }

  [[nodiscard]] T*
  allocate(size_type count)
  {
    return static_cast<T*>(je_aligned_alloc(alignof(T), count * sizeof(T)));
  }

  [[nodiscard]] T*
  allocate(size_type count, const void*)
  {
    return allocate(count);
  }

  using propagate_on_container_copy_assignment = std::true_type;
  using propagate_on_container_move_assignment = std::true_type;
  using propagate_on_container_swap = std::true_type;
  using is_always_equal = std::true_type;

  template <class U, class... Args>
  void
  construct(U* p, Args&&... args)
  {
    ::new (p) U(std::forward<Args>(args)...);
  }

  template <class U>
  void
  destroy(U* p) noexcept
  {
    p->~U();
  }

  size_type
  max_size() const noexcept
  {
    return (PTRDIFF_MAX / sizeof(value_type));
  }

  pointer
  address(reference x) const
  {
    return &x;
  }

  const_pointer
  address(const_reference x) const
  {
    return &x;
  }
};

/**
 * @brief An alias of allocators for container types.
 *
 * @tparam T a class to be contained.
 */
template <class T>
using STLAlloc = je_stl_allocator<T>;

#else

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

#endif

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
