// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>

namespace dbgroup::memory
{
constexpr std::memory_order mo_relax = std::memory_order_relaxed;
constexpr size_t kCacheLineSize = 64;

#ifdef INITIAL_GARBAGE_LIST_CAPACITY
constexpr size_t kGarbageListCapacity = INITIAL_GARBAGE_LIST_CAPACITY;
#else
constexpr size_t kGarbageListCapacity = 256;
#endif

constexpr bool
HasSingleBit(const uint64_t target)
{
  if (target == 0UL) {
    return false;
  } else {
    return (target & (target - 1UL)) == 0UL;
  }
}

}  // namespace dbgroup::memory
