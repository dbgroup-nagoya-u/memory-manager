// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <cassert>
#include <cstddef>

namespace dbgroup::gc::epoch
{
constexpr size_t kCacheLineSize = 64;

constexpr bool
HasSingleBit(const uint64_t target)
{
  if (target == 0UL) {
    return false;
  } else {
    return (target & (target - 1UL)) == 0UL;
  }
}

#ifdef BUFFER_SIZE
constexpr size_t kBufferSize = BUFFER_SIZE;
#else
constexpr size_t kBufferSize = 4096;
#endif

#ifdef PARTITION_NUM
static_assert(HasSingleBit(PARTITION_NUM));
constexpr size_t kPartitionNum = PARTITION_NUM;
constexpr size_t kPartitionMask = kPartitionNum - 1;
#else
constexpr size_t kPartitionNum = 8;
constexpr size_t kPartitionMask = 0x7;
#endif

#ifdef INITIAL_GARBAGE_LIST_CAPACITY
constexpr size_t kGarbageListCapacity = INITIAL_GARBAGE_LIST_CAPACITY;
#else
constexpr size_t kGarbageListCapacity = 128;
#endif

}  // namespace dbgroup::gc::epoch
