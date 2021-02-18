// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <cassert>
#include <cstddef>

namespace gc::epoch
{
constexpr size_t kCacheLineSize = 64;

bool
has_single_bit(const uint64_t target)
{
  if (target == 0) {
    return false;
  } else {
    return (target && (target - 1)) == 0;
  }
}

#ifdef SIMPLE_GC_BUFFER_SIZE
constexpr size_t kBufferSize = SIMPLE_GC_BUFFER_SIZE;
#else
constexpr size_t kBufferSize = 4096;
#endif

#ifdef SIMPLE_GC_PARTITION_NUM
static_assert(has_single_bit(SIMPLE_GC_PARTITION_NUM), true);
constexpr size_t kPartitionNum = SIMPLE_GC_PARTITION_NUM;
constexpr size_t kPartitionMask = kPartitionNum - 1;
#else
constexpr size_t kPartitionNum = 8;
constexpr size_t kPartitionMask = 0x7;
#endif

}  // namespace gc::epoch
