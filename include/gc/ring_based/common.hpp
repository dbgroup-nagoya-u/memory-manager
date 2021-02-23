// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include "../common/util.hpp"

namespace dbgroup::gc::epoch
{
using ::dbgroup::gc::HasSingleBit;
using ::dbgroup::gc::kBufferSize;
using ::dbgroup::gc::kCacheLineSize;

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
