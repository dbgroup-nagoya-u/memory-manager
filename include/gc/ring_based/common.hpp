// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include "gc/common/util.hpp"

namespace dbgroup::memory::ring_buffer_based
{
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

}  // namespace dbgroup::memory::ring_buffer_based
