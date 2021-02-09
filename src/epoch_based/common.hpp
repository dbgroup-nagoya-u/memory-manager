// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <cstddef>

namespace gc::epoch
{
constexpr size_t kCacheLineSize = 64;
constexpr size_t kBufferSize = 4096;
constexpr size_t kPartitionNum = 8;
constexpr size_t kPartitionMask = 0x7;
}  // namespace gc::epoch
