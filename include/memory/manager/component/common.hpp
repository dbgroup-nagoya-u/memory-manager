// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>

namespace dbgroup::memory::manager
{
constexpr std::memory_order mo_relax = std::memory_order_relaxed;

constexpr size_t kCacheLineSize = 64;

}  // namespace dbgroup::memory::manager
