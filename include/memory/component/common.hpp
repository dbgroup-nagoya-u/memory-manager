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

#ifndef MEMORY_COMPONENT_COMMON_HPP
#define MEMORY_COMPONENT_COMMON_HPP

// C++ standard libraries
#include <atomic>

// local sources
#include "memory/utility.hpp"

namespace dbgroup::memory
{
/// We do not use type checks in PMDK.
constexpr uint64_t kDefaultPMDKType = 0;

}  // namespace dbgroup::memory

#endif  // MEMORY_COMPONENT_COMMON_HPP
