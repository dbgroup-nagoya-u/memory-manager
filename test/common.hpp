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

#ifndef TEST_COMMON_HPP  // NOLINT
#define TEST_COMMON_HPP

#include <limits>

#include "memory/utility.hpp"

namespace dbgroup::memory::test
{
constexpr size_t kULMax = std::numeric_limits<size_t>::max();

#ifdef MEMORY_MANAGER_TEST_THREAD_NUM
constexpr size_t kThreadNum = MEMORY_MANAGER_TEST_THREAD_NUM;
#else
constexpr size_t kThreadNum = 1;
#endif

}  // namespace dbgroup::memory::test

#endif  // TEST_COMMON_HPP
