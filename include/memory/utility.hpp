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

#ifndef MEMORY_MANAGER_MEMORY_UTILITY_H_
#define MEMORY_MANAGER_MEMORY_UTILITY_H_

#include <cassert>
#include <cstddef>
#include <cstdint>

namespace dbgroup::memory
{
#ifdef MEMORY_MANAGER_GARBAGE_BUFFER_SIZE
/// an initial buffer size for retaining garbages
constexpr size_t kGarbageBufferSize = MEMORY_MANAGER_GARBAGE_BUFFER_SIZE;
#else
/// an initial buffer size for retaining garbages
constexpr size_t kGarbageBufferSize = 1024;
#endif

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

}  // namespace dbgroup::memory

#endif  // MEMORY_MANAGER_MEMORY_UTILITY_H_
