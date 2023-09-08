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

#ifndef TEST_COMMON_HPP
#define TEST_COMMON_HPP

// C++ standard libraries
#include <filesystem>
#include <limits>

// external sources
#include "gtest/gtest.h"

// local sources
#include "memory/utility.hpp"

namespace dbgroup::memory
{
// utility macros for expanding compile definitions as std::string
#define DBGROUP_ADD_QUOTES_INNER(x) #x                     // NOLINT
#define DBGROUP_ADD_QUOTES(x) DBGROUP_ADD_QUOTES_INNER(x)  // NOLINT

constexpr size_t kULMax = std::numeric_limits<size_t>::max();

constexpr size_t kThreadNum = DBGROUP_TEST_THREAD_NUM;

#ifdef MEMORY_MANAGER_USE_PERSISTENT_MEMORY
constexpr std::string_view kTmpPMEMPath = DBGROUP_ADD_QUOTES(DBGROUP_TEST_TMP_PMEM_PATH);

const std::string_view use_name = std::getenv("USER");

inline auto
GetTmpPoolPath()  //
    -> std::filesystem::path
{
  std::filesystem::path pool_path{kTmpPMEMPath};
  pool_path /= use_name;
  pool_path /= "tmp_test_dir";

  return pool_path;
}

class TmpDirManager : public ::testing::Environment
{
 public:
  // constructor/destructor
  TmpDirManager() = default;
  TmpDirManager(const TmpDirManager &) = default;
  TmpDirManager(TmpDirManager &&) = default;
  TmpDirManager &operator=(const TmpDirManager &) = default;
  TmpDirManager &operator=(TmpDirManager &&) = default;
  ~TmpDirManager() override = default;

  // Override this to define how to set up the environment.
  void
  SetUp() override
  {
    // check the specified path
    if (kTmpPMEMPath.empty() || !std::filesystem::exists(kTmpPMEMPath)) {
      std::cerr << "WARN: The correct path to persistent memory is not set." << std::endl;
      GTEST_SKIP();
    }

    // prepare a temporary directory for testing
    const auto &pool_path = GetTmpPoolPath();
    std::filesystem::remove_all(pool_path);
    std::filesystem::create_directories(pool_path);
  }

  // Override this to define how to tear down the environment.
  void
  TearDown() override
  {
    const auto &pool_path = GetTmpPoolPath();
    std::filesystem::remove_all(pool_path);
  }
};
#endif

}  // namespace dbgroup::memory

#endif  // TEST_COMMON_HPP
