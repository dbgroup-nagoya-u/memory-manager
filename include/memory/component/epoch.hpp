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

#ifndef MEMORY_COMPONENT_EPOCH_HPP
#define MEMORY_COMPONENT_EPOCH_HPP

// C++ standard libraries
#include <atomic>
#include <limits>
#include <optional>

// local sources
#include "common.hpp"

namespace dbgroup::memory::component
{
/**
 * @brief A class to represent epochs for epoch-based garbage collection.
 *
 */
class Epoch
{
 public:
  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  /**
   * @brief Construct a new instance.
   *
   */
  constexpr Epoch() = default;

  Epoch(const Epoch &) = delete;
  auto operator=(const Epoch &) -> Epoch & = delete;
  Epoch(Epoch &&orig) = delete;
  auto operator=(Epoch &&orig) -> Epoch & = delete;

  /*####################################################################################
   * Public destructors
   *##################################################################################*/

  /**
   * @brief Destroy the Epoch object.
   *
   */
  ~Epoch() = default;

  /*####################################################################################
   * Public getters/setters
   *##################################################################################*/

  /**
   * @return size_t a current epoch value.
   */
  [[nodiscard]] auto
  GetCurrentEpoch() const  //
      -> size_t
  {
    return current_->load(std::memory_order_acquire);
  }

  /**
   * @return size_t a protected epoch value.
   */
  [[nodiscard]] auto
  GetProtectedEpoch() const  //
      -> size_t
  {
    return entered_.load(std::memory_order_relaxed);
  }

  /**
   * @param global_epoch a pointer to the global epoch.
   */
  void
  SetGrobalEpoch(std::atomic_size_t *global_epoch)
  {
    current_ = global_epoch;
  }

  /*####################################################################################
   * Public utility functions
   *##################################################################################*/

  /**
   * @brief Keep a current epoch value to protect new garbages.
   *
   */
  void
  EnterEpoch()
  {
    entered_.store(GetCurrentEpoch(), std::memory_order_relaxed);
  }

  /**
   * @brief Release a protected epoch value to allow GC to delete old garbages.
   *
   */
  void
  LeaveEpoch()
  {
    entered_.store(std::numeric_limits<size_t>::max(), std::memory_order_relaxed);
  }

 private:
  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// A current epoch.
  std::atomic_size_t *current_{nullptr};

  /// A snapshot to denote a protected epoch.
  std::atomic_size_t entered_{std::numeric_limits<size_t>::max()};
};

}  // namespace dbgroup::memory::component

#endif  // MEMORY_COMPONENT_EPOCH_HPP
