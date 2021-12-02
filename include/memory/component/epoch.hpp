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

#include <atomic>
#include <limits>

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
  /*################################################################################################
   * Public constructors and assignment operators
   *##############################################################################################*/

  /**
   * @brief Construct a new instance with an initial epoch.
   *
   * @param current_epoch an initial epoch value.
   */
  constexpr explicit Epoch(const std::atomic_size_t &current_epoch)
      : current_{current_epoch}, entered_{std::numeric_limits<size_t>::max()}
  {
  }

  Epoch(const Epoch &) = delete;
  Epoch &operator=(const Epoch &) = delete;
  Epoch(Epoch &&orig) = delete;
  Epoch &operator=(Epoch &&orig) = delete;

  /*################################################################################################
   * Public destructors
   *##############################################################################################*/

  /**
   * @brief Destroy the Epoch object.
   *
   */
  ~Epoch() = default;

  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  /**
   * @return size_t a current epoch value.
   */
  [[nodiscard]] size_t
  GetCurrentEpoch() const
  {
    return current_.load(kMORelax);
  }

  /**
   * @return size_t a protected epoch value.
   */
  [[nodiscard]] size_t
  GetProtectedEpoch() const
  {
    return entered_.load(kMORelax);
  }

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  /**
   * @brief Keep a current epoch value to protect new garbages.
   *
   */
  void
  EnterEpoch()
  {
    entered_.store(GetCurrentEpoch(), kMORelax);
  }

  /**
   * @brief Release a protected epoch value to allow GC to delete old garbages.
   *
   */
  void
  LeaveEpoch()
  {
    entered_.store(std::numeric_limits<size_t>::max(), kMORelax);
  }

 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  /// a current epoch.
  const std::atomic_size_t &current_;

  /// a snapshot to denote a protected epoch.
  std::atomic_size_t entered_;
};

}  // namespace dbgroup::memory::component

#endif  // MEMORY_COMPONENT_EPOCH_HPP
