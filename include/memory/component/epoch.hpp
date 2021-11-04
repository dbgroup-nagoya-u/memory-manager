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

#ifndef MEMORY_MANAGER_MEMORY_COMPONENT_EPOCH_H_
#define MEMORY_MANAGER_MEMORY_COMPONENT_EPOCH_H_

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
 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  /// a current epoch. Note: this is maintained individually to improve performance.
  std::atomic_size_t current_;

  /// a snapshot to denote a protected epoch.
  std::atomic_size_t entered_;

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  /**
   * @brief Construct a new instance.
   *
   */
  constexpr Epoch() : current_{}, entered_{std::numeric_limits<size_t>::max()} {}

  /**
   * @brief Construct a new instance with an initial epoch.
   *
   * @param current_epoch an initial epoch value.
   */
  constexpr explicit Epoch(const size_t current_epoch)
      : current_{current_epoch}, entered_{std::numeric_limits<size_t>::max()}
  {
  }

  ~Epoch() = default;

  Epoch(const Epoch &) = delete;
  Epoch &operator=(const Epoch &) = delete;
  Epoch(Epoch &&orig) = delete;
  Epoch &operator=(Epoch &&orig) = delete;

  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  /**
   * @return size_t a current epoch value.
   */
  size_t
  GetCurrentEpoch() const
  {
    return current_.load(mo_relax);
  }

  /**
   * @return size_t a protected epoch value.
   */
  size_t
  GetProtectedEpoch() const
  {
    return entered_.load(mo_relax);
  }

  /**
   * @brief Set a current epoch value.
   *
   * @param current_epoch a epoch value to be set.
   */
  void
  SetCurrentEpoch(const size_t current_epoch)
  {
    current_.store(current_epoch, mo_relax);
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
    entered_.store(current_.load(mo_relax), mo_relax);
  }

  /**
   * @brief Release a protected epoch value to allow GC to delete old garbages.
   *
   */
  void
  LeaveEpoch()
  {
    entered_.store(std::numeric_limits<size_t>::max(), mo_relax);
  }
};

}  // namespace dbgroup::memory::component

#endif  // MEMORY_MANAGER_MEMORY_COMPONENT_EPOCH_H_
