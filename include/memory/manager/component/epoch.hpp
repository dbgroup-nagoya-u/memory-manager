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

#pragma once

#include <atomic>
#include <limits>

#include "common.hpp"

namespace dbgroup::memory::manager::component
{
class Epoch
{
 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  std::atomic_size_t &current_;

  std::atomic_size_t entered_;

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  constexpr explicit Epoch(std::atomic_size_t &current_epoch)
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

  size_t
  GetCurrentEpoch() const
  {
    return current_.load(mo_relax);
  }

  size_t
  GetProtectedEpoch() const
  {
    return entered_.load(mo_relax);
  }

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  void
  EnterEpoch()
  {
    entered_.store(current_.load(mo_relax), mo_relax);
  }

  void
  LeaveEpoch()
  {
    entered_.store(std::numeric_limits<size_t>::max(), mo_relax);
  }
};

}  // namespace dbgroup::memory::manager::component
