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

#include "epoch.hpp"

namespace dbgroup::memory::component
{
/**
 * @brief A class to protect epochs based on the scoped locking pattern.
 *
 */
class EpochGuard
{
 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  /// a reference to a target epoch.
  Epoch *epoch_;

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  /**
   * @brief Construct a new instance and protect a current epoch.
   *
   * @param epoch a reference to a target epoch.
   */
  explicit EpochGuard(Epoch *epoch) : epoch_{epoch} { epoch_->EnterEpoch(); }

  /**
   * @brief Destroy the instace and release a protected epoch.
   *
   */
  ~EpochGuard() { epoch_->LeaveEpoch(); }

  EpochGuard(const EpochGuard &) = delete;
  EpochGuard &operator=(const EpochGuard &) = delete;
  constexpr EpochGuard(EpochGuard &&) = default;
  constexpr EpochGuard &operator=(EpochGuard &&) = default;
};

}  // namespace dbgroup::memory::component
