// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include "epoch_based/epoch_manager.hpp"

namespace gc::epoch
{
class EpochGuard
{
 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  size_t current_epoch_;

  EpochManager *epoch_manger_;

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  explicit EpochGuard(EpochManager *epoch_manger)
  {
    current_epoch_ = epoch_manger->EnterEpoch();
    epoch_manger_ = epoch_manger;
  }

  ~EpochGuard() { epoch_manger_->LeaveEpoch(current_epoch_); }

  EpochGuard(const EpochGuard &) = delete;
  EpochGuard &operator=(const EpochGuard &) = delete;
  EpochGuard(EpochGuard &&) = default;
  EpochGuard &operator=(EpochGuard &&) = default;
};

}  // namespace gc::epoch
