// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include "epoch.hpp"

namespace dbgroup::gc::tls
{
class EpochGuard
{
 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  Epoch *epoch_;

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  explicit EpochGuard(Epoch *epoch) : epoch_{epoch} { epoch_->EnterCurrentEpoch(); }

  ~EpochGuard() { epoch_->LeaveEpoch(); }

  EpochGuard(const EpochGuard &) = delete;
  EpochGuard &operator=(const EpochGuard &) = delete;
  EpochGuard(EpochGuard &&) = default;
  EpochGuard &operator=(EpochGuard &&) = default;
};

}  // namespace dbgroup::gc::tls
