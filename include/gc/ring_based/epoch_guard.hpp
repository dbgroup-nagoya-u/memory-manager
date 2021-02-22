// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include "epoch_manager.hpp"

namespace dbgroup::gc::epoch
{
class EpochGuard
{
 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  size_t current_epoch_;

  size_t partition_id_;

  EpochManager *epoch_manger_;

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  explicit EpochGuard(EpochManager *epoch_manger)
  {
    std::tie(current_epoch_, partition_id_) = epoch_manger->EnterEpoch();
    epoch_manger_ = epoch_manger;
  }

  ~EpochGuard() { epoch_manger_->LeaveEpoch(current_epoch_, partition_id_); }

  EpochGuard(const EpochGuard &) = delete;
  EpochGuard &operator=(const EpochGuard &) = delete;
  EpochGuard(EpochGuard &&) = default;
  EpochGuard &operator=(EpochGuard &&) = default;

  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  constexpr size_t
  GetEpoch() const
  {
    return current_epoch_;
  }

  constexpr size_t
  GetPartitionId() const
  {
    return partition_id_;
  }
};

}  // namespace dbgroup::gc::epoch
