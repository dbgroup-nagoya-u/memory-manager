// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <limits>

#include "common.hpp"

namespace dbgroup::gc::tls
{
class Epoch
{
 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  std::atomic_size_t current_;

  std::atomic_size_t entered_;

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  Epoch() {}

  explicit Epoch(const size_t current_epoch)
      : current_{current_epoch}, entered_{std::numeric_limits<size_t>::max()}
  {
  }

  ~Epoch() {}

  Epoch(const Epoch &) = delete;
  Epoch &operator=(const Epoch &) = delete;
  Epoch(Epoch &&) = default;
  Epoch &operator=(Epoch &&) = default;

  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  void
  SetCurrentEpoch(const size_t current_epoch)
  {
    current_ = current_epoch;
  }

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  void
  EnterCurrentEpoch()
  {
    entered_ = current_.load();
  }

  void
  LeaveEpoch()
  {
    entered_ = std::numeric_limits<size_t>::max();
  }
};

}  // namespace dbgroup::gc::tls
