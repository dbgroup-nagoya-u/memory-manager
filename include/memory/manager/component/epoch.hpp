// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

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

  std::atomic_size_t current_;

  std::atomic_size_t entered_;

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  constexpr Epoch() : current_{0}, entered_{std::numeric_limits<size_t>::max()} {}

  explicit Epoch(const size_t current_epoch)
      : current_{current_epoch}, entered_{std::numeric_limits<size_t>::max()}
  {
  }

  ~Epoch() = default;

  Epoch(const Epoch &) = delete;
  Epoch &operator=(const Epoch &) = delete;

  Epoch(Epoch &&orig)
  {
    this->current_.store(orig.current_.load(mo_relax), mo_relax);
    this->entered_.store(orig.entered_.load(mo_relax), mo_relax);
  }

  Epoch &
  operator=(Epoch &&orig)
  {
    this->current_.store(orig.current_.load(mo_relax), mo_relax);
    this->entered_.store(orig.entered_.load(mo_relax), mo_relax);

    return *this;
  }

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

  void
  SetCurrentEpoch(const size_t current_epoch)
  {
    current_.store(current_epoch, mo_relax);
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
