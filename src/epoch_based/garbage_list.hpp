// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <memory>

#include "epoch_based/common.hpp"

namespace gc::epoch
{
template <class T>
class GarbageList
{
 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  const GarbageList* next_;

  T* target_ptr_;

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  explicit GarbageList(  //
      const GarbageList* next,
      const T* target_ptr)
      : next_{next}, target_ptr_{target_ptr}
  {
  }

  ~GarbageList()
  {
    if (target_ptr_ != nullptr) {
      delete target_ptr_;
    }
  }

  GarbageList(const GarbageList&) = delete;
  GarbageList& operator=(const GarbageList&) = delete;
  GarbageList(GarbageList&&) = default;
  GarbageList& operator=(GarbageList&&) = default;

  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  constexpr GarbageList*
  Next() const
  {
    return next_;
  }
};

}  // namespace gc::epoch
