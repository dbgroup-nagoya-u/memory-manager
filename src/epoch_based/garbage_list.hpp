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

  GarbageList* next_;

  const T* target_ptr_;

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  explicit GarbageList(  //
      const T* target_ptr,
      const GarbageList* next = nullptr)
      : next_{const_cast<GarbageList*>(next)}, target_ptr_{const_cast<T*>(target_ptr)}
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

  void
  SetNext(const GarbageList* next)
  {
    next_ = const_cast<GarbageList*>(next);
  }
};

}  // namespace gc::epoch
