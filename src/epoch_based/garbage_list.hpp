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

  std::unique_ptr<GarbageList> next_;

  const std::unique_ptr<T> target_ptr_;

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

  ~GarbageList() {}

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
    return next_.get();
  }

  void
  SetNext(const GarbageList* next)
  {
    next_.reset(const_cast<GarbageList*>(next));
  }
};

}  // namespace gc::epoch
