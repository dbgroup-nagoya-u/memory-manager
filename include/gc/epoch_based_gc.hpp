// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <array>
#include <atomic>
#include <thread>
#include <vector>

#include "epoch_based/common.hpp"
#include "epoch_based/epoch_guard.hpp"
#include "epoch_based/epoch_manager.hpp"
#include "epoch_based/garbage_list.hpp"

namespace dbagroup::gc
{
using epoch::EpochGuard;
using epoch::EpochManager;
using epoch::GarbageList;
using epoch::kBufferSize;
using epoch::kCacheLineSize;
using epoch::kPartitionMask;
using epoch::kPartitionNum;

template <class T>
class alignas(kCacheLineSize) EpochBasedGC
{
  static constexpr size_t kDefaultGCIntervalMicroSec = 100000;

 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  std::array<std::array<uintptr_t, kPartitionNum>, kBufferSize> garbage_ring_buffer_;

  EpochManager epoch_manager_;

  const size_t gc_interval_micro_sec_;

  std::thread gc_thread_;

  std::atomic_bool gc_is_running_;

  /*################################################################################################
   * Internal utility functions
   *##############################################################################################*/

  void
  DeleteGarbages(  //
      const size_t begin_epoch,
      const size_t end_epoch)
  {
    for (auto epoch = begin_epoch;                            //
         epoch != end_epoch;                                  //
         epoch = (epoch == kBufferSize - 1) ? 0 : epoch + 1)  //
    {
      for (size_t partition = 0; partition < kPartitionNum; ++partition) {
        auto garbage_list =
            reinterpret_cast<GarbageList<T>*>(garbage_ring_buffer_[epoch][partition]);
        garbage_list->Clear();
      }
    }
  }

  void
  RunGC()
  {
    while (gc_is_running_) {
      // wait for garbages to be out of scope
      std::this_thread::sleep_for(std::chrono::microseconds(gc_interval_micro_sec_));
      epoch_manager_.ForwardEpoch();
      // delete freeable garbages
      const auto [begin_epoch, end_epoch] = epoch_manager_.ListFreeableEpoch();
      DeleteGarbages(begin_epoch, end_epoch);
    }
  }

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  explicit EpochBasedGC(  //
      const size_t gc_interval_micro_sec = kDefaultGCIntervalMicroSec,
      const bool start_gc = true)
      : epoch_manager_{}, gc_interval_micro_sec_{gc_interval_micro_sec}, gc_is_running_{false}
  {
    for (size_t epoch = 0; epoch < kBufferSize; ++epoch) {
      for (size_t partition = 0; partition < kPartitionNum; ++partition) {
        garbage_ring_buffer_[epoch][partition] = reinterpret_cast<uintptr_t>(new GarbageList<T>{});
      }
    }
    if (start_gc) {
      StartGC();
    }
  }

  ~EpochBasedGC()
  {
    // stop garbage collection
    StopGC();
    epoch_manager_.ForwardEpoch();

    const auto current_epoch = epoch_manager_.GetCurrentEpoch();
    size_t begin_epoch, end_epoch;
    do {
      // wait for garbages to be out of scope
      std::this_thread::sleep_for(std::chrono::microseconds(gc_interval_micro_sec_));
      // delete freeable garbages
      std::tie(begin_epoch, end_epoch) = epoch_manager_.ListFreeableEpoch();
    } while (end_epoch != current_epoch);

    for (size_t epoch = 0; epoch < kBufferSize; ++epoch) {
      for (size_t partition = 0; partition < kPartitionNum; ++partition) {
        auto garbage_list =
            reinterpret_cast<GarbageList<T>*>(garbage_ring_buffer_[epoch][partition]);
        delete garbage_list;
      }
    }
  }

  EpochBasedGC(const EpochBasedGC&) = delete;
  EpochBasedGC& operator=(const EpochBasedGC&) = delete;
  EpochBasedGC(EpochBasedGC&&) = default;
  EpochBasedGC& operator=(EpochBasedGC&&) = default;

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  EpochGuard
  CreateEpochGuard()
  {
    return EpochGuard{&epoch_manager_};
  }

  void
  AddGarbage(const T* target_ptr)
  {
    const auto epoch = epoch_manager_.GetCurrentEpoch();
    const auto partition =
        std::hash<std::thread::id>()(std::this_thread::get_id()) & kPartitionMask;

    auto garbage_list = reinterpret_cast<GarbageList<T>*>(garbage_ring_buffer_[epoch][partition]);
    garbage_list->AddGarbage(target_ptr);
  }

  void
  AddGarbages(const std::vector<T*>& target_ptrs)
  {
    assert(target_ptrs.size() > 1);

    const auto epoch = epoch_manager_.GetCurrentEpoch();
    const auto partition =
        std::hash<std::thread::id>()(std::this_thread::get_id()) & kPartitionMask;

    auto garbage_list = reinterpret_cast<GarbageList<T>*>(garbage_ring_buffer_[epoch][partition]);
    garbage_list->AddGarbages(target_ptrs);
  }

  /*################################################################################################
   * Public GC control functions
   *##############################################################################################*/

  bool
  StartGC()
  {
    if (gc_is_running_) {
      return false;
    } else {
      gc_is_running_ = true;
      gc_thread_ = std::thread{&EpochBasedGC::RunGC, this};
      return true;
    }
  }

  bool
  StopGC()
  {
    if (!gc_is_running_) {
      return false;
    } else {
      gc_is_running_ = false;
      gc_thread_.join();
      return true;
    }
  }
};

}  // namespace dbagroup::gc
