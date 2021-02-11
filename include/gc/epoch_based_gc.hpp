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

namespace gc::epoch
{
template <class T>
class alignas(kCacheLineSize) EpochBasedGC
{
  static constexpr size_t kGCIntervalMilliSec = 100;

 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  std::array<std::array<std::atomic_uintptr_t, kPartitionNum>, kBufferSize> garbage_ring_buffer_;

  EpochManager epoch_manager_;

  const size_t gc_interval_ms_;

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
        auto uintptr = garbage_ring_buffer_[epoch][partition].load();
        const auto garbage = static_cast<GarbageList<T>*>(reinterpret_cast<void*>(uintptr));
        delete garbage;  // all garbages are deleted by domino effect
      }
    }
  }

  void
  RunGC()
  {
    while (gc_is_running_) {
      // wait for garbages to be out of scope
      std::this_thread::sleep_for(std::chrono::milliseconds(gc_interval_ms_));
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
      const size_t gc_interval_ms = kGCIntervalMilliSec,
      const bool start_gc = true)
      : epoch_manager_{}, gc_interval_ms_{gc_interval_ms}
  {
    garbage_ring_buffer_.fill({0, 0, 0, 0, 0, 0, 0, 0});
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
      std::this_thread::sleep_for(std::chrono::milliseconds(gc_interval_ms_));
      // delete freeable garbages
      std::tie(begin_epoch, end_epoch) = epoch_manager_.ListFreeableEpoch();
      DeleteGarbages(begin_epoch, end_epoch);
    } while (end_epoch != current_epoch);
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
  AddGarbage(  //
      const EpochGuard& guard,
      const T* target_ptr)
  {
    const auto epoch = guard.GetEpoch();
    const auto partition = guard.GetPartitionId();
    auto target_partition = garbage_ring_buffer_[epoch][partition];

    // create an inserting garbage
    auto garbage = new GarbageList{target_ptr};

    // swap the head of a garbage list
    auto old_head = target_partition.load();
    const auto new_head = reinterpret_cast<uintptr_t>(static_cast<void*>(garbage));
    while (!target_partition.compare_exchange_weak(old_head, new_head)) {
      // continue until installation succeeds
    }
    const auto old_head_garbage = static_cast<GarbageList<T>*>(reinterpret_cast<void*>(old_head));
    garbage->SetNext(old_head_garbage);
  }

  void
  AddGarbages(  //
      const EpochGuard& guard,
      const std::vector<T*>& target_ptrs)
  {
    assert(target_ptrs.size() > 1);

    const auto epoch = guard.GetEpoch();
    const auto partition = guard.GetPartitionId();
    auto target_partition = garbage_ring_buffer_[epoch][partition];

    // create an inserting garbage list
    auto tail_garbage = new GarbageList{target_ptrs[0]};
    auto head_garbage = tail_garbage;
    for (size_t index = 1; index < target_ptrs.size(); ++index) {
      head_garbage = new GarbageList{target_ptrs[index], head_garbage};
    }

    // swap the head of a garbage list
    auto old_head = target_partition.load();
    const auto new_head = reinterpret_cast<uintptr_t>(static_cast<void*>(head_garbage));
    while (!target_partition.compare_exchange_weak(old_head, new_head)) {
      // continue until installation succeeds
    }
    const auto old_head_garbage = static_cast<GarbageList<T>*>(reinterpret_cast<void*>(old_head));
    tail_garbage->SetNext(old_head_garbage);
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
      gc_thread_ = std::thread{EpochBasedGC::RunGC, this};
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

}  // namespace gc::epoch
