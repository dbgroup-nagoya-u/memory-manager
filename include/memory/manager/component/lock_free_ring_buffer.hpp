// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <vector>

#include "common.hpp"

namespace dbgroup::memory::manager::component
{
template <class T>
class LockFreeRingBuffer
{
 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  const size_t size_;

  std::atomic_size_t begin_index_;

  std::atomic_size_t end_index_;

  std::atomic_size_t reserved_end_;

  std::vector<T> ring_buffer_;

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  explicit LockFreeRingBuffer(const size_t size)
      : size_{size}, begin_index_{0}, end_index_{0}, reserved_end_{0}
  {
    ring_buffer_.reserve(size_);
    for (size_t count = 0; count < size_; ++count) {
      ring_buffer_.emplace_back();
    }
  }

  LockFreeRingBuffer(  //
      const size_t size,
      const T& initial_element)
      : size_{size}, begin_index_{0}, end_index_{0}, reserved_end_{0}
  {
    ring_buffer_.reserve(size_);
    for (size_t count = 0; count < size_; ++count) {
      ring_buffer_.emplace_back(initial_element);
    }
  }

  explicit LockFreeRingBuffer(const std::vector<T>& initial_elements)
      : size_{initial_elements.size()}, begin_index_{0}, end_index_{0}, reserved_end_{0}
  {
    ring_buffer_.reserve(size_);
    ring_buffer_.insert(ring_buffer_.begin(), initial_elements.begin(), initial_elements.end());
  }

  ~LockFreeRingBuffer() = default;

  LockFreeRingBuffer(const LockFreeRingBuffer&) = delete;
  LockFreeRingBuffer& operator=(const LockFreeRingBuffer&) = delete;
  LockFreeRingBuffer(LockFreeRingBuffer&&) = delete;
  LockFreeRingBuffer& operator=(LockFreeRingBuffer&&) = delete;

  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  size_t
  Size() const
  {
    const auto current_begin = begin_index_.load(mo_relax);
    const auto current_end = end_index_.load(mo_relax);

    if (current_begin < current_end) {
      return current_end - current_begin;
    } else {
      return size_ - (current_begin - current_end);
    }
  }

  size_t
  FreeSpace() const
  {
    const auto current_begin = begin_index_.load(mo_relax);
    const auto current_end = end_index_.load(mo_relax);

    if (current_begin < current_end) {
      return size_ - (current_begin - current_end);
    } else {
      return current_end - current_begin;
    }
  }

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  T
  Pop()
  {
    auto current_begin = begin_index_.load(mo_relax);
    size_t next_index;
    do {
      next_index = (current_begin == size_ - 1) ? 0 : current_begin + 1;
      const auto current_end = end_index_.load(mo_relax);
      if (next_index == current_end) {
        continue;  // retry because there is no capacity
      }
    } while (begin_index_.compare_exchange_weak(current_begin, next_index, mo_relax));

    return ring_buffer_[next_index];
  }

  void
  Push(const T element)
  {
    // reserve space
    auto current_reserved = reserved_end_.load(mo_relax);
    size_t next_reserved;
    do {
      next_reserved = (current_reserved == size_ - 1) ? 0 : current_reserved + 1;
      const auto current_begin = begin_index_.load(mo_relax);
      if (current_reserved == current_begin) {
        continue;  // retry because there is no capacity
      }
    } while (reserved_end_.compare_exchange_weak(current_reserved, next_reserved, mo_relax));

    // push an element
    ring_buffer_[next_reserved] = element;

    // update a tail index
    while (end_index_.compare_exchange_weak(current_reserved, next_reserved, mo_relax)) {
      // end_index_ is serialized by reserved_end_
    }
  }
};

}  // namespace dbgroup::memory::manager::component
