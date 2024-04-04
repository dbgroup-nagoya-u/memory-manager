/*
 * Copyright 2021 Database Group, Nagoya University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef DBGROUP_MEMORY_COMPONENT_GARBAGE_LIST_HPP
#define DBGROUP_MEMORY_COMPONENT_GARBAGE_LIST_HPP

// C++ standard libraries
#include <array>
#include <atomic>
#include <cstddef>

// local sources
#include "memory/utility.hpp"

namespace dbgroup::memory::component
{
/**
 * @brief A class for representing the buffer of garbage instances.
 *
 */
class alignas(kVMPageSize) GarbageList
{
 public:
  /*############################################################################
   * Public constructors and assignment operators
   *##########################################################################*/

  constexpr GarbageList() = default;

  GarbageList(const GarbageList &) = delete;
  GarbageList(GarbageList &&) = delete;

  auto operator=(const GarbageList &) -> GarbageList & = delete;
  auto operator=(GarbageList &&) -> GarbageList & = delete;

  /*############################################################################
   * Public destructors
   *##########################################################################*/

  ~GarbageList() = default;

  /*############################################################################
   * Public getters/setters
   *##########################################################################*/

  /**
   * @retval true if this list is empty.
   * @retval false otherwise
   */
  [[nodiscard]] auto Empty() const  //
      -> bool;

  /*############################################################################
   * Public utility functions
   *##########################################################################*/

  /**
   * @brief Add a new garbage instance to a specified buffer.
   *
   * If the buffer becomes full, create a new garbage buffer and link them.
   *
   * @param buf_addr The address of the pointer of a target buffer.
   * @param epoch An epoch when garbage is added.
   * @param garbage A new garbage instance.
   */
  static void AddGarbage(  //
      GarbageList **buf_addr,
      size_t epoch,
      void *garbage);

  /**
   * @brief Reuse a destructed page if exist.
   *
   * @param buf_addr The address of the pointer of a target buffer.
   * @retval A memory page if exist.
   * @retval nullptr otherwise.
   */
  static auto ReusePage(                     //
      std::atomic<GarbageList *> *buf_addr)  //
      -> void *;

  /**
   * @brief Destruct garbage where their epoch is less than a protected one.
   *
   * @tparam Target A class for representing target garbage.
   * @param buf_addr The address of the pointer of a target buffer.
   * @param protected_epoch A protected epoch.
   */
  template <class Target>
  static void
  Destruct(  //
      GarbageList **buf_addr,
      const size_t protected_epoch)
  {
    using T = typename Target::T;

    while (true) {
      // release unprotected garbage
      auto *buf = *buf_addr;
      const auto end_pos = buf->end_pos_.load(std::memory_order_acquire);
      auto pos = buf->mid_pos_.load(std::memory_order_relaxed);
      for (; pos < end_pos; ++pos) {
        if (buf->garbage_.at(pos).epoch >= protected_epoch) break;

        // only call destructor to reuse pages
        if constexpr (!std::is_same_v<T, void>) {
          reinterpret_cast<T *>(buf->garbage_.at(pos).ptr)->~T();
        }
      }

      // update the position to make visible destructed garbage
      buf->mid_pos_.store(pos, std::memory_order_release);
      if (pos < kGarbageBufSize) return;

      // release the next buffer recursively
      *buf_addr = buf->next_;
    }
  }

  /**
   * @brief Release garbage where their epoch is less than a protected one.
   *
   * @tparam Target A class for representing target garbage.
   * @param buf_addr The address of the pointer of a target buffer.
   * @param protected_epoch A protected epoch.
   */
  template <class Target>
  static void
  Clear(  //
      GarbageList **buf_addr,
      const size_t protected_epoch)
  {
    using T = typename Target::T;

    while (true) {
      auto *buf = *buf_addr;
      const auto mid_pos = buf->mid_pos_.load(std::memory_order_relaxed);
      const auto end_pos = buf->end_pos_.load(std::memory_order_acquire);

      // release unprotected garbage
      auto pos = buf->begin_pos_.load(std::memory_order_relaxed);
      for (; pos < mid_pos; ++pos) {
        // the garbage has been already destructed
        Release<Target>(buf->garbage_.at(pos).ptr);
      }
      for (; pos < end_pos; ++pos) {
        if (buf->garbage_.at(pos).epoch >= protected_epoch) break;

        auto *ptr = buf->garbage_.at(pos).ptr;
        if constexpr (!std::is_same_v<T, void>) {
          reinterpret_cast<T *>(ptr)->~T();
        }
        Release<Target>(ptr);
      }
      buf->begin_pos_.store(pos, std::memory_order_relaxed);
      buf->mid_pos_.store(pos, std::memory_order_relaxed);

      if (pos < kGarbageBufSize) return;

      // release the next buffer recursively
      *buf_addr = buf->next_;
      delete buf;
    }
  }

 private:
  /*############################################################################
   * Internal classes
   *##########################################################################*/

  /**
   * @brief A class for retaining registered garbage instances.
   *
   */
  struct Garbage {
    /// @brief An epoch when garbage is registered.
    size_t epoch{0};

    /// @brief A registered garbage pointer.
    void *ptr{nullptr};
  };

  /*############################################################################
   * Internal member variables
   *##########################################################################*/

  /// @brief The begin position of destructed garbage.
  std::atomic_size_t begin_pos_{0};

  /// @brief The end position of destructed garbage.
  std::atomic_size_t mid_pos_{0};

  /// @brief A buffer of garbage instances with added epochs.
  std::array<Garbage, kGarbageBufSize> garbage_{};

  /// @brief The end position of registered garbage.
  std::atomic_size_t end_pos_{0};

  /// @brief The next garbage buffer.
  GarbageList *next_{nullptr};
};

}  // namespace dbgroup::memory::component

#endif  // DBGROUP_MEMORY_COMPONENT_GARBAGE_LIST_HPP
