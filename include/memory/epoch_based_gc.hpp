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

#ifndef MEMORY_EPOCH_BASED_GC_HPP
#define MEMORY_EPOCH_BASED_GC_HPP

#include <atomic>
#include <chrono>
#include <functional>
#include <limits>
#include <memory>
#include <shared_mutex>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include "component/epoch_guard.hpp"
#include "component/garbage_list.hpp"
#include "epoch_manager.hpp"
#include "utility.hpp"

namespace dbgroup::memory
{
/**
 * @brief A class to manage garbage collection.
 *
 * @tparam T a target class of garbage collection.
 */
template <class... GCTargets>
class EpochBasedGC
{
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  // forward declaration for default parameters.
  struct DefaultTarget;

  using Epoch = component::Epoch;
  using EpochGuard = component::EpochGuard;
  using Clock_t = ::std::chrono::high_resolution_clock;

  template <class T>
  using GarbageList = component::GarbageList<T>;

  template <class Target>
  using GarbageNode = component::GarbageNode<Target>;

 public:
  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  /**
   * @brief Construct a new instance.
   *
   * @param gc_interval_micro_sec the duration of interval for GC.
   * @param gc_thread_num the maximum number of threads to perform GC.
   */
  constexpr EpochBasedGC(  //
      const size_t gc_interval_micro_sec,
      const size_t gc_thread_num)
      : gc_interval_{gc_interval_micro_sec}, gc_thread_num_{gc_thread_num}
  {
    cleaner_threads_.reserve(gc_thread_num_);
  }

  EpochBasedGC(const EpochBasedGC &) = delete;
  auto operator=(const EpochBasedGC &) -> EpochBasedGC & = delete;
  EpochBasedGC(EpochBasedGC &&) = delete;
  auto operator=(EpochBasedGC &&) -> EpochBasedGC & = delete;

  /*####################################################################################
   * Public destructors
   *##################################################################################*/

  /**
   * @brief Destroy the instance.
   *
   * If protected garbage remains, this destructor waits for them to be free.
   */
  ~EpochBasedGC()
  {
    // stop garbage collection
    StopGC();
  }

  /*####################################################################################
   * Public utility functions
   *##################################################################################*/

  /**
   * @brief Create a guard instance to protect garbage based on the scoped locking
   * pattern.
   *
   * @return EpochGuard a created epoch guard.
   */
  auto
  CreateEpochGuard()  //
      -> EpochGuard
  {
    return epoch_manager_.CreateEpochGuard();
  }

  /**
   * @brief Add a new garbage instance.
   *
   * @param garbage_ptr a pointer to a target garbage.
   */
  template <class Target = DefaultTarget>
  void
  AddGarbage(const void *garbage_ptr)
  {
    using T = typename Target::T;
    using GarbageNode_t = GarbageNode<Target>;
    using TLSList_t = TLSList<GarbageNode_t>;
    using TLSNode_t = TLSNode<GarbageNode_t>;

    auto *tls_list = GetTLSGarbageList<Target, GCTargets...>();
    if (tls_list->use_count() <= 1) {
      // register the garbage list with GC
      auto *garbage_list = new GarbageList<Target>{};
      auto [garbage_head, mtx_p] = GetGarbageNodeHead<Target>();
      mtx_p->lock();
      auto *garbage_node = new GarbageNode_t{garbage_list, *garbage_head};
      *garbage_head = garbage_node;
      mtx_p->unlock();

      // register the TLS information with GC
      *tls_list = std::make_shared<TLSList_t>(garbage_list, garbage_node);
      auto *tls_head = GetTLSNodeHead<Target>();
      TLSNode_t::AddNewNode(tls_list, tls_head);
    }

    // the current thread has already joined GC
    auto *ptr = static_cast<T *>(const_cast<void *>(garbage_ptr));
    (*tls_list)->AddGarbage(epoch_manager_.GetCurrentEpoch(), ptr);
  }

  /**
   * @brief Reuse a released memory page if it exists.
   *
   * @retval nullptr if there are no reusable pages.
   * @retval a memory page.
   */
  template <class T = DefaultTarget>
  auto
  GetPageIfPossible()  //
      -> void *
  {
    auto *tls_list = GetTLSGarbageList<T, GCTargets...>();

    if (tls_list->use_count() <= 1) return nullptr;
    return (*tls_list)->GetPageIfPossible();
  }

  /*####################################################################################
   * Public GC control functions
   *##################################################################################*/

  /**
   * @brief Start garbage collection.
   *
   * @retval true if garbage collection has started.
   * @retval false if garbage collection is already running.
   */
  auto
  StartGC()  //
      -> bool
  {
    if (gc_is_running_.load(std::memory_order_relaxed)) return false;

    gc_is_running_.store(true, std::memory_order_relaxed);
    gc_thread_ = std::thread{&EpochBasedGC::RunGC, this};
    return true;
  }

  /**
   * @brief Stop garbage collection.
   *
   * @retval true if garbage collection has stopped.
   * @retval false if garbage collection is not running.
   */
  auto
  StopGC()  //
      -> bool
  {
    if (!gc_is_running_.load(std::memory_order_relaxed)) return false;

    gc_is_running_.store(false, std::memory_order_relaxed);
    gc_thread_.join();
    return true;
  }

 private:
  /*####################################################################################
   * Internal classes
   *##################################################################################*/

  /**
   * @brief A default GC information.
   *
   */
  struct DefaultTarget {
    /// use the void type and do not perform destructors.
    using T = void;

    /// do not reuse pages after GC (release immediately).
    static constexpr bool kReusePages = false;

    /// use the standard delete function to release pages.
    static const inline std::function<void(void *)> deleter = [](void *ptr) {
      ::operator delete(ptr);
    };
  };

  /**
   * @brief A class for retaining thread-local garbage lists.
   *
   * @tparam DataNode a class for representing garbage nodes to be contained.
   */
  template <class DataNode>
  class TLSList
  {
   public:
    /*##################################################################################
     * Type aliases
     *################################################################################*/

    using GarbageList_t = typename DataNode::GarbageList_t;
    using GarbageList_p = typename DataNode::GarbageList_p;
    using GarbageNode_p = typename DataNode::GarbageNode_p;
    using T = typename DataNode::GarbageList_t::T;

    /*##################################################################################
     * Public constructors and assignment operators
     *################################################################################*/

    /**
     * @brief Construct a new TLSList object.
     *
     * @param list an initial garbage list.
     * @param node the corresponding garbage node.
     */
    TLSList(  //
        GarbageList_p list,
        GarbageNode_p node)
        : tail_{list}
    {
      data_node_.store(node, std::memory_order_release);
    }

    TLSList(const TLSList &) = delete;
    TLSList(TLSList &&) = delete;

    auto operator=(const TLSList &) -> TLSList & = delete;
    auto operator=(TLSList &&) -> TLSList & = delete;

    /*##################################################################################
     * Public destructors
     *################################################################################*/

    ~TLSList() = default;

    /*##################################################################################
     * Public utilities for a worker thread
     *################################################################################*/

    /**
     * @brief Add a new garbage instance.
     *
     * @param epoch an epoch value when a garbage is added.
     * @param garbage_ptr a pointer to a target garbage.
     */
    void
    AddGarbage(  //
        const size_t epoch,
        T *garbage_ptr)
    {
      tail_ = GarbageList_t::AddGarbage(tail_, epoch, garbage_ptr);
    }

    /**
     * @brief Reuse a released memory page if it exists in the list.
     *
     * @retval nullptr if the list does not have reusable pages.
     * @retval a memory page.
     */
    auto
    GetPageIfPossible()  //
        -> void *
    {
      auto *data_node = data_node_.load(std::memory_order_relaxed);
      return (data_node != nullptr) ? data_node->GetPageIfPossible() : nullptr;
    }

    /*##################################################################################
     * Public utilities for a GC thread
     *################################################################################*/

    /**
     * @brief Expire the corresponding garbage node for destruction.
     *
     */
    void
    Expire()
    {
      auto *data_node = data_node_.load(std::memory_order_acquire);
      data_node->Expire();
      data_node_.store(nullptr, std::memory_order_relaxed);
    }

   private:
    /*##################################################################################
     * Internal member variables
     *################################################################################*/

    /// @brief A garbage list to be added new garbage.
    GarbageList_p tail_{nullptr};

    /// @brief The corresponding garbage node.
    std::atomic<GarbageNode_p> data_node_{nullptr};
  };

  /**
   * @brief A class for representing linked-list nodes.
   *
   * @tparam DataNode a class for representing garbage nodes to be contained.
   */
  template <class DataNode>
  class TLSNode
  {
   public:
    /*##################################################################################
     * Type aliases
     *################################################################################*/

    using TLSList_t = TLSList<DataNode>;

    /*##################################################################################
     * Public constructors and assignment operators
     *################################################################################*/

    /**
     * @brief Construct a new TLSNode object.
     *
     * @param list the corresponding thread-local garbage list.
     */
    explicit TLSNode(std::shared_ptr<TLSList_t> list) : list_{std::move(list)} {}

    TLSNode(const TLSNode &) = delete;
    TLSNode(TLSNode &&) = delete;

    auto operator=(const TLSNode &) -> TLSNode & = delete;
    auto operator=(TLSNode &&) -> TLSNode & = delete;

    /*##################################################################################
     * Public destructors
     *################################################################################*/

    ~TLSNode() = default;

    /*##################################################################################
     * Public utilities for worker threads
     *################################################################################*/

    /**
     * @brief Add a new node to a given linked list atomically.
     *
     * @param list a garbage list to be added.
     * @param head the head of a linked list.
     */
    static void
    AddNewNode(  //
        const std::shared_ptr<TLSList_t> *list,
        std::atomic<TLSNode *> *head)
    {
      auto *node = new TLSNode{*list};
      auto *next = head->load(std::memory_order_relaxed);
      do {
        node->next_ = next;
      } while (!head->compare_exchange_weak(next, node, std::memory_order_release));
    }

    /*##################################################################################
     * Public utilities for a GC thread
     *################################################################################*/

    /**
     * @brief Remove expired nodes from a linked list.
     *
     * @param node_p the address of the head pointer.
     * @param force_expire expire all the nodes forcefully if true.
     * @retval true if all the node are removed.
     * @retval false otherwise.
     */
    static auto
    RemoveExpiredNodes(  //
        std::atomic<TLSNode *> *node_p,
        const bool force_expire)  //
        -> bool
    {
      auto *node = node_p->load(std::memory_order_acquire);
      if (node == nullptr) return true;

      while (node != nullptr) {
        if (!force_expire && node->list_.use_count() > 1) {
          // go to the next node
          node_p = &(node->next_);
          node = node_p->load(std::memory_order_relaxed);
          continue;
        }

        // this node can be removed
        auto *next = node->next_.load(std::memory_order_relaxed);
        if (node_p->compare_exchange_strong(node, next, std::memory_order_acquire)) {
          node->list_->Expire();
          delete node;
          node = next;
        }
      }

      return false;
    }

   private:
    /*##################################################################################
     * Internal member variables
     *################################################################################*/

    /// @brief The corresponding thread-local garbage list.
    std::shared_ptr<TLSList_t> list_{nullptr};

    /// @brief The next node in a linked list.
    std::atomic<TLSNode *> next_{nullptr};
  };

  /**
   * @brief A class for representing the heads of garbage lists.
   *
   * @tparam Target a class for representing target garbage.
   */
  template <class Target>
  class GarbageHead
  {
   public:
    /*##################################################################################
     * Type aliases
     *################################################################################*/

    using GarbageNode_t = GarbageNode<Target>;
    using GarbageNode_p = typename GarbageNode_t::GarbageNode_p;

    /*##################################################################################
     * Public constructors and assignment operators
     *################################################################################*/

    /**
     * @brief Construct a new GarbageHead object.
     *
     */
    GarbageHead() : head_{new GarbageNode_p{nullptr}} {}

    GarbageHead(GarbageHead &&obj) noexcept
    {
      head_ = obj.head_;
      obj.head_ = nullptr;
    }

    auto
    operator=(GarbageHead &&obj) noexcept -> GarbageHead &
    {
      head_ = obj.head_;
      obj.head_ = nullptr;
      return *this;
    }

    // copies are deleted
    GarbageHead(const GarbageHead &) = delete;
    auto operator=(const GarbageHead &) -> GarbageHead & = delete;

    /*##################################################################################
     * Public destructors
     *################################################################################*/

    ~GarbageHead() { delete head_; }

    /*##################################################################################
     * Public getters/setters
     *################################################################################*/

    /**
     * @retval 1st: the head pointer.
     * @retval 2nd: the corresponding mutex.
     */
    auto
    GetHead()  //
        -> std::pair<GarbageNode_p *, std::mutex *>
    {
      return {head_, mtx_.get()};
    }

   private:
    /*##################################################################################
     * Internal member variables
     *################################################################################*/

    /// @brief The head of a linked list.
    GarbageNode_p *head_{nullptr};

    /// @brief A mutex object for modifying the head pointer.
    std::unique_ptr<std::mutex> mtx_ = std::make_unique<std::mutex>();
  };

  /**
   * @brief A class for representing the heads of TLS lists.
   *
   * @tparam Target a class for representing target garbage.
   */
  template <class Target>
  struct TLSHead {
    /*##################################################################################
     * Type aliases
     *################################################################################*/

    using GarbageNode_t = typename GarbageHead<Target>::GarbageNode_t;
    using TLSNode_t = std::atomic<TLSNode<GarbageNode_t> *>;

    /*##################################################################################
     * Public member variables
     *################################################################################*/

    /// @brief The head of a linked list.
    std::unique_ptr<TLSNode_t> head = std::make_unique<TLSNode_t>(nullptr);
  };

  /*####################################################################################
   * Recursive functions for converting parameter packs
   *##################################################################################*/

  template <template <class T> class OutT, class InT, class... Tails>
  static auto
  ConvToHeads()
  {
    using OutT_t = OutT<InT>;
    return std::tuple_cat(std::tuple<OutT_t>{}, ConvToHeads<OutT, Tails...>());
  }

  template <template <class T> class OutT>
  static auto
  ConvToHeads()
  {
    using OutT_t = OutT<DefaultTarget>;
    return std::tuple<OutT_t>{};
  }

  using GarbageHeads_t = decltype(ConvToHeads<GarbageHead, GCTargets...>());
  using TLSHeads_t = decltype(ConvToHeads<TLSHead, GCTargets...>());

  /*####################################################################################
   * Recursive functions for managing thread_local variables
   *##################################################################################*/

  /**
   * @tparam Target a class for representing target garbage.
   * @tparam Head the current class in garbage targets.
   * @tparam Tails the remaining classes in garbage targets.
   * @return a garbage list for each thread.
   */
  template <class Target, class Head, class... Tails>
  [[nodiscard]] auto
  GetTLSGarbageList() const  //
      -> std::shared_ptr<TLSList<GarbageNode<Target>>> *
  {
    using TLSList_t = TLSList<GarbageNode<Target>>;

    if constexpr (std::is_same_v<Head, Target>) {
      thread_local std::shared_ptr<TLSList_t> garbage_list{nullptr};
      return &garbage_list;
    } else {
      return GetTLSGarbageList<Target, Tails...>();
    }
  }

  /**
   * @tparam Target a class for representing target garbage.
   * @return a garbage list for each thread.
   */
  template <class Target>
  [[nodiscard]] auto
  GetTLSGarbageList() const  //
      -> std::shared_ptr<TLSList<GarbageNode<DefaultTarget>>> *
  {
    static_assert(std::is_same_v<Target, DefaultTarget>);
    using TLSList_t = TLSList<GarbageNode<DefaultTarget>>;

    thread_local std::shared_ptr<TLSList_t> garbage_list{nullptr};
    return &garbage_list;
  }

  /*####################################################################################
   * Internal utility functions
   *##################################################################################*/

  /**
   * @tparam Target a class for representing target garbage.
   * @return the head of a linked list of TLS nodes.
   */
  template <class Target>
  [[nodiscard]] auto
  GetTLSNodeHead()  //
      -> std::atomic<TLSNode<GarbageNode<Target>> *> *
  {
    auto &target = std::get<TLSHead<Target>>(tls_heads_);
    return target.head.get();
  }

  /**
   * @tparam Target a class for representing target garbage.
   * @return the head of a linked list of garbage nodes and its mutex object.
   */
  template <class Target>
  [[nodiscard]] auto
  GetGarbageNodeHead()  //
      -> std::pair<GarbageNode<Target> **, std::mutex *>
  {
    return std::get<GarbageHead<Target>>(garbage_heads_).GetHead();
  }

  /**
   * @brief Remove expired TLS nodes from garbage collection.
   *
   * @tparam Head the current class in garbage targets.
   * @tparam Tails the remaining classes in garbage targets.
   * @param force_expire expire all the nodes forcefully if true.
   * @retval true if all the node are removed for every target garbage.
   * @retval false otherwise.
   */
  template <class Head, class... Tails>
  auto
  RemoveExpiredNodes(const bool force_expire)  //
      -> bool
  {
    using TLSNode_t = TLSNode<GarbageNode<Head>>;

    auto *head = GetTLSNodeHead<Head>();
    auto all_node_expired = TLSNode_t::RemoveExpiredNodes(head, force_expire);

    if constexpr (sizeof...(Tails) > 0) {
      all_node_expired &= RemoveExpiredNodes<Tails...>(force_expire);
    }

    return all_node_expired;
  }

  /**
   * @brief Clear registered garbage if possible.
   *
   * @tparam Head the current class in garbage targets.
   * @tparam Tails the remaining classes in garbage targets.
   * @param protected_epoch an epoch value to be protected.
   * @retval true if all the garbage is released for every target type.
   * @retval false otherwise.
   */
  template <class Head, class... Tails>
  auto
  ClearGarbage(const size_t protected_epoch)  //
      -> bool
  {
    using GarbageNode_t = GarbageNode<Head>;

    auto [head, mtx_p] = GetGarbageNodeHead<Head>();
    auto all_garbage_released = GarbageNode_t::ClearGarbage(protected_epoch, mtx_p, head);

    if constexpr (sizeof...(Tails) > 0) {
      all_garbage_released &= ClearGarbage<Tails...>(protected_epoch);
    }

    return all_garbage_released;
  }

  /**
   * @brief Run a procedure of garbage collection.
   *
   */
  void
  RunGC()
  {
    // create cleaner threads
    for (size_t i = 0; i < gc_thread_num_; ++i) {
      cleaner_threads_.emplace_back([&]() {
        for (auto wake_time = Clock_t::now() + gc_interval_; true; wake_time += gc_interval_) {
          // release unprotected garbage
          auto released = ClearGarbage<DefaultTarget, GCTargets...>(epoch_manager_.GetMinEpoch());
          auto is_running = gc_is_running_.load(std::memory_order_relaxed);
          if (!is_running && released) break;

          // wait until the next epoch
          std::this_thread::sleep_until(wake_time);
        }
      });
    }

    // manage the global epoch
    for (auto wake_time = Clock_t::now() + gc_interval_; true; wake_time += gc_interval_) {
      // remove expired TLS nodes
      auto is_running = gc_is_running_.load(std::memory_order_relaxed);
      auto expired = RemoveExpiredNodes<DefaultTarget, GCTargets...>(!is_running);
      if (!is_running && expired) break;

      // wait until the next epoch
      std::this_thread::sleep_until(wake_time);
      epoch_manager_.ForwardGlobalEpoch();
    }

    // wait all the cleaner threads return
    for (auto &&t : cleaner_threads_) {
      t.join();
    }
    cleaner_threads_.clear();
  }

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// the duration of garbage collection in micro seconds.
  const std::chrono::microseconds gc_interval_{static_cast<size_t>(1e5)};

  /// the maximum number of cleaner threads
  const size_t gc_thread_num_{1};

  /// an epoch manager.
  EpochManager epoch_manager_{};

  /// a mutex to protect liked garbage lists
  std::shared_mutex garbage_lists_lock_{};

  /// a thread to run garbage collection.
  std::thread gc_thread_{};

  /// worker threads to release garbage
  std::vector<std::thread> cleaner_threads_{};

  /// a flag to check whether garbage collection is running.
  std::atomic_bool gc_is_running_{false};

  /// @brief The heads of linked lists for each GC target.
  GarbageHeads_t garbage_heads_ = ConvToHeads<GarbageHead, GCTargets...>();

  /// @brief The heads of linked lists for each TLS watcher.
  TLSHeads_t tls_heads_ = ConvToHeads<TLSHead, GCTargets...>();
};

}  // namespace dbgroup::memory

#endif  // MEMORY_EPOCH_BASED_GC_HPP
