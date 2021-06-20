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

#pragma once

#include <atomic>
#include <thread>
#include <vector>

#include "common.hpp"

namespace dbgroup::memory::manager::component
{
class PageStack
{
 private:
  /*################################################################################################
   * Internal structs
   *##############################################################################################*/

  struct PageNode {
    void *page_addr = nullptr;
    PageNode *next = nullptr;
  };

  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  std::atomic<PageNode *> pages_;

  std::atomic_size_t size_;

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  constexpr PageStack() : pages_{nullptr}, size_{0} {}

  ~PageStack()
  {
    auto next_node = pages_.load();
    while (next_node != nullptr) {
      auto deleting_node = next_node;
      next_node = next_node->next;
      delete deleting_node;
    }
  }

  PageStack(const PageStack &) = delete;
  PageStack &operator=(const PageStack &) = delete;
  PageStack(PageStack &&) = delete;
  PageStack &operator=(PageStack &&) = delete;

  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  size_t
  Size() const
  {
    return size_.load(mo_relax);
  }

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  void
  AddPage(void *page_addr)
  {
    auto page_node = new PageNode{page_addr, pages_.load(mo_relax)};
    while (!pages_.compare_exchange_weak(page_node->next, page_node, mo_relax)) {
      // continue until inserting succeeds
    }
    size_.fetch_add(1, mo_relax);
  }

  void
  AddPages(const std::vector<void *> &page_addresses)
  {
    assert(page_addresses.size() > 1);

    // prepare inserting page nodes
    auto addr_iter = page_addresses.begin();
    auto bottom_node = new PageNode{*addr_iter, pages_.load(mo_relax)};
    auto top_node = bottom_node;
    while (++addr_iter != page_addresses.end()) {
      top_node = new PageNode{*addr_iter, top_node};
    }

    // insert page nodes
    while (!pages_.compare_exchange_weak(bottom_node->next, top_node, mo_relax)) {
      // continue until inserting succeeds
    }
    size_.fetch_add(page_addresses.size(), mo_relax);
  }

  void *
  GetPage()
  {
    auto page_node = pages_.load(mo_relax);
    while (true) {
      if (page_node == nullptr) {
        // wait other threads to free pages
        std::this_thread::sleep_for(std::chrono::microseconds(200));
        page_node = pages_.load(mo_relax);
      } else if (pages_.compare_exchange_weak(page_node, page_node->next, mo_relax)) {
        break;
      }
    }
    size_.fetch_sub(1, mo_relax);

    auto reserved_page = page_node->page_addr;
    delete page_node;

    return reserved_page;
  }
};

}  // namespace dbgroup::memory::manager::component
