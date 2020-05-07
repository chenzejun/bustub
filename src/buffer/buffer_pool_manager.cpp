//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new ClockReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::scoped_lock<std::mutex> lock(latch_);
  if (page_table_.count(page_id) > 0) {
    auto page = pages_ + page_table_[page_id];
    page->pin_count_++;
    replacer_->Pin(page_table_[page_id]);
    return page;
  }
  frame_id_t frame_id;
  Page *page = nullptr;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    page = pages_ + frame_id;
    replacer_->Unpin(frame_id);
    free_list_.pop_front();
  } else if (replacer_->Victim(&frame_id)) {
    page = pages_ + frame_id;
    if (page->is_dirty_) {
      disk_manager_->WritePage(page->page_id_, page->data_);
    }
    page->ResetMemory();
    page->is_dirty_ = false;
    page->pin_count_ = 0;
  } else {
    return nullptr;
  }
  page->page_id_ = page_id;
  disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());
  page->pin_count_++;
  replacer_->Pin(page_id);
  page_table_[page_id] = frame_id;
  return page;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (page_table_.count(page_id) == 0) {
    return false;
  }
  auto frame_id = page_table_[page_id];
  if (pages_[frame_id].GetPinCount() > 0) {
    pages_[frame_id].is_dirty_ |= is_dirty;
    if (--pages_[frame_id].pin_count_ == 0) {
      replacer_->Unpin(frame_id);
    }
  }
  return true;
}  // namespace bustub

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::scoped_lock<std::mutex> lock(latch_);
  if (page_table_.count(page_id) > 0) {
    auto page = pages_ + page_table_[page_id];
    if (page->is_dirty_) {
      disk_manager_->WritePage(page_id, page->data_);
      page->is_dirty_ = false;
    }
    return true;
  }
  return false;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  Page *page = nullptr;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    replacer_->Unpin(frame_id);
    page = pages_ + frame_id;
    free_list_.pop_front();
  } else if (replacer_->Victim(&frame_id)) {
    page = pages_ + frame_id;
    if (page->is_dirty_) {
      disk_manager_->WritePage(page->page_id_, page->data_);
    }
    page->ResetMemory();
    page->is_dirty_ = false;
    page->pin_count_ = 0;
  } else {
    return nullptr;
  }
  *page_id = disk_manager_->AllocatePage();
  page->page_id_ = *page_id;
  page_table_[*page_id] = frame_id;
  page->pin_count_++;
  replacer_->Pin(*page_id);
  return page;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::scoped_lock<std::mutex> lock(latch_);
  if (page_table_.count(page_id) > 0) {
    auto page = pages_ + page_table_[page_id];
    if (page->GetPinCount() > 0) {
      return false;
    } else {
      page_table_.erase(page_id);
      disk_manager_->DeallocatePage(page_id);
      page->ResetMemory();
      page->is_dirty_ = false;
      page->pin_count_ = 0;
      page->page_id_ = INVALID_PAGE_ID;
      return true;
    }
  }
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  std::scoped_lock<std::mutex> lock(latch_);
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].page_id_ != INVALID_PAGE_ID && pages_[i].is_dirty_) {
      disk_manager_->WritePage(pages_[i].page_id_, pages_[i].data_);
      pages_[i].is_dirty_ = false;
    }
  }
}

}  // namespace bustub
