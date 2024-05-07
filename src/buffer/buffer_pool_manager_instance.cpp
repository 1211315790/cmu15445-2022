//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  // "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  // "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}
auto BufferPoolManagerInstance::GetFrame(frame_id_t *frame_id) -> bool {
  // Check if there's an available frame in the free list.
  if (!free_list_.empty()) {
    *frame_id = free_list_.front();
    free_list_.pop_front();
    return true;
  }
  // No free frames, try to evict a page from the buffer pool.
  if (replacer_->Evict(frame_id)) {
    Page *evicted_page = &pages_[*frame_id];
    if (evicted_page->IsDirty()) {
      disk_manager_->WritePage(evicted_page->GetPageId(), evicted_page->GetData());
    }
    page_table_->Remove(evicted_page->GetPageId());
    evicted_page->page_id_ = INVALID_PAGE_ID;
    evicted_page->pin_count_ = 0;
    evicted_page->ResetMemory();
    return true;
  }
  return false;
}
auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (!GetFrame(&frame_id)) {
    return nullptr;
  }
  Page *new_page = &pages_[frame_id];
  // Allocate a new page ID and create a new page in the buffer pool.
  *page_id = AllocatePage();
  // Initialize the new page metadata.
  new_page->page_id_ = *page_id;
  new_page->pin_count_ = 1;
  new_page->ResetMemory();
  // Insert the new page into the page table and mark it as evictable.
  page_table_->Insert(*page_id, frame_id);
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  return new_page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  // Search for the page in the page table.
  frame_id_t frame_id;
  // Page found in the buffer pool.
  if (page_table_->Find(page_id, frame_id)) {
    // Increment the pin count and mark the frame as unpinnable by the replacer.
    pages_[frame_id].pin_count_++;
    // Record the access for the LRU-K algorithm.
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return &pages_[frame_id];
  }
  if (!GetFrame(&frame_id)) {
    return nullptr;
  }
  // Fetch the page from disk into the newly selected frame.
  disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());

  // Initialize the new page metadata and add it to the page table.
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].is_dirty_ = false;
  page_table_->Insert(page_id, frame_id);
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  /* If page_id is not in the buffer pool or its pin count is already
   * 0, return false.*/
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id) || pages_[frame_id].pin_count_ <= 0) {
    return false;
  }

  // If the pin count reaches 0, the frame should be evictable by the replacer.
  if (--pages_[frame_id].pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  // Also, set the dirty flag on the page to indicate if the page was modified.
  if (is_dirty) {
    pages_[frame_id].is_dirty_ = true;
  }

  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lck(latch_);
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }
  disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  pages_[frame_id].is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> lck(latch_);
  for (size_t i = 0; i < pool_size_; ++i) {
    FlushPgImp(pages_[i].GetPageId());
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lck(latch_);
  // If page_id is not in the buffer pool, do nothing and return true.
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return true;
  }
  // If the page is pinned and cannot be deleted, return false immediately.
  if (pages_[frame_id].pin_count_ > 0) {
    return false;
  }
  /* After deleting the page from the page table, stop tracking the frame in the replacer and add the frame
   * back to the free list. Also, reset the page's memory and metadata. Finally, you should call DeallocatePage() to
   * imitate freeing the page on the disk.*/
  page_table_->Remove(page_id);
  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].is_dirty_ = false;
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
