//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}
auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> locker(latch_);
  // 没有允许驱逐的页框
  if (curr_size_ == 0) {
    return false;
  }
  // 如果历史队列不为空，优先按FIFO从历史队列中驱逐
  if (!history_list_.empty()) {
    for (auto iter = history_list_.begin(); iter != history_list_.end(); iter++) {
      if (entries_[*iter].evictable_) {
        *frame_id = *iter;
        entries_.erase(*iter);
        history_list_.erase(iter);
        curr_size_--;
        return true;
      }
    }
  }

  // 如果没能从历史队列中驱逐，则从缓存队列中按照依据k-distance驱逐
  if (!cache_list_.empty()) {
    for (auto iter = cache_list_.begin(); iter != cache_list_.end(); iter++) {
      if (entries_[*iter].evictable_) {
        *frame_id = *iter;
        entries_.erase(*iter);
        cache_list_.erase(iter);
        curr_size_--;
        return true;
      }
    }
  }

  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> locker(latch_);
  // 检查帧id是否在有效范围内
  BUSTUB_ASSERT(frame_id <= static_cast<frame_id_t>(replacer_size_), "Frame ID is out of bounds.");
  size_t new_count = ++entries_[frame_id].hit_count_;
  // 第一次访问
  if (new_count == 1) {
    curr_size_++;
    history_list_.emplace_back(frame_id);
    entries_[frame_id].pos_ = std::prev(history_list_.end());
  } else {
    if (new_count == k_) {
      // 从历史队列移到缓存队列
      cache_list_.emplace_back(frame_id);
      history_list_.erase(entries_[frame_id].pos_);

      entries_[frame_id].pos_ = std::prev(cache_list_.end());
    } else if (new_count > k_) {
      // 从缓存队列移到缓存队列
      cache_list_.erase(entries_[frame_id].pos_);
      cache_list_.emplace_back(frame_id);
      entries_[frame_id].pos_ = std::prev(cache_list_.end());
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> locker(latch_);
  // 检查帧id是否在有效范围内
  BUSTUB_ASSERT(frame_id <= static_cast<frame_id_t>(replacer_size_), "Frame ID is out of bounds.");
  /* frame_id不存在 */
  if (entries_.count(frame_id) == 0) {
    return;
  }
  /* 根据evictable_的初始值和目标值修改curr_size_ */
  if (entries_[frame_id].evictable_ != set_evictable) {
    entries_[frame_id].evictable_ = set_evictable;
    curr_size_ += set_evictable ? 1 : -1;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> locker(latch_);
  // 对应页框不存在
  if (entries_.count(frame_id) == 0) {
    return;
  }
  // 对应页框不可驱逐
  if (!entries_[frame_id].evictable_) {
    return;
  }
  // 根据对应页框的引用次数从历史队列或缓存队列中将其删除
  if (entries_[frame_id].hit_count_ >= k_) {
    cache_list_.erase(entries_[frame_id].pos_);
  } else {
    history_list_.erase(entries_[frame_id].pos_);
  }
  entries_.erase(frame_id);
  curr_size_--;
}
auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> locker(latch_);
  return curr_size_;
}

}  // namespace bustub
