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
#include <algorithm>
#include <cstdio>
#include <iterator>
#include <list>
#include "common/config.h"
#include "common/macros.h"
#include "fmt/core.h"

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
      if (iter->evictable_) {
        *frame_id = iter->frame_id_;
        histroy_entries_.erase(iter->frame_id_);
        history_list_.erase(iter);
        curr_size_--;
        return true;
      }
    }
  }

  // 如果没能从历史队列中驱逐，则从缓存队列中按照依据k-distance驱逐
  if (!cache_list_.empty()) {
    size_t min_k = std::numeric_limits<size_t>::max();
    auto frame_iter = cache_list_.end();
    for (auto iter = cache_list_.begin(); iter != cache_list_.end(); iter++) {
      if (iter->evictable_) {
        size_t k = cache_entries_[iter->frame_id_]
                       ->timestamp_list_[cache_entries_[iter->frame_id_]->timestamp_list_.size() - k_];
        if (min_k > k) {
          min_k = k;
          frame_iter = iter;
        }
      }
    }
    if (frame_iter != cache_list_.end()) {
      *frame_id = frame_iter->frame_id_;
      cache_entries_.erase(frame_iter->frame_id_);
      cache_list_.erase(frame_iter);
      curr_size_--;
      return true;
    }
  }

  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> locker(latch_);
  // 检查帧id是否在有效范围内
  BUSTUB_ASSERT(frame_id <= static_cast<frame_id_t>(replacer_size_), "Frame ID is out of bounds.");
  if (histroy_entries_.count(frame_id) != 0) {
    auto entry = histroy_entries_[frame_id];
    entry->timestamp_list_.push_back(current_timestamp_);
    if (entry->timestamp_list_.size() == k_) {
      // insert into cache_list_ head
      cache_list_.push_back(std::move(*entry));
      cache_entries_[frame_id] = std::prev(cache_list_.end());
      // remove from history_list_
      history_list_.erase(entry);
      histroy_entries_.erase(frame_id);
    }
  } else if (cache_entries_.count(frame_id) != 0) {
    auto entry = cache_entries_[frame_id];
    entry->timestamp_list_.push_back(current_timestamp_);
  } else {
    FrameEntry entry(frame_id);
    entry.timestamp_list_.push_back(current_timestamp_);
    history_list_.push_back(std::move(entry));
    histroy_entries_[frame_id] = std::prev(history_list_.end());
    curr_size_++;
  }
  current_timestamp_++;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> locker(latch_);
  // 检查帧id是否在有效范围内
  BUSTUB_ASSERT(frame_id <= static_cast<frame_id_t>(replacer_size_), "Frame ID is out of bounds.");
  if (histroy_entries_.count(frame_id) != 0) {
    if (histroy_entries_[frame_id]->evictable_ != set_evictable) {
      histroy_entries_[frame_id]->evictable_ = set_evictable;
      curr_size_ += set_evictable ? 1 : -1;
    }
  }
  if (cache_entries_.count(frame_id) != 0) {
    if (cache_entries_[frame_id]->evictable_ != set_evictable) {
      cache_entries_[frame_id]->evictable_ = set_evictable;
      curr_size_ += set_evictable ? 1 : -1;
    }
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> locker(latch_);
  if (histroy_entries_.count(frame_id) != 0) {
    BUSTUB_ASSERT(histroy_entries_[frame_id]->evictable_ == true, "Frame is not evictable");
    history_list_.erase(histroy_entries_[frame_id]);
    histroy_entries_.erase(frame_id);
    curr_size_--;
  }
  if (cache_entries_.count(frame_id) != 0) {
    BUSTUB_ASSERT(cache_entries_[frame_id]->evictable_ == true, "Frame is not evictable");
    cache_list_.erase(cache_entries_[frame_id]);
    cache_entries_.erase(frame_id);
    curr_size_--;
  }
}
auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> locker(latch_);
  return curr_size_;
}

}  // namespace bustub
