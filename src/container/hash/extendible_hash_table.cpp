//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.push_back(std::make_shared<ExtendibleHashTable<K, V>::Bucket>(bucket_size_, 0));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t dir_idx = IndexOf(key);
  std::shared_ptr<ExtendibleHashTable<K, V>::Bucket> bucket_ptr = dir_[dir_idx];
  return bucket_ptr->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t dir_idx = IndexOf(key);
  std::shared_ptr<ExtendibleHashTable<K, V>::Bucket> bucket_ptr = dir_[dir_idx];
  return bucket_ptr->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t dir_idx = IndexOf(key);
  std::shared_ptr<ExtendibleHashTable<K, V>::Bucket> bucket_ptr = dir_[dir_idx];
  //  1. If the local depth of the bucket is equal to the global depth,
  //        increment the global depth and double the size of the directory.
  while (!bucket_ptr->Insert(key, value)) {
    int local_depth = bucket_ptr->GetDepth();
    if (local_depth == global_depth_) {
      // double the size of the directory
      global_depth_ += 1;
      dir_.resize(dir_.size() * 2);
      // copy the first half of the directory to the second half
      std::copy(dir_.begin(), dir_.begin() + dir_.size() / 2, dir_.begin() + dir_.size() / 2);
    }
    bucket_ptr->IncrementDepth();
    RedistributeBucket(bucket_ptr);
    dir_idx = IndexOf(key);
    bucket_ptr = dir_[dir_idx];
  }
}
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket) -> void {
  // Increment the local depth of the bucket.
  size_t local_mask = 1 << (bucket->GetDepth() - 1);
  // Split the bucket and redistribute directory pointers & the kv pairs in the bucket.
  std::shared_ptr<Bucket> bucket1 = std::make_shared<Bucket>(bucket_size_, bucket->GetDepth());  // 0
  std::shared_ptr<Bucket> bucket2 = std::make_shared<Bucket>(bucket_size_, bucket->GetDepth());  // 1
  num_buckets_ += 1;
  for (const auto &[k, v] : bucket->GetItems()) {
    size_t idx = IndexOf(k);
    if ((idx & local_mask) == local_mask) {
      bucket2->Insert(k, v);
    } else {
      bucket1->Insert(k, v);
    }
  }
  for (size_t i = 0; i < dir_.size(); i++) {
    if (dir_[i] == bucket) {
      if ((i & local_mask) == local_mask) {
        dir_[i] = bucket2;
      } else {
        dir_[i] = bucket1;
      }
    }
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  auto position =
      std::find_if(std::begin(list_), std::end(list_), [key](const auto &item) { return key == item.first; });
  // exist
  if (position != std::end(list_)) {
    value = position->second;
    return true;
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  auto position =
      std::find_if(std::begin(list_), std::end(list_), [key](const auto &item) { return key == item.first; });
  // exist
  if (position != std::end(list_)) {
    list_.erase(position);
    return true;
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  auto position =
      std::find_if(std::begin(list_), std::end(list_), [key](const auto &item) { return key == item.first; });
  // exist
  if (position != std::end(list_)) {
    position->second = value;
    return true;
  }
  // full
  if (IsFull()) {
    return false;
  }
  list_.emplace_back(key, value);
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
