//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetSize(0);
  SetMaxSize(max_size);
  SetPageType(IndexPageType::INTERNAL_PAGE);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "index out of bound");
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "index out of bound");
  array_[index].first = key;
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  BUSTUB_ASSERT(index < GetSize(), "index out of bound");
  array_[index].second = value;
}
/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  // replace with your own code
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "index out of bound");
  return array_[index].second;
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "index out of bound");
  std::move(array_ + index + 1, array_ + GetSize(), array_ + index);
  IncreaseSize(-1);
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(const KeyType &key, const KeyComparator &comparator) -> bool {
  auto target = std::lower_bound(array_ + 1, array_ + GetSize(), key,
                                 [&comparator](const auto &kv, auto k) { return comparator(kv.first, k) < 0; });
  // if the key is not found, return
  if (target == array_ + GetSize() || comparator(target->first, key) != 0) {
    return false;
  }
  Remove(std::distance(array_, target));
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindKeyInPageId(const KeyType &key, const KeyComparator &comparator) const
    -> std::optional<ValueType> {
  // TODO(zsp): SIMD compare
  if (GetSize() == 0) {
    return std::nullopt;
  }
  // find the first key that is greater than or equal to the key
  auto target = std::lower_bound(array_ + 1, array_ + GetSize(), key,
                                 [&comparator](const auto &kv, auto k) { return comparator(kv.first, k) < 0; });
  // if all keys are less than key, return the last value
  if (target == array_ + GetSize()) {
    return ValueAt(GetSize() - 1);
  }
  // if the key in the array_, return the value.
  if (comparator(target->first, key) == 0) {
    return target->second;
  }
  return std::prev(target)->second;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> bool {
  // if page is full, return false
  if (GetSize() == GetMaxSize()) {
    return false;
  }
  // find the first key that is greater than or equal to the key
  auto target = std::lower_bound(array_ + 1, array_ + GetSize(), key,
                                 [&comparator](const auto &kv, auto k) { return comparator(kv.first, k) < 0; });
  int index = std::distance(array_, target);
  // if key exist, return false
  if (index != GetSize() && comparator(target->first, key) == 0) {
    return false;
  }
  // move the key-value pairs after the target to the right
  std::move_backward(target, array_ + GetSize(), array_ + GetSize() + 1);
  // insert the key-value pair
  array_[index] = {key, value};
  // increase the size
  IncreaseSize(1);
  return true;
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Split(BPlusTreeInternalPage *new_node, BufferPoolManager *buffer_pool_manager) {
  BUSTUB_ASSERT(buffer_pool_manager != nullptr, "buffer_pool_manager is nullptr");
  std::copy(array_ + GetMinSize(), array_ + GetSize(), new_node->array_ + new_node->GetSize());
  int move_num = GetSize() - GetMinSize();
  SetSize(GetMinSize());
  int newnode_original_size = new_node->GetSize();
  new_node->IncreaseSize(move_num);
  for (int i = 0; i < move_num; i++) {
    auto page = buffer_pool_manager->FetchPage(new_node->ValueAt(i + newnode_original_size));
    BUSTUB_ASSERT(page != nullptr, "page is nullptr");
    auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    node->SetParentPageId(new_node->GetPageId());
    buffer_pool_manager->UnpinPage(page->GetPageId(), true);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MergeFromRight(BPlusTreeInternalPage *right_sibling_node,
                                                    BufferPoolManager *buffer_pool_manager) {
  // move right_sibling_node's all key-value pairs to this page
  int move_num = right_sibling_node->GetSize();
  std::move_backward(right_sibling_node->array_, right_sibling_node->array_ + right_sibling_node->GetSize(),
                     array_ + GetSize() + move_num);
  right_sibling_node->SetSize(0);
  int origin_size = GetSize();
  IncreaseSize(move_num);
  if (buffer_pool_manager != nullptr) {
    for (int i = 0; i < move_num; i++) {
      Page *page = buffer_pool_manager->FetchPage(ValueAt(i + origin_size));
      BUSTUB_ASSERT(page != nullptr, "page is nullptr");
      auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());
      node->SetParentPageId(GetPageId());
      BUSTUB_ASSERT(buffer_pool_manager->UnpinPage(page->GetPageId(), true), "unpin page failed");
    }
  }
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::BorrowFromRight(BPlusTreeInternalPage *right_sibling, const KeyType &middle_key,
                                                     BufferPoolManager *buffer_pool_manager) {
  array_[GetSize()] = {middle_key, right_sibling->ValueAt(0)};
  IncreaseSize(1);
  Page *page = buffer_pool_manager->FetchPage(right_sibling->ValueAt(0));
  auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  node->SetParentPageId(GetPageId());
  BUSTUB_ASSERT(buffer_pool_manager->UnpinPage(page->GetPageId(), true), "unpin page failed");
  right_sibling->Remove(0);
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::BorrowFromLeft(BPlusTreeInternalPage *left_sibling, const KeyType &middle_key,
                                                    BufferPoolManager *buffer_pool_manager) {
  SetKeyAt(0, middle_key);
  std::move_backward(array_, array_ + GetSize(), array_ + GetSize() + 1);
  IncreaseSize(1);
  array_[0] = {left_sibling->array_[left_sibling->GetSize() - 1]};

  Page *page = buffer_pool_manager->FetchPage(left_sibling->ValueAt(left_sibling->GetSize() - 1));
  auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  node->SetParentPageId(GetPageId());
  BUSTUB_ASSERT(buffer_pool_manager->UnpinPage(page->GetPageId(), true), "unpin page failed");
  left_sibling->Remove(left_sibling->GetSize() - 1);
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int {
  auto it = std::find_if(array_, array_ + GetSize(), [&value](const auto &pair) { return pair.second == value; });
  return std::distance(array_, it);
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const
    -> std::optional<int> {
  // find the key
  auto target = std::lower_bound(array_ + 1, array_ + GetSize(), key, [&](const MappingType &pair, const KeyType &key) {
    return comparator(pair.first, key) < 0;
  });
  int index = std::distance(array_, target);
  // if the key is not found, return std::nullopt
  if (index == GetSize() || comparator(array_[index].first, key) != 0) {
    return std::nullopt;
  }
  return index;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) -> int {
  auto new_value_idx = ValueIndex(old_value) + 1;
  std::move_backward(array_ + new_value_idx, array_ + GetSize(), array_ + GetSize() + 1);

  array_[new_value_idx].first = new_key;
  array_[new_value_idx].second = new_value;

  IncreaseSize(1);

  return GetSize();
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
