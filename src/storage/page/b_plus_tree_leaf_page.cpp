//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetSize(0);
  SetMaxSize(max_size);
  SetPageType(IndexPageType::LEAF_PAGE);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "index out of bound");
  return array_[index].first;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) const -> const MappingType & {
  // replace with your own code
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "index out of bound");
  return array_[index];
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const
    -> std::optional<int> {
  // find the key
  auto target = std::lower_bound(array_, array_ + GetSize(), key, [&](const MappingType &pair, const KeyType &key) {
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
auto B_PLUS_TREE_LEAF_PAGE_TYPE::LookUp(const KeyType &key, const KeyComparator &comparator) const
    -> std::optional<ValueType> {
  auto index = KeyIndex(key, comparator);
  if (!index.has_value()) {
    return std::nullopt;
  }
  return array_[index.value()].second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> bool {
  // if page is full, return false
  if (GetSize() == GetMaxSize()) {
    return false;
  }
  // find the first key that is greater than or equal to the key
  auto target = std::lower_bound(array_, array_ + GetSize(), key, [&](const MappingType &pair, const KeyType &key) {
    return comparator(pair.first, key) < 0;
  });
  int index = std::distance(array_, target);
  // if key already exists, return false
  if (index != GetSize() && comparator(array_[index].first, key) == 0) {
    return false;
  }

  // move the elements to make space for the new element
  std::move_backward(target, array_ + GetSize(), array_ + GetSize() + 1);
  // insert the new element
  array_[index] = {key, value};
  // update the size
  IncreaseSize(1);
  return true;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(const KeyType &key, const KeyComparator &comparator) -> bool {
  // find the key
  auto target = std::lower_bound(array_, array_ + GetSize(), key, [&](const MappingType &pair, const KeyType &key) {
    return comparator(pair.first, key) < 0;
  });
  int index = std::distance(array_, target);
  // if key not exist, return false
  if (index == GetSize() || comparator(array_[index].first, key) != 0) {
    return false;
  }
  // move the elements to cover the removed element
  std::move(target + 1, array_ + GetSize(), target);
  // update the size
  IncreaseSize(-1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::BorrowFromRight(BPlusTreeLeafPage *right_sibling_node) {
  // get the first element
  BUSTUB_ASSERT(right_sibling_node->GetSize() > 0, "no element to move");
  MappingType first = right_sibling_node->array_[0];
  // move the elements
  std::move_backward(right_sibling_node->array_ + 1, right_sibling_node->array_ + right_sibling_node->GetSize(),
                     right_sibling_node->array_ + right_sibling_node->GetSize() - 1);
  array_[GetSize()] = first;
  // update the size
  right_sibling_node->IncreaseSize(-1);
  IncreaseSize(1);
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::BorrowFromLeft(BPlusTreeLeafPage *left_sibling_node) {
  // get the last element
  BUSTUB_ASSERT(left_sibling_node->GetSize() > 0, "no element to move");
  MappingType last = left_sibling_node->array_[left_sibling_node->GetSize() - 1];
  // move the elements
  std::move_backward(array_, array_ + GetSize(), array_ + GetSize() + 1);
  array_[0] = last;
  // update the size
  left_sibling_node->IncreaseSize(-1);
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {
  // calculate the number of elements to move
  int move_num = GetMaxSize() - GetMinSize();
  // move the elements
  std::move_backward(array_ + GetMinSize(), array_ + GetMaxSize(), recipient->array_ + recipient->GetSize() + move_num);
  // update the size
  IncreaseSize(-move_num);
  recipient->IncreaseSize(move_num);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Split(BPlusTreeLeafPage *new_node) {
  // calculate the number of elements to move
  int move_num = GetMaxSize() - GetMinSize();
  // move the elements
  std::move_backward(array_ + GetMinSize(), array_ + GetMaxSize(), new_node->array_ + new_node->GetSize() + move_num);
  // update the size
  IncreaseSize(-move_num);
  new_node->IncreaseSize(move_num);
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MergeFromRight(BPlusTreeLeafPage *right_sibling_node) {
  // calculate the number of elements to move
  int move_num = right_sibling_node->GetSize();
  std::move_backward(right_sibling_node->array_, right_sibling_node->array_ + right_sibling_node->GetSize(),
                     array_ + GetSize() + move_num);
  // update the size
  IncreaseSize(move_num);
  right_sibling_node->IncreaseSize(-move_num);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
