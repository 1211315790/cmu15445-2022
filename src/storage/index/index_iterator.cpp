/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *buffer_pool_manager, Page *page, int index)
    : buffer_pool_manager_(buffer_pool_manager), page_(page), index_(index) {
  leaf_page_ = page == nullptr ? nullptr : reinterpret_cast<LeafPage *>(page->GetData());
}

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::~IndexIterator() {
  if (page_ != nullptr) {
    page_->RUnlatch();
    BUSTUB_ASSERT(buffer_pool_manager_->UnpinPage(page_->GetPageId(), false), "Unpin failed");
  }
}  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  return leaf_page_->GetNextPageId() == INVALID_PAGE_ID && index_ == leaf_page_->GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return leaf_page_->GetItem(index_); }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  int size = leaf_page_->GetSize();
  // 当前页还有元素
  if (++index_ < size) {
    return *this;
  }
  // 当前页没有元素了，找下一页
  page_id_t next_page_id = leaf_page_->GetNextPageId();
  if (next_page_id == INVALID_PAGE_ID) {
    // 没有下一页了
    return *this;
  }
  // 有下一页
  Page *next_page = buffer_pool_manager_->FetchPage(next_page_id);
  BUSTUB_ASSERT(next_page != nullptr, "FetchPage failed");
  next_page->RLatch();
  page_->RUnlatch();
  BUSTUB_ASSERT(buffer_pool_manager_->UnpinPage(page_->GetPageId(), false), "Unpin failed");

  page_ = next_page;
  leaf_page_ = reinterpret_cast<LeafPage *>(page_->GetData());
  index_ = 0;
  return *this;
}
INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const -> bool {
  if (page_ != nullptr && itr.page_ != nullptr) {
    return leaf_page_->GetPageId() == itr.leaf_page_->GetPageId() && index_ == itr.index_;
  }
  if (page_ == nullptr) {
    return itr.page_ == nullptr;
  }
  if (itr.page_ == nullptr) {
    return false;
  }
  // not go here
  return false;
}
INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const -> bool { return !(*this == itr); }
template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;
template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;
template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;
template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
