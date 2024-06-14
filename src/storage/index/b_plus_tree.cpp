#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  // 对当前树加锁
  root_latch_.RLock();
  // find the leaf page that contains the key
  Page *page = FindLeaf(key, OperationType::SEARCH, transaction);
  if (page == nullptr) {
    return false;
  }
  auto leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  std::optional<ValueType> ret = leaf_page->LookUp(key, comparator_);
  page->RUnlatch();
  BUSTUB_ASSERT(buffer_pool_manager_->UnpinPage(page->GetPageId(), false), "Unpin failed");
  if (ret.has_value()) {
    result->push_back(ret.value());
    return true;
  }
  return false;
}

// find the leaf page that contains the key
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeaf(const KeyType &key, OperationType operation_type, Transaction *transaction,
                              bool leaf_most, bool right_most) -> Page * {
  BUSTUB_ASSERT(operation_type == OperationType::SEARCH ? !(leaf_most && right_most) : transaction != nullptr,
                "Invalid input");

  if (IsEmpty()) {
    return nullptr;
  }

  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  if (page == nullptr) {
    return nullptr;
  }
  auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  if (operation_type == OperationType::SEARCH) {
    page->RLatch();
    root_latch_.RUnlock();
  } else {
    page->WLatch();
    if (IsSafe(operation_type, node)) {
      ReleaseLatchFromQueue(transaction);
    }
  }

  page_id_t child_id;
  while (!node->IsLeafPage()) {
    auto internal_node = reinterpret_cast<InternalPage *>(node);
    if (leaf_most) {
      child_id = internal_node->ValueAt(0);
    } else if (right_most) {
      child_id = internal_node->ValueAt(internal_node->GetSize() - 1);
    } else {
      std::optional<page_id_t> ret = internal_node->FindKeyInPageId(key, comparator_);
      BUSTUB_ASSERT(ret.has_value(), "FindKeyInPageId failed");
      child_id = ret.value();
    }

    Page *child_page = buffer_pool_manager_->FetchPage(child_id);
    auto child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    if (operation_type == OperationType::SEARCH) {
      child_page->RLatch();
      page->RUnlatch();
      BUSTUB_ASSERT(buffer_pool_manager_->UnpinPage(page->GetPageId(), false), "Unpin failed");
    } else if (operation_type == OperationType::INSERT || operation_type == OperationType::DELETE) {
      child_page->WLatch();
      transaction->AddIntoPageSet(page);
      if (IsSafe(operation_type, child_node)) {
        ReleaseLatchFromQueue(transaction);
      }
    }

    node = child_node;
    page = child_page;
  }
  return page;
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsSafe(OperationType operation_type, BPlusTreePage *node) -> bool {
  if (operation_type == OperationType::SEARCH) {
    return true;
  }
  if (operation_type == OperationType::INSERT) {
    if (node->IsLeafPage() && node->GetSize() < node->GetMaxSize() - 1) {
      return true;
    }
    if (!node->IsLeafPage() && node->GetSize() < node->GetMaxSize()) {
      return true;
    }
    return false;
  }
  if (operation_type == OperationType::DELETE) {
    if (node->IsRootPage() && node->GetSize() > 2) {
      return true;
    }
    if (!node->IsRootPage() && node->GetSize() > node->GetMinSize()) {
      return true;
    }
    return false;
  }
  // not go here
  exit(EXIT_FAILURE);
  return false;
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  root_latch_.WLock();
  transaction->AddIntoPageSet(nullptr);  // nullptr means root latch
  if (IsEmpty()) {
    Page *page = buffer_pool_manager_->NewPage(&root_page_id_);
    BUSTUB_ASSERT(page != nullptr, "Cannot allocate new page");
    auto root_page = reinterpret_cast<LeafPage *>(page->GetData());
    root_page->Init(page->GetPageId(), INVALID_PAGE_ID, leaf_max_size_);
    BUSTUB_ASSERT(root_page->Insert(key, value, comparator_), "Insert failed");
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    // UpdateRootPageId(1);
    ReleaseLatchFromQueue(transaction);
    return true;
  }
  Page *leaf_page = FindLeaf(key, OperationType::INSERT, transaction);
  if (leaf_page == nullptr) {
    return false;
  }
  auto leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());

  // duplicate key
  if (bool ret = leaf_node->Insert(key, value, comparator_); !ret) {
    ReleaseLatchFromQueue(transaction);
    leaf_page->WUnlatch();
    BUSTUB_ASSERT(buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false), "Unpin failed");
    return false;
  }

  // leaf is not full
  if (leaf_node->GetSize() < leaf_max_size_) {
    ReleaseLatchFromQueue(transaction);
    leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    return true;
  }

  // leaf is full, need to split
  auto sibling_leaf_node = Split(leaf_node);

  sibling_leaf_node->SetNextPageId(leaf_node->GetNextPageId());
  leaf_node->SetNextPageId(sibling_leaf_node->GetPageId());

  auto smallest_key = sibling_leaf_node->KeyAt(0);
  InsertIntoParent(leaf_node, smallest_key, sibling_leaf_node, transaction);
  leaf_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(sibling_leaf_node->GetPageId(), true);
  return true;
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseLatchFromQueue(Transaction *transaction) {
  while (!transaction->GetPageSet()->empty()) {
    Page *page = transaction->GetPageSet()->front();
    transaction->GetPageSet()->pop_front();
    if (page == nullptr) {
      this->root_latch_.WUnlock();
    } else {
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    }
  }
}
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::Split(N *node) -> N * {
  page_id_t page_id;
  Page *page = buffer_pool_manager_->NewPage(&page_id);
  BUSTUB_ASSERT(page != nullptr, "Cannot allocate new page");

  N *new_node = reinterpret_cast<N *>(page->GetData());

  if (node->IsLeafPage()) {
    new_node->SetPageType(IndexPageType::LEAF_PAGE);
    auto leaf = reinterpret_cast<LeafPage *>(node);
    auto new_leaf = reinterpret_cast<LeafPage *>(new_node);

    new_leaf->Init(page->GetPageId(), node->GetParentPageId(), leaf_max_size_);
    leaf->Split(new_leaf);
  } else {
    new_node->SetPageType(IndexPageType::INTERNAL_PAGE);
    auto internal = reinterpret_cast<InternalPage *>(node);
    auto new_internal = reinterpret_cast<InternalPage *>(new_node);

    new_internal->Init(page->GetPageId(), node->GetParentPageId(), internal_max_size_);
    internal->Split(new_internal, buffer_pool_manager_);
  }

  return new_node;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  if (old_node->IsRootPage()) {
    auto root_page = buffer_pool_manager_->NewPage(&root_page_id_);
    BUSTUB_ASSERT(root_page != nullptr, "Cannot allocate new page");

    auto new_root = reinterpret_cast<InternalPage *>(root_page->GetData());
    new_root->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
    new_root->SetSize(2);
    new_root->SetKeyAt(1, key);
    new_root->SetValueAt(0, old_node->GetPageId());
    new_root->SetValueAt(1, new_node->GetPageId());

    old_node->SetParentPageId(new_root->GetPageId());
    new_node->SetParentPageId(new_root->GetPageId());

    buffer_pool_manager_->UnpinPage(root_page->GetPageId(), true);

    UpdateRootPageId(0);
    ReleaseLatchFromQueue(transaction);
    return;
  }
  auto parent_page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
  auto parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());

  if (parent_node->GetSize() < internal_max_size_) {
    parent_node->Insert(key, new_node->GetPageId(), comparator_);
    ReleaseLatchFromQueue(transaction);
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    return;
  }
  /*split parent_node*/
  // copy parent_node
  // 如果直接插入有可能overflow
  std::unique_ptr<char[]> mem(new char[INTERNAL_PAGE_HEADER_SIZE + sizeof(MappingType) * (parent_node->GetSize() + 1)]);
  auto copy_parent_node = reinterpret_cast<InternalPage *>(mem.get());
  std::memcpy(mem.get(), parent_page->GetData(),
              INTERNAL_PAGE_HEADER_SIZE + sizeof(MappingType) * (parent_node->GetSize()));
  copy_parent_node->SetMaxSize(copy_parent_node->GetMaxSize() + 1);
  BUSTUB_ASSERT(copy_parent_node->Insert(key, new_node->GetPageId(), comparator_), "Insert failed");
  copy_parent_node->SetMaxSize(copy_parent_node->GetMaxSize() - 1);
  auto parent_new_sibling_node = Split(copy_parent_node);
  KeyType new_key = parent_new_sibling_node->KeyAt(0);
  std::memcpy(parent_page->GetData(), mem.get(),
              INTERNAL_PAGE_HEADER_SIZE + sizeof(MappingType) * copy_parent_node->GetMinSize());

  InsertIntoParent(parent_node, new_key, parent_new_sibling_node, transaction);

  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(parent_new_sibling_node->GetPageId(), true);
}
/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  root_latch_.WLock();
  transaction->AddIntoPageSet(nullptr);  // nullptr means root latch
  if (IsEmpty()) {
    ReleaseLatchFromQueue(transaction);
    return;
  }
  Page *leaf_page = FindLeaf(key, OperationType::DELETE, transaction);
  if (leaf_page == nullptr) {
    ReleaseLatchFromQueue(transaction);
    return;
  }
  auto leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  if (RemoveEntry(leaf_node, key, transaction)) {
    leaf_page->WUnlatch();
    BUSTUB_ASSERT(buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true), "Unpin failed");
  } else {  // key is existed
    leaf_page->WUnlatch();
    BUSTUB_ASSERT(buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false), "Unpin failed");
  }
  std::for_each(transaction->GetDeletedPageSet()->begin(), transaction->GetDeletedPageSet()->end(),
                [&](page_id_t page_id) {
                  // 有可能其他页刚刚WUnlatch, 但是还没有Unpin, 所以会删除失败
                  while (!buffer_pool_manager_->DeletePage(page_id)) {
                  }
                });
  transaction->GetDeletedPageSet()->clear();
}
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::RemoveEntry(N *node, const KeyType &key, Transaction *transaction) -> bool {
  // key not exists, return false
  if (!node->Remove(key, comparator_)) {
    ReleaseLatchFromQueue(transaction);
    return false;
  }

  if (node->IsRootPage()) {
    AdjustRoot(node, transaction);
    ReleaseLatchFromQueue(transaction);
    return true;
  }
  // 当前节点删除后满足最小key的条件
  if (node->GetSize() >= node->GetMinSize()) {
    ReleaseLatchFromQueue(transaction);
    return true;
  }
  Page *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  auto parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  int idx = parent_node->ValueIndex(node->GetPageId());
  BUSTUB_ASSERT(idx >= 0, "ValueIndex failed");
  Page *sibling_page;
  N *sibling_node;
  // 左邻居节点
  if (idx > 0) {
    sibling_page = buffer_pool_manager_->FetchPage(parent_node->ValueAt(idx - 1));
  } else {  // 右邻居节点
    sibling_page = buffer_pool_manager_->FetchPage(parent_node->ValueAt(1));
  }
  sibling_page->WLatch();
  sibling_node = reinterpret_cast<N *>(sibling_page->GetData());
  // borrow an entry from sibling_node
  if (sibling_node->GetSize() > sibling_node->GetMinSize()) {
    Redistribution(node, sibling_node, parent_node);
    ReleaseLatchFromQueue(transaction);
  } else {
    // merge
    Coalesce(node, sibling_node, parent_node, transaction);
  }
  BUSTUB_ASSERT(buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true), "Unpin failed");
  sibling_page->WUnlatch();
  BUSTUB_ASSERT(buffer_pool_manager_->UnpinPage(sibling_node->GetPageId(), true), "Unpin failed");
  return true;
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *node, Transaction *transaction) {
  // 当前节点为根节点且为叶子结点
  if (node->IsLeafPage() && node->GetSize() == 0) {
    root_page_id_ = INVALID_PAGE_ID;
    transaction->AddIntoDeletedPageSet(node->GetPageId());
    return;
  }
  // 当前节点为Internal 节点且只有一个孩子
  if (!node->IsLeafPage() && node->GetSize() == 1) {
    auto root_node = reinterpret_cast<InternalPage *>(node);
    Page *child_page = buffer_pool_manager_->FetchPage(root_node->ValueAt(0));
    auto only_child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    only_child_node->SetParentPageId(INVALID_PAGE_ID);

    root_page_id_ = only_child_node->GetPageId();

    UpdateRootPageId(0);

    BUSTUB_ASSERT(buffer_pool_manager_->UnpinPage(child_page->GetPageId(), true), "Unpin failed");

    transaction->AddIntoDeletedPageSet(node->GetPageId());
    return;
  }
}
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Coalesce(N *node, N *sibling_node, InternalPage *parent_node, Transaction *transaction) {
  // index 为 node 在 parent_node 中的索引
  int idx = parent_node->ValueIndex(node->GetPageId());
  BUSTUB_ASSERT(idx >= 0, "ValueIndex failed");
  KeyType middle_key;
  if (idx > 0) {
    std::swap(node, sibling_node);
    middle_key = parent_node->KeyAt(idx);
  } else {
    middle_key = parent_node->KeyAt(1);
  }
  // leafpage merge
  if (node->IsLeafPage()) {
    auto leaf_node = reinterpret_cast<LeafPage *>(node);
    auto right_sibling_node = reinterpret_cast<LeafPage *>(sibling_node);
    leaf_node->MergeFromRight(right_sibling_node);
    leaf_node->SetNextPageId(right_sibling_node->GetNextPageId());
  } else {
    // InternalPage merge
    auto internal_node = reinterpret_cast<InternalPage *>(node);
    auto right_sibling_node = reinterpret_cast<InternalPage *>(sibling_node);
    // InternalPage 的第0位是无效的
    right_sibling_node->SetKeyAt(0, middle_key);
    internal_node->MergeFromRight(right_sibling_node, buffer_pool_manager_);
  }
  transaction->AddIntoDeletedPageSet(sibling_node->GetPageId());
  // delete node from parent_node
  BUSTUB_ASSERT(RemoveEntry(parent_node, middle_key, transaction), "RemoveEntry failed");
}
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribution(N *node, N *sibling_node, InternalPage *parent_node) {
  // index 为 node 在 parent_node 中的索引
  int idx = parent_node->ValueIndex(node->GetPageId());
  BUSTUB_ASSERT(idx >= 0, "ValueIndex failed");
  bool left_sibling = idx > 0;
  auto middle_key = left_sibling ? parent_node->KeyAt(idx) : parent_node->KeyAt(1);

  // leafpage borrow
  if (node->IsLeafPage()) {
    auto leaf_node = reinterpret_cast<LeafPage *>(node);
    auto sibling_leaf_node = reinterpret_cast<LeafPage *>(sibling_node);
    // borrow an entry from sibling_node
    if (left_sibling) {
      leaf_node->BorrowFromLeft(sibling_leaf_node);
      parent_node->SetKeyAt(idx, leaf_node->KeyAt(0));
    } else {
      leaf_node->BorrowFromRight(sibling_leaf_node);
      parent_node->SetKeyAt(1, sibling_node->KeyAt(0));
    }
  } else {
    // internalpage borrow
    auto internal_node = reinterpret_cast<InternalPage *>(node);
    auto sibling_internal_node = reinterpret_cast<InternalPage *>(sibling_node);
    if (left_sibling) {
      internal_node->BorrowFromLeft(sibling_internal_node, middle_key, buffer_pool_manager_);
      parent_node->SetKeyAt(idx, internal_node->KeyAt(0));
    } else {
      internal_node->BorrowFromRight(sibling_internal_node, middle_key, buffer_pool_manager_);
      parent_node->SetKeyAt(1, sibling_internal_node->KeyAt(0));
    }
  }
}
/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  if (root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(nullptr, nullptr);
  }
  root_latch_.RLock();
  Page *page = FindLeaf(KeyType(), OperationType::SEARCH, nullptr, true, false);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, page, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  if (root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(nullptr, nullptr);
  }
  root_latch_.RLock();
  Page *page = FindLeaf(key, OperationType::SEARCH);
  auto leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  std::optional<int> index = leaf_page->KeyIndex(key, comparator_);
  BUSTUB_ASSERT(index.has_value(), "KeyIndex failed");
  return INDEXITERATOR_TYPE(buffer_pool_manager_, page, index.value());
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  if (root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(nullptr, nullptr);
  }
  root_latch_.RLock();
  Page *page = FindLeaf(KeyType(), OperationType::SEARCH, nullptr, false, true);
  auto leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  return INDEXITERATOR_TYPE(buffer_pool_manager_, page, leaf_page->GetSize());
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
