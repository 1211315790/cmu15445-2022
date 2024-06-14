//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <algorithm>
#include <any>
#include <memory>

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    // The transaction is required to take only IX, X locks.(READ_UNCOMMITTED)
    if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
        lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
    // X/IX locks on rows are not allowed if the the Transaction State is SHRINKING
    if (txn->GetState() == TransactionState::SHRINKING &&
        (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  // Only IS, S locks are allowed in the SHRINKING state(READ_COMMITED)
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (txn->GetState() == TransactionState::SHRINKING && lock_mode != LockMode::INTENTION_SHARED &&
        lock_mode != LockMode::SHARED) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  // No locks are allowed in the SHRINKING state(REPEATABLE_READ)
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  std::shared_ptr<LockRequestQueue> lock_request_queue;
  {
    std::lock_guard<std::mutex> lk(table_lock_map_latch_);
    table_lock_map_.try_emplace(oid, std::make_shared<LockRequestQueue>());
    lock_request_queue = table_lock_map_[oid];
  }
  std::unique_lock<std::mutex> lock_request_queue_lck(lock_request_queue->latch_);
  bool upgrade = false;
  // if the request queue on the table have the same txn_id_, then upgrade the lock
  for (const auto &request : lock_request_queue->request_queue_) {
    if (request->txn_id_ != txn->GetTransactionId()) {
      continue;
    }
    // if the transaction already has the lock
    if (request->lock_mode_ == lock_mode) {
      return true;
    }
    // if other transactions are upgrading lock, then throw UPGRADE_CONFICT
    if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    }
    // if the transaction is downgrading lock, then throw INCOMPATIBLE_UPGRADE
    if (!CheckUpgradeValid(request->lock_mode_, lock_mode)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    }
    // remove the request from the queue
    DeleteTableLockSet(txn, request);
    lock_request_queue->request_queue_.remove(request);
    upgrade = true;
    break;
  }
  if (upgrade) {
    // A lock request being upgraded should be prioritised over other waiting lock requests on the same resource.
    auto upgrade_lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
    auto pos = std::find_if(lock_request_queue->request_queue_.begin(), lock_request_queue->request_queue_.end(),
                            [&](const auto &request) { return !request->granted_; });
    lock_request_queue->request_queue_.insert(pos, upgrade_lock_request);
    lock_request_queue->upgrading_ = txn->GetTransactionId();

    while (!GrantLock(upgrade_lock_request, lock_request_queue)) {
      lock_request_queue->cv_.wait(lock_request_queue_lck);
      if (txn->GetState() == TransactionState::ABORTED) {
        lock_request_queue->upgrading_ = INVALID_TXN_ID;
        lock_request_queue->request_queue_.remove(upgrade_lock_request);
        lock_request_queue->cv_.notify_all();
        return false;
      }
    }

    lock_request_queue->upgrading_ = INVALID_TXN_ID;
    upgrade_lock_request->granted_ = true;
    InsertTableLockSet(txn, upgrade_lock_request);
  } else {
    // 不需要升级锁，正常加锁
    auto lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
    lock_request_queue->request_queue_.push_back(lock_request);

    while (!GrantLock(lock_request, lock_request_queue)) {
      lock_request_queue->cv_.wait(lock_request_queue_lck);
      if (txn->GetState() == TransactionState::ABORTED) {
        lock_request_queue->request_queue_.remove(lock_request);
        lock_request_queue->cv_.notify_all();
        return false;
      }
    }
    lock_request->granted_ = true;
    InsertTableLockSet(txn, lock_request);
  }
  if (lock_mode != LockMode::EXCLUSIVE) {
    lock_request_queue->cv_.notify_all();
  }
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  std::shared_ptr<LockRequestQueue> lock_request_queue;
  {
    std::lock_guard<std::mutex> lck(table_lock_map_latch_);
    //  ensure that the transaction currently holds a lock on the resource it is attempting to unlock.
    if (table_lock_map_.find(oid) == table_lock_map_.end()) {
      txn->SetState(TransactionState::ABORTED);
      throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    }
    auto s_row_lock_set = txn->GetSharedRowLockSet();
    auto x_row_lock_set = txn->GetExclusiveRowLockSet();
    /* unlocking a table should only be allowed if the transaction does not hold locks on any row on that table */
    if (!(s_row_lock_set->find(oid) == s_row_lock_set->end() || s_row_lock_set->at(oid).empty()) ||
        !(x_row_lock_set->find(oid) == x_row_lock_set->end() || x_row_lock_set->at(oid).empty())) {
      txn->SetState(TransactionState::ABORTED);
      throw bustub::TransactionAbortException(txn->GetTransactionId(),
                                              AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
    }
    lock_request_queue = table_lock_map_[oid];
  }
  std::unique_lock<std::mutex> lock_request_queue_lck(lock_request_queue->latch_);

  for (const auto &lock_request : lock_request_queue->request_queue_) {  // NOLINT
    if (lock_request->txn_id_ == txn->GetTransactionId() && lock_request->granted_) {
      if ((txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ &&
           (lock_request->lock_mode_ == LockMode::SHARED || lock_request->lock_mode_ == LockMode::EXCLUSIVE)) ||
          (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&
           lock_request->lock_mode_ == LockMode::EXCLUSIVE) ||
          (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED &&
           lock_request->lock_mode_ == LockMode::EXCLUSIVE)) {
        if (txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED) {
          txn->SetState(TransactionState::SHRINKING);
        }
      }
      DeleteTableLockSet(txn, lock_request);
      lock_request_queue->request_queue_.remove(lock_request);
      lock_request_queue->cv_.notify_all();
      return true;
    }
  }

  txn->SetState(TransactionState::ABORTED);
  throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  // Row locking should not support Intention locks
  if (lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::INTENTION_SHARED ||
      lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }

  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    // The transaction is required to take only IX, X locks.(READ_UNCOMMITTED)
    if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
        lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
    // X/IX locks on rows are not allowed if the the Transaction State is SHRINKING
    if (txn->GetState() == TransactionState::SHRINKING &&
        (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  // Only IS, S locks are allowed in the SHRINKING state(READ_COMMITED)
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (txn->GetState() == TransactionState::SHRINKING && lock_mode != LockMode::INTENTION_SHARED &&
        lock_mode != LockMode::SHARED) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  // No locks are allowed in the SHRINKING state(REPEATABLE_READ)
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  if (lock_mode == LockMode::EXCLUSIVE) {
    if (!txn->IsTableExclusiveLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid) &&
        !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }
  std::shared_ptr<LockRequestQueue> lock_request_queue;
  {
    std::lock_guard<std::mutex> lck(row_lock_map_latch_);
    row_lock_map_.try_emplace(rid, std::make_shared<LockRequestQueue>());
    lock_request_queue = row_lock_map_[rid];
  }
  std::unique_lock<std::mutex> lock_request_queue_lck(lock_request_queue->latch_);

  bool upgrade{false};
  // if the request queue on the table have the same txn_id_, then upgrade the lock
  for (const auto &request : lock_request_queue->request_queue_) {
    if (request->txn_id_ != txn->GetTransactionId()) {
      continue;
    }
    // if the transaction already has the lock
    if (request->lock_mode_ == lock_mode) {
      return true;
    }
    // if other transactions are upgrading lock, then throw UPGRADE_CONFICT
    if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    }
    // if the transaction is downgrading lock, then throw INCOMPATIBLE_UPGRADE
    if (!CheckUpgradeValid(request->lock_mode_, lock_mode)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    }
    // remove the request from the queue
    DeleteRowLockSet(txn, request);
    lock_request_queue->request_queue_.remove(request);
    upgrade = true;
    break;
  }
  if (upgrade) {
    // A lock request being upgraded should be prioritised over other waiting lock requests on the same resource.
    auto upgrade_lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
    auto pos = std::find_if(lock_request_queue->request_queue_.begin(), lock_request_queue->request_queue_.end(),
                            [&](const auto &request) { return !request->granted_; });
    lock_request_queue->request_queue_.insert(pos, upgrade_lock_request);
    lock_request_queue->upgrading_ = txn->GetTransactionId();

    while (!GrantLock(upgrade_lock_request, lock_request_queue)) {
      lock_request_queue->cv_.wait(lock_request_queue_lck);
      if (txn->GetState() == TransactionState::ABORTED) {
        lock_request_queue->upgrading_ = INVALID_TXN_ID;
        lock_request_queue->request_queue_.remove(upgrade_lock_request);
        lock_request_queue->cv_.notify_all();
        return false;
      }
    }

    lock_request_queue->upgrading_ = INVALID_TXN_ID;
    upgrade_lock_request->granted_ = true;
    InsertRowLockSet(txn, upgrade_lock_request);
  } else {
    // 不需要升级锁，正常加锁
    auto lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
    lock_request_queue->request_queue_.push_back(lock_request);

    while (!GrantLock(lock_request, lock_request_queue)) {
      lock_request_queue->cv_.wait(lock_request_queue_lck);
      if (txn->GetState() == TransactionState::ABORTED) {
        lock_request_queue->request_queue_.remove(lock_request);
        lock_request_queue->cv_.notify_all();
        return false;
      }
    }
    lock_request->granted_ = true;
    InsertRowLockSet(txn, lock_request);
  }
  if (lock_mode != LockMode::EXCLUSIVE) {
    lock_request_queue->cv_.notify_all();
  }
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  std::shared_ptr<LockRequestQueue> lock_request_queue;
  {
    std::lock_guard<std::mutex> lck(row_lock_map_latch_);
    //  ensure that the transaction currently holds a lock on the resource it is attempting to unlock.
    if (row_lock_map_.find(rid) == row_lock_map_.end()) {
      txn->SetState(TransactionState::ABORTED);
      throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    }
    lock_request_queue = row_lock_map_[rid];
  }
  std::unique_lock<std::mutex> lock_request_queue_lck(lock_request_queue->latch_);

  for (const auto &lock_request : lock_request_queue->request_queue_) {  // NOLINT
    if (lock_request->txn_id_ == txn->GetTransactionId() && lock_request->granted_) {
      if ((txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ &&
           (lock_request->lock_mode_ == LockMode::SHARED || lock_request->lock_mode_ == LockMode::EXCLUSIVE)) ||
          (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&
           lock_request->lock_mode_ == LockMode::EXCLUSIVE) ||
          (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED &&
           lock_request->lock_mode_ == LockMode::EXCLUSIVE)) {
        if (txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED) {
          txn->SetState(TransactionState::SHRINKING);
        }
      }
      DeleteRowLockSet(txn, lock_request);
      lock_request_queue->request_queue_.remove(lock_request);
      lock_request_queue->cv_.notify_all();
      return true;
    }
  }

  txn->SetState(TransactionState::ABORTED);
  throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  txn_set_.insert(t1);
  txn_set_.insert(t2);
  waits_for_[t1].push_back(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  if (waits_for_.find(t1) == waits_for_.end()) {
    return;
  }
  if (auto iter = std::find(waits_for_[t1].begin(), waits_for_[t1].end(), t2); iter != waits_for_[t1].end()) {
    waits_for_[t1].erase(iter);
  }
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  auto found = std::any_of(txn_set_.begin(), txn_set_.end(), [this, &txn_id](const auto &start_txn_id) {
    visited_txn_.clear();
    if (Dfs(start_txn_id)) {
      *txn_id = *std::max_element(visited_txn_.begin(), visited_txn_.end());
      return true;
    }
    return false;
  });

  return found;
}
auto LockManager::DeleteNode(txn_id_t txn_id) -> void {
  waits_for_.erase(txn_id);

  for (auto a_txn_id : txn_set_) {
    if (a_txn_id != txn_id) {
      RemoveEdge(a_txn_id, txn_id);
    }
  }
}
auto LockManager::Dfs(txn_id_t txn_id) -> bool {
  if (safe_set_.find(txn_id) != safe_set_.end()) {
    return false;
  }
  visited_txn_.insert(txn_id);

  std::vector<txn_id_t> &neighbor_node = waits_for_[txn_id];
  std::sort(neighbor_node.begin(), neighbor_node.end());
  for (auto next_node : neighbor_node) {
    if (visited_txn_.find(next_node) != visited_txn_.end()) {
      return true;
    }
    if (Dfs(next_node)) {
      return true;
    }
  }

  visited_txn_.erase(txn_id);
  safe_set_.insert(txn_id);
  return false;
}
auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> result;
  for (auto const &pair : waits_for_) {
    auto t1 = pair.first;
    for (auto const &t2 : pair.second) {
      result.emplace_back(t1, t2);
    }
  }
  return result;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    // TODO(students): detect deadlock
    {
      std::lock_guard<std::mutex> lck(table_lock_map_latch_);
      for (const auto &[table_id, lock_request_queue] : table_lock_map_) {
        std::unordered_set<txn_id_t> granted_set;
        std::lock_guard<std::mutex> lck(lock_request_queue->latch_);
        for (const auto &lock_request : lock_request_queue->request_queue_) {
          if (lock_request->granted_) {
            granted_set.emplace(lock_request->txn_id_);
          } else {
            for (auto txn_id : granted_set) {
              map_txn_oid_.emplace(lock_request->txn_id_, lock_request->oid_);
              AddEdge(lock_request->txn_id_, txn_id);
            }
          }
        }
      }
    }
    {
      std::lock_guard<std::mutex> lck(row_lock_map_latch_);
      for (const auto &[rid, lock_request_queue] : row_lock_map_) {
        std::unordered_set<txn_id_t> granted_set;
        std::lock_guard<std::mutex> lck(lock_request_queue->latch_);
        for (const auto &lock_request : lock_request_queue->request_queue_) {
          if (lock_request->granted_) {
            granted_set.emplace(lock_request->txn_id_);
          } else {
            for (auto txn_id : granted_set) {
              map_txn_rid_.emplace(lock_request->txn_id_, lock_request->rid_);
              AddEdge(lock_request->txn_id_, txn_id);
            }
          }
        }
      }
    }
    txn_id_t txn_id;
    while (HasCycle(&txn_id)) {
      Transaction *txn = TransactionManager::GetTransaction(txn_id);
      txn->SetState(TransactionState::ABORTED);
      DeleteNode(txn_id);

      if (map_txn_oid_.count(txn_id) > 0) {
        table_lock_map_[map_txn_oid_[txn_id]]->cv_.notify_all();
      }

      if (map_txn_rid_.count(txn_id) > 0) {
        row_lock_map_[map_txn_rid_[txn_id]]->cv_.notify_all();
      }
    }

    waits_for_.clear();
    safe_set_.clear();
    txn_set_.clear();
    map_txn_oid_.clear();
    map_txn_rid_.clear();
  }
}
auto LockManager::InsertTableLockSet(Transaction *txn, const std::shared_ptr<LockRequest> &lock_request) -> void {
  switch (lock_request->lock_mode_) {
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->insert(lock_request->oid_);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->insert(lock_request->oid_);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->insert(lock_request->oid_);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->insert(lock_request->oid_);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->insert(lock_request->oid_);
      break;
  }
}
auto LockManager::DeleteTableLockSet(Transaction *txn, const std::shared_ptr<LockRequest> &lock_request) -> void {
  switch (lock_request->lock_mode_) {
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->erase(lock_request->oid_);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->erase(lock_request->oid_);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->erase(lock_request->oid_);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->erase(lock_request->oid_);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->erase(lock_request->oid_);
      break;
  }
}
auto LockManager::InsertRowLockSet(Transaction *txn, const std::shared_ptr<LockRequest> &lock_request) -> void {
  auto s_row_lock_set = txn->GetSharedRowLockSet();
  auto x_row_lock_set = txn->GetExclusiveRowLockSet();
  table_oid_t oid = lock_request->oid_;
  RID rid = lock_request->rid_;
  switch (lock_request->lock_mode_) {
    case LockMode::SHARED:
      s_row_lock_set->try_emplace(oid, std::unordered_set<RID>());
      s_row_lock_set->at(oid).insert(rid);
      break;
    case LockMode::EXCLUSIVE:
      x_row_lock_set->try_emplace(oid, std::unordered_set<RID>());
      x_row_lock_set->at(oid).insert(rid);
      break;
    case LockMode::INTENTION_SHARED:
    case LockMode::INTENTION_EXCLUSIVE:
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      break;
  }
}
auto LockManager::DeleteRowLockSet(Transaction *txn, const std::shared_ptr<LockRequest> &lock_request) -> void {
  auto s_row_lock_set = txn->GetSharedRowLockSet();
  auto x_row_lock_set = txn->GetExclusiveRowLockSet();
  table_oid_t oid = lock_request->oid_;
  RID rid = lock_request->rid_;
  switch (lock_request->lock_mode_) {
    case LockMode::SHARED:
      if (s_row_lock_set->find(oid) != s_row_lock_set->end()) {
        s_row_lock_set->at(oid).erase(rid);
      }
      break;
    case LockMode::EXCLUSIVE:
      if (x_row_lock_set->find(oid) != x_row_lock_set->end()) {
        x_row_lock_set->at(oid).erase(rid);
      }
      break;
    case LockMode::INTENTION_SHARED:
    case LockMode::INTENTION_EXCLUSIVE:
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      break;
  }
}
auto LockManager::CheckUpgradeValid(LockMode current_mode, LockMode upgrade_mode) -> bool {
  /**
   *  While upgrading, only the following transitions should be allowed:
   *        IS -> [S, X, IX, SIX]
   *        S -> [X, SIX]
   *        IX -> [X, SIX]
   *        SIX -> [X]
   */
  switch (current_mode) {
    case LockMode::INTENTION_SHARED:
      return upgrade_mode == LockMode::SHARED || upgrade_mode == LockMode::EXCLUSIVE ||
             upgrade_mode == LockMode::INTENTION_EXCLUSIVE || upgrade_mode == LockMode::SHARED_INTENTION_EXCLUSIVE;
    case LockMode::SHARED:
    case LockMode::INTENTION_EXCLUSIVE:
      return upgrade_mode == LockMode::EXCLUSIVE || upgrade_mode == LockMode::SHARED_INTENTION_EXCLUSIVE;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      return upgrade_mode == LockMode::EXCLUSIVE;
    default:
      return false;
  }
}

auto LockManager::GrantLock(const std::shared_ptr<LockRequest> &lock_request,
                            const std::shared_ptr<LockRequestQueue> &lock_request_queue) -> bool {
  /**
   *    IS	IX	S	 SIX	X
   * IS	 ✔ 	✔ 	✔ 	✔ 	✖
   * IX	 ✔ 	✔ 	✖ 	✖ 	✖
   * S 	 ✔ 	✖ 	✔ 	✖ 	✖
   * SIX ✔	✖	  ✖ 	✖ 	✖
   * X	 ✖	✖	  ✖ 	✖ 	✖
   */
  for (const auto &lr : lock_request_queue->request_queue_) {
    if (lr->granted_) {
      // 当前的锁与前面的锁(granted_=1)冲突
      switch (lock_request->lock_mode_) {
        case LockMode::SHARED:
          if (lr->lock_mode_ == LockMode::INTENTION_EXCLUSIVE ||
              lr->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE || lr->lock_mode_ == LockMode::EXCLUSIVE) {
            return false;
          }
          break;
        case LockMode::EXCLUSIVE:
          return false;
          break;
        case LockMode::INTENTION_SHARED:
          if (lr->lock_mode_ == LockMode::EXCLUSIVE) {
            return false;
          }
          break;
        case LockMode::INTENTION_EXCLUSIVE:
          if (lr->lock_mode_ == LockMode::SHARED || lr->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE ||
              lr->lock_mode_ == LockMode::EXCLUSIVE) {
            return false;
          }
          break;
        case LockMode::SHARED_INTENTION_EXCLUSIVE:
          if (lr->lock_mode_ != LockMode::INTENTION_SHARED) {
            return false;
          }
          break;
      }
    } else if (lock_request.get() != lr.get()) {
      // 当前的锁与前面的锁(granted_=1)都不冲突，但是前面的锁(granted_=0)还没有被授予
      // FIFO order
      return false;
    } else {
      return true;
    }
  }
  return false;
}

}  // namespace bustub
