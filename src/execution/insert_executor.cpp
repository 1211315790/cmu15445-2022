//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_info_ = exec_ctx->GetCatalog()->GetTable(plan_->TableOid());
}

void InsertExecutor::Init() {
  child_executor_->Init();
  table_indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_end_) {
    return false;
  }
  Tuple to_insert_tuple{};
  RID emit_rid;
  int32_t insert_count = 0;

  while (child_executor_->Next(&to_insert_tuple, &emit_rid)) {
    bool inserted = table_info_->table_->InsertTuple(to_insert_tuple, rid, exec_ctx_->GetTransaction());  // 插入成功
    if (inserted) {
      auto update_index = [&](IndexInfo *index_info) {
        auto index_key = to_insert_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_,
                                                      index_info->index_->GetKeyAttrs());
        index_info->index_->InsertEntry(index_key, *rid, exec_ctx_->GetTransaction());
      };
      std::for_each(std::begin(table_indexes_), std::end(table_indexes_), update_index);  // 更新索引
      insert_count++;
    } else {
      LOG_ERROR("insert failed!");
    }
  }
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  // 输出插入的元组数量
  values.emplace_back(TypeId::INTEGER, insert_count);
  *tuple = Tuple{values, &GetOutputSchema()};
  is_end_ = true;
  return true;
}

}  // namespace bustub
