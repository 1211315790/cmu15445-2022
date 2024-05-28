//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), child_executor_(std::move(child_executor)) {
  plan_ = plan;
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  table_indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

void DeleteExecutor::Init() { child_executor_->Init(); }

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_end_) {
    return false;
  }
  Tuple to_delete_tuple{};
  RID emit_rid;
  int32_t delete_count = 0;
  while (child_executor_->Next(&to_delete_tuple, &emit_rid)) {
    auto deleted = table_info_->table_->MarkDelete(emit_rid, exec_ctx_->GetTransaction());
    if (deleted) {
      auto update_index = [&](IndexInfo *index_info) {
        auto index_key = to_delete_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_,
                                                      index_info->index_->GetKeyAttrs());
        index_info->index_->DeleteEntry(index_key, *rid, exec_ctx_->GetTransaction());
      };
      std::for_each(std::begin(table_indexes_), std::end(table_indexes_), update_index);  // 更新索引
    } else {
      LOG_ERROR("delete failed!");
    }

    delete_count++;
  }
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(TypeId::INTEGER, delete_count);
  *tuple = Tuple{values, &GetOutputSchema()};
  is_end_ = true;
  return true;
}
}  // namespace bustub
