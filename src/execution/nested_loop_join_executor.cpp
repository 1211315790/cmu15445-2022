//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  switch (plan->GetJoinType()) {
    case JoinType::INNER:
    case JoinType::OUTER:
    case JoinType::LEFT:
      outer_executor_ = std::move(left_executor);
      inner_executor_ = std::move(right_executor);
      break;
    case JoinType::RIGHT:
      outer_executor_ = std::move(right_executor);
      inner_executor_ = std::move(left_executor);
      break;

    default:
      throw NotImplementedException("Join type not supported");
  }
}

void NestedLoopJoinExecutor::Init() {
  outer_executor_->Init();
  inner_executor_->Init();
  Tuple tuple{};
  RID rid{};
  while (inner_executor_->Next(&tuple, &rid)) {
    inner_tuples_.push_back(tuple);
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  RID emit_rid{};
  while (inner_tuple_idx_ != -1 || outer_executor_->Next(&out_tuple_, &emit_rid)) {
    std::vector<Value> vals;
    for (uint32_t ridx = (inner_tuple_idx_ < 0 ? 0 : inner_tuple_idx_); ridx < inner_tuples_.size(); ridx++) {
      auto &inner_tuple = inner_tuples_[ridx];
      if (Matched(&out_tuple_, &inner_tuple)) {
        for (uint32_t idx = 0; idx < outer_executor_->GetOutputSchema().GetColumnCount(); idx++) {
          vals.push_back(out_tuple_.GetValue(&outer_executor_->GetOutputSchema(), idx));
        }
        for (uint32_t idx = 0; idx < inner_executor_->GetOutputSchema().GetColumnCount(); idx++) {
          vals.push_back(inner_tuple.GetValue(&inner_executor_->GetOutputSchema(), idx));
        }
        *tuple = Tuple(vals, &GetOutputSchema());
        inner_tuple_idx_ = ridx + 1;
        inner_matched_idx_.insert(ridx);
        return true;
      }
    }
    if (inner_tuple_idx_ == -1 && plan_->GetJoinType() == JoinType::LEFT) {
      for (uint32_t idx = 0; idx < outer_executor_->GetOutputSchema().GetColumnCount(); idx++) {
        vals.push_back(out_tuple_.GetValue(&outer_executor_->GetOutputSchema(), idx));
      }
      for (uint32_t idx = 0; idx < inner_executor_->GetOutputSchema().GetColumnCount(); idx++) {
        vals.push_back(ValueFactory::GetNullValueByType(inner_executor_->GetOutputSchema().GetColumn(idx).GetType()));
      }
      *tuple = Tuple(vals, &GetOutputSchema());
      return true;
    }
    inner_tuple_idx_ = -1;
  }
  // outerjoin
  if (plan_->GetJoinType() == JoinType::OUTER) {
    for (uint32_t i = inner_tuple_match_idx_; i < inner_tuples_.size(); i++) {
      auto &inner_tuple = inner_tuples_[i];
      // if inner not matched
      if (inner_matched_idx_.find(i) == inner_matched_idx_.end()) {
        std::vector<Value> vals;
        for (uint32_t idx = 0; idx < outer_executor_->GetOutputSchema().GetColumnCount(); idx++) {
          vals.push_back(ValueFactory::GetNullValueByType(outer_executor_->GetOutputSchema().GetColumn(idx).GetType()));
        }
        for (uint32_t idx = 0; idx < inner_executor_->GetOutputSchema().GetColumnCount(); idx++) {
          vals.push_back(inner_tuple.GetValue(&inner_executor_->GetOutputSchema(), idx));
        }
        *tuple = Tuple(vals, &GetOutputSchema());
        inner_tuple_match_idx_ = i + 1;
        return true;
      }
    }
  }
  return false;
}
auto NestedLoopJoinExecutor::Matched(Tuple *outer_tuple, Tuple *inner_tuple) const -> bool {
  auto value = plan_->Predicate().EvaluateJoin(outer_tuple, outer_executor_->GetOutputSchema(), inner_tuple,
                                               inner_executor_->GetOutputSchema());
  return !value.IsNull() && value.GetAs<bool>();
}

}  // namespace bustub
