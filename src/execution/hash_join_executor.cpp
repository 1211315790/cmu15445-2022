//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"
// Note for 2022 Fall: You don't need to implement HashJoinExecutor to pass all tests. You ONLY need to implement it
// if you want to get faster in leaderboard tests.

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_{plan},
      left_executor_{std::move(left_child)},
      right_executor_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  Tuple right_tuple{};
  Tuple left_tuple{};
  RID rid;
  // build hash_table
  while (right_executor_->Next(&right_tuple, &rid)) {
    auto join_key = plan_->RightJoinKeyExpression().Evaluate(&right_tuple, plan_->GetRightPlan()->OutputSchema());
    hash_join_table_[HashUtil::HashValue(&join_key)].push_back(right_tuple);
  }
  // hash probe
  while (left_executor_->Next(&left_tuple, &rid)) {
    auto left_join_key = plan_->LeftJoinKeyExpression().Evaluate(&left_tuple, plan_->GetLeftPlan()->OutputSchema());
    if (hash_join_table_.count(HashUtil::HashValue(&left_join_key)) > 0) {
      std::vector<Tuple> right_tuples = hash_join_table_[HashUtil::HashValue(&left_join_key)];
      for (const auto &tuple : right_tuples) {
        auto right_join_key = plan_->RightJoinKeyExpression().Evaluate(&tuple, plan_->GetRightPlan()->OutputSchema());
        // matched
        if (right_join_key.CompareEquals(left_join_key) == CmpBool::CmpTrue) {
          std::vector<Value> values{};
          values.reserve(left_executor_->GetOutputSchema().GetColumnCount() +
                         right_executor_->GetOutputSchema().GetColumnCount());
          for (uint32_t col_idx = 0; col_idx < left_executor_->GetOutputSchema().GetColumnCount(); col_idx++) {
            values.push_back(left_tuple.GetValue(&left_executor_->GetOutputSchema(), col_idx));
          }
          for (uint32_t col_idx = 0; col_idx < right_executor_->GetOutputSchema().GetColumnCount(); col_idx++) {
            values.push_back(tuple.GetValue(&right_executor_->GetOutputSchema(), col_idx));
          }
          output_tuples_.emplace_back(values, &GetOutputSchema());
        }
      }
    } else if (plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> values{};
      values.reserve(left_executor_->GetOutputSchema().GetColumnCount() +
                     right_executor_->GetOutputSchema().GetColumnCount());
      for (uint32_t col_idx = 0; col_idx < left_executor_->GetOutputSchema().GetColumnCount(); col_idx++) {
        values.push_back(left_tuple.GetValue(&left_executor_->GetOutputSchema(), col_idx));
      }
      for (uint32_t col_idx = 0; col_idx < right_executor_->GetOutputSchema().GetColumnCount(); col_idx++) {
        values.push_back(
            ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(col_idx).GetType()));
      }
      output_tuples_.emplace_back(values, &GetOutputSchema());
    }
  }

  output_tuples_iter_ = output_tuples_.cbegin();
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (output_tuples_iter_ == output_tuples_.cend()) {
    return false;
  }
  *tuple = *output_tuples_iter_;
  ++output_tuples_iter_;
  return true;
}

}  // namespace bustub
