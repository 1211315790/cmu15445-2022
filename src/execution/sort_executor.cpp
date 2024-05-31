#include "execution/executors/sort_executor.h"
#include <functional>
#include "common/logger.h"
#include "type/value.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  Tuple child_tuple{};
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    child_tuples_.push_back(child_tuple);
  }
  auto comp_func = [order_bys = plan_->order_bys_, schema = child_executor_->GetOutputSchema()](const Tuple &tuple_a,
                                                                                                const Tuple &tuple_b) {
    for (const auto &[order_type, expr] : order_bys) {
      Value a;
      Value b;
      switch (order_type) {
        case OrderByType::DEFAULT:
        case OrderByType::ASC:
          a = expr->Evaluate(&tuple_a, schema);
          b = expr->Evaluate(&tuple_b, schema);
          if (a.CompareLessThan(b) == CmpBool::CmpTrue) {
            return true;
          }
          if (a.CompareGreaterThan(b) == CmpBool::CmpTrue) {
            return false;
          }
          // if a == b, continue to next order by
          break;
        case OrderByType::DESC:
          a = expr->Evaluate(&tuple_a, schema);
          b = expr->Evaluate(&tuple_b, schema);
          if (a.CompareGreaterThan(b) == CmpBool::CmpTrue) {
            return true;
          }
          if (a.CompareLessThan(b) == CmpBool::CmpTrue) {
            return false;
          }
          // if a == b, continue to next order by
          break;
        default:
          throw Exception("Invalid OrderByType");
          break;
      }
    }
    return false;
  };
  std::sort(child_tuples_.begin(), child_tuples_.end(), comp_func);

  child_iter_ = child_tuples_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (child_iter_ == child_tuples_.end()) {
    return false;
  }

  *tuple = *child_iter_;
  *rid = tuple->GetRid();
  ++child_iter_;

  return true;
}

}  // namespace bustub
