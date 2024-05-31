#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();
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
  std::priority_queue<Tuple, std::vector<Tuple>, decltype(comp_func)> pq(comp_func);

  Tuple child_tuple{};
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    pq.push(child_tuple);
    if (pq.size() > plan_->GetN()) {
      pq.pop();
    }
  }

  while (!pq.empty()) {
    top_n_.push(pq.top());
    pq.pop();
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (top_n_.empty()) {
    return false;
  }
  *tuple = top_n_.top();
  top_n_.pop();
  return true;
}

}  // namespace bustub
