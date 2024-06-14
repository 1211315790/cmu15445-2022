#include <bits/types/FILE.h>
#include <cstddef>
#include <memory>
#include <unordered_map>
#include <vector>
#include "catalog/column.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/executors/filter_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/arithmetic_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/mock_scan_plan.h"
#include "execution/plans/nested_index_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "execution/plans/values_plan.h"
#include "fmt/core.h"
#include "optimizer/optimizer.h"
#include "pg_definitions.hpp"
#include "type/value_factory.h"

// Note for 2022 Fall: You can add all optimizer rule implementations and apply the rules as you want in this file. Note
// that for some test cases, we force using starter rules, so that the configuration here won't take effects. Starter
// rule can be forcibly enabled by `set force_optimizer_starter_rule=yes`.

namespace bustub {

auto Optimizer::OptimizeCustom(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  auto p = plan;
  p = OptimizeMergeProjection(p);
  p = OptimizeMergeFilterIndexScan(p);
  p = OptimizeMergeFilterScan(p);
  p = OptimizeMergeFilterNLJ(p);
  p = OptimizeNLJAsIndexJoin(p);
  p = OptimizePredicatePushDown(p);
  p = OptimizeFalseFilter(p);
  p = OptimizeRemoveJoin(p);
  p = OptimizeColumnPruning(p);
  p = OptimizeNestedLoopJoinAndNestedIndexJoin(p);
  p = OptimizeNLJAsHashJoin(p);  // Enable this rule after you have implemented hash join.
  p = OptimizeJoinOrder(p);
  p = OptimizeOrderByAsIndexScan(p);
  p = OptimizeSortLimitAsTopN(p);
  return p;
}
auto Optimizer::OptimizeMergeFilterIndexScan(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeMergeFilterIndexScan(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::Filter) {
    const auto &filter_plan = dynamic_cast<const FilterPlanNode &>(*optimized_plan);
    BUSTUB_ASSERT(optimized_plan->children_.size() == 1, "must have exactly one children");
    const auto &child_plan = *optimized_plan->children_[0];
    if (child_plan.GetType() == PlanType::SeqScan) {
      const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(child_plan);
      const auto *table_info = catalog_.GetTable(seq_scan_plan.GetTableOid());
      const auto indices = catalog_.GetTableIndexes(table_info->name_);
      if (indices.empty()) {
        return optimized_plan;
      }
      if (const auto *expr = dynamic_cast<const ComparisonExpression *>(filter_plan.GetPredicate().get());
          expr != nullptr) {
        if (expr->comp_type_ == ComparisonType::Equal) {
          if (const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get());
              left_expr != nullptr) {
            if (const auto *right_expr = dynamic_cast<const ConstantValueExpression *>(expr->children_[1].get());
                right_expr != nullptr) {
              for (const auto *index : indices) {
                const auto &columns = index->key_schema_.GetColumns();
                if (columns.size() == 1 &&
                    columns[0].GetName() == table_info->schema_.GetColumn(left_expr->GetColIdx()).GetName()) {
                  return std::make_shared<IndexScanPlanNode>(optimized_plan->output_schema_, index->index_oid_,
                                                             filter_plan.GetPredicate());
                }
              }
            }
          }
        }
      }
    }
  }
  return optimized_plan;
}
auto Optimizer::OptimizeJoinOrder(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeJoinOrder(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  // accord EstimatedCardinality() ,swap children
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    auto &nlj_plan = dynamic_cast<NestedLoopJoinPlanNode &>(*optimized_plan);
    // Has exactly two children
    BUSTUB_ENSURE(optimized_plan->GetChildren().size() == 2, "NLJ should have exactly 2 children.");
    if (nlj_plan.GetLeftPlan()->GetType() == PlanType::MockScan &&
        nlj_plan.GetRightPlan()->GetType() == PlanType::MockScan) {
      const auto &left_plan_node = dynamic_cast<const MockScanPlanNode &>(*nlj_plan.GetLeftPlan());
      const auto &right_plan_node = dynamic_cast<const MockScanPlanNode &>(*nlj_plan.GetRightPlan());
      const auto left_table_name = left_plan_node.GetTable();
      const auto right_table_name = right_plan_node.GetTable();
      if (EstimatedCardinality(left_table_name).has_value() && EstimatedCardinality(right_table_name).has_value()) {
        if (EstimatedCardinality(left_table_name).value() > EstimatedCardinality(right_table_name).value()) {
          // 改写predicate
          std::swap(nlj_plan.predicate_->children_[0], nlj_plan.predicate_->children_[1]);
          // 改写output_schema
          auto new_output_schema = std::make_shared<Schema>(
              NestedLoopJoinPlanNode::InferJoinSchema(*nlj_plan.GetRightPlan(), *nlj_plan.GetLeftPlan()));
          optimized_plan->output_schema_ = new_output_schema;
          std::swap(optimized_plan->children_[0], optimized_plan->children_[1]);
        }
      }
    }
  }
  return optimized_plan;
}
auto Optimizer::OptimizeNestedLoopJoinAndNestedIndexJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNestedLoopJoinAndNestedIndexJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    auto &nlj_plan = dynamic_cast<NestedLoopJoinPlanNode &>(*optimized_plan);
    // Has exactly two children
    BUSTUB_ENSURE(optimized_plan->GetChildren().size() == 2, "NLJ should have exactly 2 children.");
    if (nlj_plan.GetLeftPlan()->GetType() == PlanType::NestedIndexJoin &&
        (nlj_plan.GetRightPlan()->GetType() == PlanType::MockScan ||
         nlj_plan.GetRightPlan()->GetType() == PlanType::SeqScan)) {
      if (const auto *expr = dynamic_cast<const ComparisonExpression *>(&nlj_plan.Predicate()); expr != nullptr) {
        if (expr->comp_type_ == ComparisonType::Equal) {
          if (const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get());
              left_expr != nullptr) {
            if (const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[1].get());
                right_expr != nullptr) {
              const auto &nij_plan = dynamic_cast<const NestedIndexJoinPlanNode &>(*nlj_plan.GetLeftPlan());
              const auto nlj_predicate = dynamic_cast<const ComparisonExpression &>(nlj_plan.Predicate());
              BUSTUB_ENSURE(nij_plan.GetChildren().size() == 1, "NIJ should have exactly 1 child.");
              Schema nij_child_output_schema = nij_plan.GetChildAt(0)->OutputSchema();
              Schema nij_output_schema = nij_plan.OutputSchema();
              auto pos = std::find_if(std::begin(nij_child_output_schema.GetColumns()),
                                      std::end(nij_child_output_schema.GetColumns()), [&](const auto &col) {
                                        return col.GetName() ==
                                               nij_output_schema.GetColumns()[left_expr->GetColIdx()].GetName();
                                      });
              // 无法优化,NLJ的谓词不是比较NIJ的内表
              if (pos == nij_child_output_schema.GetColumns().end()) {
                return optimized_plan;
              }
              size_t idx = std::distance(nij_child_output_schema.GetColumns().begin(), pos);
              // if NLJ的谓词是比较NIJ的内表, 交换NLJ和NIJ
              auto nij_inner_plan = nij_plan.GetChildPlan();
              auto nlj_right_plan = nlj_plan.GetRightPlan();
              // 创建新的 NLJ 计划，使用 NIJ 的子计划和 NLJ 的右子计划
              // 修改predicate的左表达式
              nlj_plan.predicate_->children_[0] =
                  std::make_shared<ColumnValueExpression>(left_expr->GetTupleIdx(), idx, left_expr->GetReturnType());
              auto new_nlj_paln_schema_ref =
                  std::make_shared<Schema>(NestedLoopJoinPlanNode::InferJoinSchema(*nij_inner_plan, *nlj_right_plan));
              auto new_nlj_plan = std::make_shared<NestedLoopJoinPlanNode>(
                  new_nlj_paln_schema_ref, nij_inner_plan, nlj_right_plan, nlj_plan.predicate_, nlj_plan.GetJoinType());

              // 创建新的 NIJ 计划，使用新的 NLJ 计划作为子计划
              auto new_nij_paln_schema_ref =
                  std::make_shared<Schema>(NestedLoopJoinPlanNode::InferJoinSchema(nij_plan, *nij_inner_plan));
              auto new_nij_plan = std::make_unique<NestedIndexJoinPlanNode>(
                  new_nij_paln_schema_ref, new_nlj_plan, nij_plan.KeyPredicate(), nij_plan.GetInnerTableOid(),
                  nij_plan.GetIndexOid(), nij_plan.GetIndexName(), nij_plan.index_table_name_,
                  nij_plan.inner_table_schema_, nij_plan.GetJoinType());
              return new_nij_plan;
            }
          }
        }
      }
    }
  }
  return optimized_plan;
}
auto Optimizer::OptimizePredicatePushDown(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizePredicatePushDown(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);

    if (nlj_plan.GetLeftPlan()->GetType() != PlanType::NestedLoopJoin) {
      return optimized_plan;
    }
    const auto &left_nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*nlj_plan.GetLeftPlan());

    if (nlj_plan.GetRightPlan()->GetType() != PlanType::MockScan ||
        left_nlj_plan.GetLeftPlan()->GetType() != PlanType::MockScan ||
        left_nlj_plan.GetRightPlan()->GetType() != PlanType::MockScan) {
      return optimized_plan;
    }

    std::vector<AbstractExpressionRef> join_preds;
    std::vector<AbstractExpressionRef> filter_preds;
    if (const auto *expr = dynamic_cast<const LogicExpression *>(&nlj_plan.Predicate()); expr != nullptr) {
      while (dynamic_cast<const LogicExpression *>(expr->children_[0].get()) != nullptr) {
        if (const auto *pred = dynamic_cast<const ColumnValueExpression *>(expr->children_[1]->children_[1].get());
            pred != nullptr) {
          join_preds.push_back(expr->children_[1]);
        } else {
          filter_preds.push_back(expr->children_[1]);
        }
        expr = dynamic_cast<const LogicExpression *>(expr->children_[0].get());
      }
      if (const auto *pred = dynamic_cast<const ColumnValueExpression *>(expr->children_[1]->children_[1].get());
          pred != nullptr) {
        join_preds.push_back(expr->children_[1]);
      } else {
        filter_preds.push_back(expr->children_[1]);
      }
      if (const auto *pred = dynamic_cast<const ColumnValueExpression *>(expr->children_[0]->children_[1].get());
          pred != nullptr) {
        join_preds.push_back(expr->children_[0]);
      } else {
        filter_preds.push_back(expr->children_[0]);
      }

      std::vector<AbstractExpressionRef> first_filter;
      std::vector<AbstractExpressionRef> third_filter;

      for (const auto &pred : filter_preds) {
        const auto *outer = dynamic_cast<const ComparisonExpression *>(pred.get());
        const auto *inner = dynamic_cast<const ColumnValueExpression *>(pred->children_[0].get());
        if (inner->GetTupleIdx() == 0) {
          first_filter.push_back(pred);
        } else {
          third_filter.push_back(std::make_shared<ComparisonExpression>(
              std::make_shared<ColumnValueExpression>(0, inner->GetColIdx(), inner->GetReturnType()),
              pred->children_[1], outer->comp_type_));
        }
      }
      BUSTUB_ASSERT(first_filter.size() == 2, "only in leader board test!");
      BUSTUB_ASSERT(third_filter.size() == 2, "only in leader board test!");

      auto first_pred = std::make_shared<LogicExpression>(first_filter[0], first_filter[1], LogicType::And);
      auto third_pred = std::make_shared<LogicExpression>(third_filter[0], third_filter[1], LogicType::And);

      auto first_filter_scan = std::make_shared<FilterPlanNode>(left_nlj_plan.children_[0]->output_schema_, first_pred,
                                                                left_nlj_plan.children_[0]);
      auto third_filter_scan = std::make_shared<FilterPlanNode>(nlj_plan.GetRightPlan()->output_schema_, third_pred,
                                                                nlj_plan.GetRightPlan());
      auto left_node = std::make_shared<NestedLoopJoinPlanNode>(left_nlj_plan.output_schema_, first_filter_scan,
                                                                left_nlj_plan.GetRightPlan(), left_nlj_plan.predicate_,
                                                                left_nlj_plan.GetJoinType());
      return std::make_shared<NestedLoopJoinPlanNode>(nlj_plan.output_schema_, left_node, third_filter_scan,
                                                      join_preds[0], nlj_plan.GetJoinType());
    }
  }

  return optimized_plan;
}
auto Optimizer::OptimizeFalseFilter(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeFalseFilter(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::Filter) {
    const auto &filter_plan = dynamic_cast<const FilterPlanNode &>(*optimized_plan);

    if (IsPredicateFalse(*filter_plan.GetPredicate())) {
      return std::make_shared<ValuesPlanNode>(filter_plan.children_[0]->output_schema_,
                                              std::vector<std::vector<AbstractExpressionRef>>{});
    }
  }
  return optimized_plan;
}
auto Optimizer::IsPredicateFalse(const AbstractExpression &expr) -> bool {
  if (const auto *const_expr = dynamic_cast<const ConstantValueExpression *>(&expr); const_expr != nullptr) {
    return const_expr->val_.CompareEquals(ValueFactory::GetBooleanValue(false)) == CmpBool::CmpTrue;
  }
  if (const auto *compare_expr = dynamic_cast<const ComparisonExpression *>(&expr); compare_expr != nullptr) {
    if (const auto *left_expr = dynamic_cast<const ConstantValueExpression *>(compare_expr->children_[0].get());
        left_expr != nullptr) {
      if (const auto *right_expr = dynamic_cast<const ConstantValueExpression *>(compare_expr->children_[1].get());
          right_expr != nullptr) {
        std::vector<Column> dummy_cols;
        Schema dummy_schema{dummy_cols};
        auto res = compare_expr->Evaluate(nullptr, dummy_schema);
        if (res.CompareEquals(ValueFactory::GetBooleanValue(false)) == CmpBool::CmpTrue) {
          return true;
        }
      }
    }
  }
  return false;
}
auto Optimizer::OptimizeRemoveJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeRemoveJoin(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    if (nlj_plan.GetRightPlan()->GetType() == PlanType::Values) {
      const auto &right_plan = dynamic_cast<const ValuesPlanNode &>(*nlj_plan.GetRightPlan());

      if (right_plan.GetValues().empty()) {
        return nlj_plan.children_[0];
      }
    }
    if (nlj_plan.GetLeftPlan()->GetType() == PlanType::Values) {
      const auto &left_plan = dynamic_cast<const ValuesPlanNode &>(*nlj_plan.GetLeftPlan());

      if (left_plan.GetValues().empty()) {
        return nlj_plan.children_[1];
      }
    }
  }
  return optimized_plan;
}

auto Optimizer::OptimizeColumnPruning(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  if (plan->GetType() == PlanType::Projection) {
    if (plan->GetChildAt(0)->GetType() == PlanType::Projection) {
      // pj->pj优化，合并成一个pj，只需要上层pj所需要的表达式即可即可

      auto pj_plan = dynamic_cast<const ProjectionPlanNode *>(plan.get());
      auto child_pj_plan = dynamic_cast<const ProjectionPlanNode *>(plan->GetChildAt(0).get());

      // 获取上层pj需要输出的下层pj里表达式的编号（列号）
      std::vector<uint32_t> output_cols;
      GetOutputCols(pj_plan->GetExpressions(), output_cols);

      // 从下层的pj中根据编号提取出需要的表达式
      auto child_exprs = child_pj_plan->GetExpressions();
      std::vector<AbstractExpressionRef> new_child_exprs(output_cols.size());
      for (size_t i = 0; i < output_cols.size(); i++) {
        new_child_exprs[i] = child_exprs[output_cols[i]];
      }

      // 创建出新的pj_plan
      auto new_pj_plan =
          std::make_shared<ProjectionPlanNode>(pj_plan->output_schema_, new_child_exprs, child_pj_plan->GetChildAt(0));

      // 从新的pj_plan往下递归
      auto optimize_pj_plan = OptimizeColumnPruning(new_pj_plan);

      return optimize_pj_plan;
    }
    if (plan->GetChildAt(0)->GetType() == PlanType::Aggregation) {
      // 上层pj，下层agg，agg只需要输出group_by+上层pj需要的列即可（group_by一定要输出，agg_exec规定输出group_by）

      auto pj_plan = dynamic_cast<const ProjectionPlanNode *>(plan.get());
      auto agg_plan = dynamic_cast<const AggregationPlanNode *>(plan->GetChildAt(0).get());

      // 获取上层pj需要的列
      std::vector<uint32_t> output_cols;
      GetOutputCols(pj_plan->GetExpressions(), output_cols);

      // 获取所有agg的表达式和类型，即agg_plan原本需要输出的列
      auto aggs = agg_plan->GetAggregates();
      auto agg_types = agg_plan->GetAggregateTypes();

      // agg可能为空，此时无需优化，直接往下走即可，兼容前面测试的空表情况
      if (aggs.empty()) {
        std::vector<AbstractPlanNodeRef> children;
        for (const auto &child : plan->GetChildren()) {
          children.emplace_back(OptimizeColumnPruning(child));
        }
        auto optimized_plan = plan->CloneWithChildren(std::move(children));
        return optimized_plan;
      }

      // 提取上层pg需要的agg
      std::vector<AbstractExpressionRef> new_aggs;
      std::vector<AggregationType> new_agg_types;
      for (auto &col : output_cols) {
        // 上层需要的列中可能存在group_by，group_by不需要加入agg中
        if (col < agg_plan->GetGroupBys().size()) {
          continue;
        }
        // 由于agg的输出形式为group_bys + agg1 + agg2 + ...
        // output_cols的编号（pj看到的）是从group_size开始的，映射到agg表达式数组里时要减去一个偏移
        new_aggs.push_back(aggs[col - agg_plan->GetGroupBys().size()]);
        new_agg_types.push_back(agg_types[col - agg_plan->GetGroupBys().size()]);
      }

      // 获取新的scheme，groupbys+需要的aggs
      auto new_output_schema = GetSchema(agg_plan->output_schema_, output_cols, agg_plan->GetGroupBys().size());

      // 创造新的agg_plan
      auto new_agg_plan = std::make_shared<AggregationPlanNode>(new_output_schema, agg_plan->GetChildAt(0),
                                                                agg_plan->GetGroupBys(), new_aggs, new_agg_types);
      // 从新的agg_plan往下递归
      auto optimize_agg_plan = OptimizeColumnPruning(new_agg_plan);

      // 创造新的nj_plan，其孩子为优化后的new_agg_plan
      auto new_pj_plan =
          std::make_shared<ProjectionPlanNode>(pj_plan->output_schema_, pj_plan->GetExpressions(), optimize_agg_plan);
      // 返回新的nj_plan
      return new_pj_plan;
    }
  }

  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeColumnPruning(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  return optimized_plan;
}
// leaderboard-3
auto Optimizer::CheckArithMetic(const AbstractExpressionRef &expr) -> bool {
  auto express = dynamic_cast<const ArithmeticExpression *>(expr.get());
  return express != nullptr;
}
auto Optimizer::CheckColumnValue(const AbstractExpressionRef &expr) -> bool {
  auto express = dynamic_cast<const ColumnValueExpression *>(expr.get());
  return express != nullptr;
}

// 解析当前表达式获取编号
// 这里支持的表达式有：colval (arithMetic:+,-) colval，和colval
void Optimizer::ParseExprForColumnPruning(const AbstractExpressionRef &expr, std::vector<uint32_t> &output_cols) {
  if (CheckArithMetic(expr)) {
    ParseExprForColumnPruning(expr->GetChildAt(0), output_cols);
    ParseExprForColumnPruning(expr->GetChildAt(1), output_cols);
  } else if (CheckColumnValue(expr)) {
    auto express = dynamic_cast<const ColumnValueExpression *>(expr.get());
    output_cols.push_back(express->GetColIdx());
  }
}

// 提取每个表达式要用到的cols编号
// 如#0.1 #0.2 #0.3+#0.4
// 提取出{1,2,3,4}
void Optimizer::GetOutputCols(const std::vector<AbstractExpressionRef> &exprs, std::vector<uint32_t> &output_cols) {
  for (auto &x : exprs) {
    // 解析当前表达式获取编号
    ParseExprForColumnPruning(x, output_cols);
  }
}

// 获取新的scheme，groupbys + 需要的aggs
auto Optimizer::GetSchema(const SchemaRef &schema, std::vector<uint32_t> &output_cols, size_t group_by_nums)
    -> SchemaRef {
  auto origin_cols = schema->GetColumns();
  std::vector<Column> new_cols;

  // 由于上层pj需要的列可能会重复，如#0.1,#0.2,#0.1+#0.2，此时下层agg只需输出#0.1和#0.2即可，但output_cols里
  // 存的是{1,2,1,2}，所以需要去重
  std::unordered_set<uint32_t> s;

  // 先把groups加进来,groups表达式的需要一定是0~groups_size-1(先输出的groups)
  for (size_t i = 0; i < group_by_nums; i++) {
    new_cols.push_back(origin_cols[i]);
    s.insert(i);
  }

  // 然后再将需要的aggs加进来
  for (auto &x : output_cols) {
    if (s.count(x) == 0U) {
      new_cols.push_back(origin_cols[x]);
      s.insert(x);
    }
  }
  return std::make_shared<Schema>(new_cols);
}
}  // namespace bustub
