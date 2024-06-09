#include "optimizer/optimizer.h"
#include <algorithm>
#include <cstddef>
#include <memory>
#include <optional>
#include <unordered_set>
#include <utility>
#include "catalog/schema.h"
#include "common/macros.h"
#include "common/util/string_util.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/mock_scan_plan.h"
#include "execution/plans/nested_index_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "fmt/core.h"

namespace bustub {

auto Optimizer::Optimize(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  if (force_starter_rule_) {
    // Use starter rules when `force_starter_rule_` is set to true.
    auto p = plan;
    p = OptimizeMergeProjection(p);
    p = OptimizeMergeFilterNLJ(p);
    p = OptimizeNLJAsIndexJoin(p);
    p = OptimizeOrderByAsIndexScan(p);
    p = OptimizeSortLimitAsTopN(p);
    return p;
  }
  // By default, use user-defined rules.
  return OptimizeCustom(plan);
}

auto Optimizer::EstimatedCardinality(const std::string &table_name) -> std::optional<size_t> {
  if (StringUtil::EndsWith(table_name, "_1m")) {
    return std::make_optional(1000000);
  }
  if (StringUtil::EndsWith(table_name, "_100k")) {
    return std::make_optional(100000);
  }
  if (StringUtil::EndsWith(table_name, "_50k")) {
    return std::make_optional(50000);
  }
  if (StringUtil::EndsWith(table_name, "_10k")) {
    return std::make_optional(10000);
  }
  if (StringUtil::EndsWith(table_name, "_1k")) {
    return std::make_optional(1000);
  }
  if (StringUtil::EndsWith(table_name, "_100")) {
    return std::make_optional(100);
  }
  return std::nullopt;
}

}  // namespace bustub
