/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.plan.rules.logical

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.logical.LogicalTableScan
import org.apache.flink.table.calcite.FlinkPlannerImpl
import org.apache.flink.table.catalog.CatalogView
import org.apache.flink.table.plan.schema.CatalogCalciteTable

/**
  * Catalog View's subquery to RelNode.
  */
class catalogViewToRelNodeRule
    extends RelOptRule(
      operand(classOf[LogicalTableScan], any), "CatalogViewToRelNode"){

  override def matches(call: RelOptRuleCall): Boolean = {
    val rel = call.rel(0).asInstanceOf[LogicalTableScan]
    val table = rel.getTable.unwrap(classOf[CatalogCalciteTable])
    table != null && table.table.isInstanceOf[CatalogView]
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val oldRel = call.rel(0).asInstanceOf[LogicalTableScan]
    val view = oldRel.getTable.unwrap(classOf[CatalogCalciteTable]).table.asInstanceOf[CatalogView]
    val flinkPlanner = call.getPlanner.getContext.unwrap(classOf[FlinkPlannerImpl])

    val parsed = flinkPlanner.parse(view.getExpandedQuery)
    if (parsed != null) {
      // validate the sql query
      val validated = flinkPlanner.validate(parsed)
      // transform to a relational tree
      val relational = flinkPlanner.rel(validated)

      call.transformTo(relational.rel)
    } else {
      throw new RuntimeException(String.format("Unsupport SQL subquery: %s", view.getExpandedQuery))
    }
  }
}


object CatalogViewRules {
  val VIEW_SCAN_RULE = new catalogViewToRelNodeRule
}
