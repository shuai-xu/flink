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

package org.apache.flink.table.plan.util

import org.apache.flink.table.plan.schema.RelTable

import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rel.{RelNode, RelShuttleImpl}
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.logical.LogicalTableScan

/**
  * Convert logical table scan to a relational expression.
  */
class ExpandTableScanShuttle extends RelShuttleImpl {

  /**
    * Override this method to use `replaceInput` method instead of `copy` method
    * if any children change. This will not change any output of LogicalTableScan
    * when LogicalTableScan is replaced with RelNode tree in its RelTable.
    */
  override def visitChild(parent: RelNode, i: Int, child: RelNode): RelNode = {
    stack.push(parent)
    try {
      val child2 = child.accept(this)
      if (child2 ne child) {
        parent.replaceInput(i, child2)
      }
      parent
    } finally {
      stack.pop
    }
  }

  /**
    * Converts [[LogicalTableScan]] the result [[RelNode]] tree by calling [[RelTable]]#toRel
    */
  override def visit(scan: TableScan): RelNode = {
    scan match {
      case tableScan: LogicalTableScan =>
        val relTable = tableScan.getTable.unwrap(classOf[RelTable])
        if (relTable != null) {
          val rel = relTable.toRel(RelOptUtil.getContext(tableScan.getCluster), tableScan.getTable)
          rel.accept(this)
        } else {
          tableScan
        }
      case otherScan => otherScan
    }
  }
}
