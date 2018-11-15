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

package com.alibaba.blink.table.plan.rules.utils

import com.alibaba.blink.table.sources.HBaseDimensionTableSource
import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rex.RexInputRef
import org.apache.flink.table.plan.nodes.logical.{FlinkLogicalCalc, FlinkLogicalTableSourceScan}
import org.apache.flink.table.plan.schema.{FlinkRelOptTable, TableSourceTable}

import scala.collection.JavaConversions._

object HTableRelUtil {

 def hasHBaseTableSource(rel: RelNode): Boolean = {
    rel match {
      case s: RelSubset => hasHBaseTableSource(s.getRelList.get(0))

      // should distinguish whether it is a projection after Join or inside on left child
      case c: FlinkLogicalCalc => {
        if (hasSelectionOnly(c)) {
          hasHBaseTableSource(c.getInput(0))
        } else { // unsupported
          false
        }
      }

      case l: FlinkLogicalTableSourceScan => {
        if (l.getTable.isInstanceOf[FlinkRelOptTable] && null !=
          l.getTable.asInstanceOf[FlinkRelOptTable].unwrap(classOf[TableSourceTable])) {
          l.getTable.asInstanceOf[FlinkRelOptTable].unwrap(classOf[TableSourceTable])
            .tableSource.isInstanceOf[HBaseDimensionTableSource[_]]
        } else {
          false
        }
      }

      // other operations on HBaseTableSource not supported yet.
      case _ => false
    }
  }

  def hasSelectionOnly(calc: FlinkLogicalCalc): Boolean = {
    // validate if the projection has no other calculations except selection (no expressions or
    // alias or udx-call)
    !calc.getProgram.getExprList.exists(!_.isInstanceOf[RexInputRef])
  }

  def extractHTableSource(rel: RelNode): HBaseDimensionTableSource[_] = {
    rel match {
      case s: RelSubset => extractHTableSource(s.getRelList.get(0))

      case c: FlinkLogicalCalc => extractHTableSource(rel.getInput(0))

      case l: FlinkLogicalTableSourceScan => {
        if(null != l.tableSource && l.tableSource.isInstanceOf[HBaseDimensionTableSource[_]]) {
          l.tableSource.asInstanceOf[HBaseDimensionTableSource[_]]
        } else {
          l.getTable.asInstanceOf[FlinkRelOptTable].unwrap(classOf[TableSourceTable])
            .tableSource.asInstanceOf[HBaseDimensionTableSource[_]]
        }
      }

      case _ => null
    }
  }

}
