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

package org.apache.flink.table.plan.optimize.program

import org.apache.flink.table.plan.nodes.calcite.LogicalSink
import org.apache.flink.table.sinks.{OperationType, TableSink, UpdateDeleteTableSink}

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableList

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.logical.LogicalProject
import org.apache.calcite.rex.RexInputRef

import java.util

import _root_.scala.collection.JavaConversions._

/**
  * Adds project before delete table Sink.
  */
class FlinkDeleteDMLAddProjectProgram extends FlinkOptimizeProgram[BatchOptimizeContext] {

  override def optimize(input: RelNode, context: BatchOptimizeContext): RelNode = {
    input match {
      case logicalSink: LogicalSink =>
        logicalSink.sink match {
            // match delete table sink && primaryKeys is not empty.
          case t: UpdateDeleteTableSink
            if t.operationType == OperationType.DELETE
                && t.primaryKeys != null
                && t.primaryKeys.length != 0 => {
            val configuredSink = configPrimaryKeys(logicalSink.sink, t.primaryKeys)
            if (configuredSink.getFieldNames.sameElements(t.primaryKeys)) {
              // if sink fields after configured equals primaryKeys, add project.

              // change tableSink to configured tableSink.
              val newLogicalSink = LogicalSink.create(
                logicalSink.getInput,
                configuredSink,
                logicalSink.sinkName)
              val inputRowType = logicalSink.getInput.getRowType
              val primaryKeyList: util.List[String] = ImmutableList.copyOf(t.primaryKeys)
              // create project fields.
              val projects = primaryKeyList.map { field: String =>
                val index = inputRowType.getFieldNames.indexOf(field)
                assert(index >= 0, "it should not happen.")
                val fieldType = inputRowType.getFieldList.get(index).getType
                new RexInputRef(index, fieldType)
              }
              // create logical project.
              val logicalProject = LogicalProject.create(
                newLogicalSink.getInput,
                projects,
                primaryKeyList)
              // replace the input of logicalSink to logicalProject.
              newLogicalSink.replaceInput(0, logicalProject)
              return newLogicalSink
            }
            logicalSink
          }
          case _ => logicalSink
        }
      case _ => input
    }
  }

  // config primaryKeys to table sink.
  private def configPrimaryKeys(
      tableSink: TableSink[_],
      primaryKeys: Array[String]): TableSink[_] = {
    val oldFieldNames = tableSink.getFieldNames
    val oldFieldTypes = tableSink.getFieldTypes
    val primaryTypes = primaryKeys.map { field =>
      val index = oldFieldNames.indexOf(field)
      assert(index >= 0,
        "primaryKeys(" + primaryKeys.mkString(",") + ") must be contained in table sink " +
            "field names(" + oldFieldNames.mkString(",") + ").")
      oldFieldTypes(index)
    }
    val configuredSink = tableSink.configure(primaryKeys, primaryTypes)
    configuredSink
  }
}
