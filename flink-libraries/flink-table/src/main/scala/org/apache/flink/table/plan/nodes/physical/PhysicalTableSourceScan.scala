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

package org.apache.flink.table.plan.nodes.physical

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelWriter
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.TableScan
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.schema.{FlinkRelOptTable, TableSourceTable}
import org.apache.flink.table.sources.TableSource

import scala.collection.JavaConverters._

abstract class PhysicalTableSourceScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    relOptTable: FlinkRelOptTable)
  extends TableScan(cluster, traitSet, relOptTable) {

  protected val tableSourceTable: TableSourceTable =
    relOptTable.unwrap(classOf[TableSourceTable])

  protected[flink] val tableSource: TableSource = tableSourceTable.tableSource

  override def deriveRowType(): RelDataType = {
    val flinkTypeFactory = cluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    tableSourceTable.getRowType(flinkTypeFactory)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
        .item("fields", deriveRowType().getFieldNames.asScala.mkString(", "))
  }

  override def toString: String = {
    val tableName = getTable.getQualifiedName
    val s = s"table:$tableName, fields:(${getRowType.getFieldNames.asScala.toList.mkString(", ")})"
    s"Scan($s)"
  }

  def copy(traitSet: RelTraitSet, relOptTable: FlinkRelOptTable): PhysicalTableSourceScan

}
