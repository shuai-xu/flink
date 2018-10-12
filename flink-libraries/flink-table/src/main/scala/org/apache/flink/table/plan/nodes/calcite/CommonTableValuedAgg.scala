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

package org.apache.flink.table.plan.nodes.calcite

import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex. RexNode
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.dataformat.GenericRow
import org.apache.flink.table.types.{BaseRowType, DataType, DataTypes, TypeInfoWrappedType}
import org.apache.flink.table.typeutils.TypeUtils

/**
  * Common table-valued aggregate.
  */
trait CommonTableValuedAgg {

  def getRowType(
    cluster: RelOptCluster,
    resultType: DataType,
    groupKey: Seq[RexNode],
    groupKeyNames: Seq[String]): RelDataType = {

    val typeFactory = cluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]

    val isResultTypeInternal = resultType match {
      case t: BaseRowType if t.getTypeClass == classOf[GenericRow] => true
      case _ => false
    }

    val isResultTypeCompositeType = resultType match {
      case t: TypeInfoWrappedType if TypeUtils.isInternalCompositeType(t.getTypeInfo) => true
      case _ => false
    }

    val outputRowType: BaseRowType = if (isResultTypeInternal) {
      resultType.asInstanceOf[BaseRowType]
    } else if (isResultTypeCompositeType) {
      DataTypes.internal(resultType).asInstanceOf[BaseRowType]
    } else {
      new BaseRowType(DataTypes.internal(resultType))
    }

    typeFactory.buildLogicalRowType(
      groupKeyNames ++ outputRowType.getFieldNames,
      groupKey.map(info=>FlinkTypeFactory.toTypeInfo(info.getType)) ++
        outputRowType.getFieldTypes.map(DataTypes.toTypeInfo))
  }
}
