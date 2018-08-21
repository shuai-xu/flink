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
package org.apache.flink.table.util

import java.io.PrintWriter
import java.util.{ArrayList => JArrayList, List => JList}

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.externalize.RelWriterImpl
import org.apache.calcite.sql.SqlExplainLevel
import org.apache.calcite.util.Pair
import org.apache.flink.api.common.operators.ResourceSpec
import org.apache.flink.table.plan.cost.BatchExecCost
import org.apache.flink.table.plan.nodes.physical.batch.{BatchExecHashJoinBase, BatchExecNestedLoopJoinBase, BatchExecReused, BatchExecScan, RowBatchExecRel}

import scala.collection.JavaConversions._

class RelTreeWriterImpl(
    pw: PrintWriter,
    explainLevel: SqlExplainLevel = SqlExplainLevel.EXPPLAN_ATTRIBUTES,
    printResource: Boolean = false,
    printMemCost: Boolean = false,
    withRelNodeId: Boolean = false)
  extends RelWriterImpl(pw, explainLevel, false) {

  var lastChildren: Seq[Boolean] = Nil

  var depth = 0

  override def explain_(rel: RelNode, values: JList[Pair[String, AnyRef]]): Unit = {
    // does not output child and cost attributes for ReusedBatchExec
    val isReusedNode = rel.isInstanceOf[BatchExecReused]
    val inputs = rel.getInputs
    val mq = rel.getCluster.getMetadataQuery
    if (!mq.isVisibleInExplain(rel, explainLevel)) {
      // render children in place of this, at same level
      inputs.toSeq.foreach(_.explain(this))
      return
    }

    val s = new StringBuilder
    if (depth > 0) {
      lastChildren.init.foreach { isLast =>
        s.append(if (isLast) "   " else ":  ")
      }
      s.append(if (lastChildren.last) "+- " else ":- ")
    }

    rel.getRelTypeName match {
      case name if name.endsWith("BatchExec") => s.append(name.substring(0, name.length - 9))
      case name if name.startsWith("BatchExec") => s.append(name.substring(9))
      case name => s.append(name)
    }

    val printValues = new JArrayList[Pair[String, AnyRef]]()
    if (printResource) {
      rel match {
        case rowBatchExec: RowBatchExecRel =>
          printValues.add(Pair.of(
            "partition",
            rowBatchExec.resultPartitionCount.asInstanceOf[AnyRef]))
          rowBatchExec match {
            case scan: BatchExecScan => {
              printValues.add(Pair.of(
                "sourceRes",
                resourceSpecToString(rowBatchExec.asInstanceOf[BatchExecScan].sourceResSpec)))
              if (scan.needInternalConversion) {
                printValues.add(Pair.of(
                  "conversionRes",
                  resourceSpecToString(rowBatchExec.asInstanceOf[BatchExecScan].conversionResSpec)))
              }
            }
            case hashJoin: BatchExecHashJoinBase => printValues.add(Pair.of(
                "shuffleCount",
                hashJoin.shuffleBuildCount(mq).toString))
            case nestedLoopJoin: BatchExecNestedLoopJoinBase => printValues.add(Pair.of(
              "shuffleCount",
              nestedLoopJoin.shuffleBuildCount(mq).toString))
            case _ => Unit
          }
          addResourceToPrint(rowBatchExec, printValues)
      }
    }
    if (printMemCost || printResource) {
      rel match {
        case rowBatchExec: RowBatchExecRel =>
          val memCost = mq.getNonCumulativeCost(rowBatchExec).asInstanceOf[BatchExecCost].memory
          printValues.add(Pair.of(
            "memCost",
            memCost.asInstanceOf[AnyRef]))
          printValues.add(Pair.of(
            "rowcount",
            mq.getRowCount(rel)))
      }
    }

    if (explainLevel != SqlExplainLevel.NO_ATTRIBUTES) {
      printValues.addAll(values)
    }

    if (withRelNodeId) {
      rel match {
        case rowBatchExec: RowBatchExecRel =>
          printValues.add(Pair.of("__id__", rowBatchExec.getId.toString))
      }
    }

    if (!printValues.isEmpty) {
      var j = 0
      printValues.toSeq.foreach {
        case value if value.right.isInstanceOf[RelNode] => // do nothing
        case value =>
          if (j == 0) s.append("(") else s.append(", ")
          j = j + 1
          s.append(value.left).append("=[").append(value.right).append("]")
      }
      if (j > 0) s.append(")")
    }

    if (explainLevel == SqlExplainLevel.ALL_ATTRIBUTES && !isReusedNode) {
      s.append(": rowcount = ")
        .append(mq.getRowCount(rel))
        .append(", cumulative cost = ")
        .append(mq.getCumulativeCost(rel))
    }
    pw.println(s)
    if (inputs.length > 1 && !isReusedNode) {
      inputs.toSeq.init.foreach { rel =>
        depth = depth + 1
        lastChildren = lastChildren :+ false
        rel.explain(this)
        depth = depth - 1
        lastChildren = lastChildren.init
      }
    }
    if (!inputs.isEmpty && !isReusedNode) {
      depth = depth + 1
      lastChildren = lastChildren :+ true
      inputs.toSeq.last.explain(this)
      depth = depth - 1
      lastChildren = lastChildren.init
    }
  }

  private def addResourceToPrint(
      rowBatchExec: RowBatchExecRel,
      printValues: JArrayList[Pair[String, AnyRef]]): Unit = {
    if (rowBatchExec.reservedResSpec != null) {
      printValues.add(Pair.of(
        "reserveRes",
        resourceSpecToString(rowBatchExec.reservedResSpec)))
    }
    if (rowBatchExec.preferResSpec != null) {
      printValues.add(Pair.of(
        "preferRes",
        resourceSpecToString(rowBatchExec.preferResSpec)))
    }
  }

  private def resourceSpecToString(resourceSpec: ResourceSpec): String = {
    val s = "Resource: {cpu=" + resourceSpec.getCpuCores + ", heap=" + resourceSpec.getHeapMemory;
    if (resourceSpec.getManagedMemoryInMB == 0) {
      s + "}"
    } else {
      s + ", managed=" + resourceSpec.getManagedMemoryInMB + "}"
    }
  }
}
