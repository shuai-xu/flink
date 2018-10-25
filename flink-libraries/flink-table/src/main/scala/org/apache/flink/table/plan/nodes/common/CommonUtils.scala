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

package org.apache.flink.table.plan.nodes.common

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.flink.streaming.api.bundle.{CombinedBundleTrigger, CountBundleTrigger, TimeBundleTrigger, BundleTrigger}
import org.apache.flink.table.api.StreamQueryConfig
import org.apache.flink.table.dataformat.BaseRow

import scala.collection.JavaConverters._

object CommonUtils {
  def groupingToString(inputType: RelDataType, grouping: Array[Int]): String = {
    val inFields = inputType.getFieldNames.asScala
    grouping.map( inFields(_) ).mkString(", ")
  }

  def getMiniBatchTrigger(
      queryConfig: StreamQueryConfig,
      useLocalAgg: Boolean): CombinedBundleTrigger[BaseRow] = {
    val triggerTime = if (useLocalAgg) {
      queryConfig.getMiniBatchTriggerTime / 2
    } else {
      queryConfig.getMiniBatchTriggerTime
    }
    val timeTrigger: Option[BundleTrigger[BaseRow]] =
      if (queryConfig.isMicroBatchEnabled) {
        None
      } else {
        Some(new TimeBundleTrigger[BaseRow](triggerTime))
      }
    val sizeTrigger: Option[BundleTrigger[BaseRow]] =
      if (queryConfig.getMiniBatchTriggerSize == Long.MinValue) {
        None
      } else {
        Some(new CountBundleTrigger[BaseRow](queryConfig.getMiniBatchTriggerSize))
      }
    new CombinedBundleTrigger[BaseRow](
      Array(timeTrigger, sizeTrigger).filter(_.isDefined).map(_.get): _*
    )
  }

}
