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

package org.apache.flink.table.plan.nodes

import org.apache.calcite.plan.{Convention, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.flink.table.plan.nodes.physical.stream.StreamExecRel
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalRel
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecRel

/**
 * Override the default convention implementation to support using AbstractConverter for conversion
 */
class FlinkConvention(name: String, relClass: Class[_ <: RelNode])
  extends Convention.Impl(name, relClass) {

  override def useAbstractConvertersForConversion(
      fromTraits: RelTraitSet,
      toTraits: RelTraitSet): Boolean = {
    if (relClass == classOf[StreamExecRel]) {
      //stream
      !fromTraits.satisfies(toTraits) &&
        fromTraits.containsIfApplicable(FlinkConventions.STREAMEXEC) &&
        toTraits.containsIfApplicable(FlinkConventions.STREAMEXEC)
    } else {
      //batch
      !fromTraits.satisfies(toTraits) &&
        fromTraits.containsIfApplicable(FlinkConventions.BATCHEXEC) &&
        toTraits.containsIfApplicable(FlinkConventions.BATCHEXEC)
    }
  }
}

object FlinkConventions {

  val LOGICAL: Convention = new Convention.Impl("LOGICAL", classOf[FlinkLogicalRel])
  val STREAMEXEC: Convention = new FlinkConvention("STREAMEXEC", classOf[StreamExecRel])
  val BATCHEXEC: Convention = new FlinkConvention("BATCHEXEC", classOf[BatchExecRel[_]])
}
