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
package com.alibaba.blink.table.api

import com.alibaba.blink.table.plan.rules.datastream.{StreamExecJoinHTableSourceRule, StreamExecJoinHTableToMultiJoinRule}
import org.apache.calcite.tools.RuleSets
import org.apache.flink.table.api.{StreamTableEnvironment, TableEnvironment}
import org.apache.flink.table.calcite.CalciteConfigBuilder
import org.apache.flink.table.plan.optimize.FlinkStreamPrograms

object BlinkTableUtils {
  /**
    * Injects blink rules and blink built-in functions
    *
    * @param tEnv the table environment to be injected
    */
  def injectBlinkRules(tEnv: TableEnvironment): Unit = {
    // add logical optimization rules
    val builder = new CalciteConfigBuilder()
    // add physical optimization rules
    if (tEnv.isInstanceOf[StreamTableEnvironment]) {
      val programs = builder.getStreamPrograms
      programs.getFlinkRuleSetProgram(FlinkStreamPrograms.PHYSICAL)
        .getOrElse(throw new RuntimeException(s"${FlinkStreamPrograms.PHYSICAL} does not exist"))
        .add(RuleSets.ofList(
          StreamExecJoinHTableSourceRule.INSTANCE,
          StreamExecJoinHTableToMultiJoinRule.INSTANCE
        ))
      tEnv.getConfig.setCalciteConfig(builder.build())
    }
  }
}
