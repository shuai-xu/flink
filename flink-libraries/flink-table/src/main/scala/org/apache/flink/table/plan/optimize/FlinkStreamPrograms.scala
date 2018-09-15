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

package org.apache.flink.table.plan.optimize

import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.rules.FlinkStreamExecRuleSets

/**
  * Defines a sequence of programs to optimize for stream table plan.
  */
object FlinkStreamPrograms {
  val SEMI_JOIN = "semi_join"
  val QUERY_REWRITE = "query_rewrite"
  val TABLE_REF = "table_ref"
  val DECORRELATE = "decorrelate"
  val TIME_INDICATOR = "time_indicator"
  val NORMALIZATION = "normalization"
  val FPD_PREPARE = "fpd_prepare"
  val FPD = "filter_tablescan_pushdown"
  val PRUNE_EMPTY = "prune_empty"
  val JOIN_REORDER = "join_reorder"
  val PPD_PREPARE = "ppd_prepare"
  val PPD = "project_tablescan_pushdown"
  val WINDOW = "window"
  val LOGICAL = "logical"
  val MICRO_BATCH = "micro_batch"
  val TOPN = "topn"
  val LAST_ROW = "last_row"
  val PHYSICAL = "physical"
  val DECORATE = "decorate"
  val AGG_SPLIT = "agg_split"
  val DECORATE_2 = "decorate_2"
  val POST = "post"

  def buildPrograms(): FlinkChainedPrograms[StreamOptimizeContext] = {
    val programs = new FlinkChainedPrograms[StreamOptimizeContext]()

    programs.addLast(
      SEMI_JOIN,
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(FlinkStreamExecRuleSets.SEMI_JOIN_RULES)
        .build()
    )

    // convert queries before query decorrelation
    programs.addLast(
      QUERY_REWRITE,
      FlinkGroupProgramBuilder.newBuilder[StreamOptimizeContext]
        .addProgram(FlinkHepRuleSetProgramBuilder.newBuilder
          .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
          .add(FlinkStreamExecRuleSets.TABLE_SUBQUERY_RULES)
          .build(), "sub-queries remove")
        .addProgram(FlinkHepRuleSetProgramBuilder.newBuilder
          .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
          .add(FlinkStreamExecRuleSets.REWRITE_RELNODE_RULES)
          .build(), "relnode rewrite")
        .build())

    // convert table references
    programs.addLast(
      TABLE_REF,
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(FlinkStreamExecRuleSets.TABLE_REF_RULES)
        .build())

    // decorrelate
    programs.addLast(
      DECORRELATE,
      FlinkGroupProgramBuilder.newBuilder[StreamOptimizeContext]
        .addProgram(new FlinkDecorrelateProgram, "decorrelate")
        .addProgram(new FlinkCorrelateVariablesValidationProgram, "correlate variables validation")
        .build()
    )

    // convert time indicators
    programs.addLast(TIME_INDICATOR, new FlinkRelTimeIndicatorProgram)

    //  normalize the logical plan
    programs.addLast(
      NORMALIZATION,
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(FlinkStreamExecRuleSets.STREAM_EXEC_NORM_RULES)
        .build())

    // filter push down prepare
    programs.addLast(
      FPD_PREPARE,
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(FlinkStreamExecRuleSets.FILTER_PREPARE_RULES)
        .build())

    // filter push down
    programs.addLast(
      FPD,
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(FlinkStreamExecRuleSets.FILTER_TABLESCAN_PUSHDOWN_RULES)
        .build())

    // prune empty results generated by FPD_PREPARE steps
    programs.addLast(
      PRUNE_EMPTY,
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(FlinkStreamExecRuleSets.PRUNE_EMPTY_RULES)
        .build())

    // join reorder
    programs.addLast(
      JOIN_REORDER,
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(FlinkStreamExecRuleSets.STREAM_EXEC_JOIN_REORDER)
        .build())

    // project push down prepare
    programs.addLast(
      PPD_PREPARE,
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(FlinkStreamExecRuleSets.PROJECT_RULES)
        .build())

    // project push down
    programs.addLast(
      PPD,
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(FlinkStreamExecRuleSets.PROJECT_TABLESCAN_PUSHDOWN_RULES)
        .build())

    programs.addLast(
      WINDOW,
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(FlinkStreamExecRuleSets.STREAM_EXEC_WINDOW_RULES)
        .build())

    // optimize the logical Flink plan
    programs.addLast(
      LOGICAL,
      FlinkVolcanoProgramBuilder.newBuilder
        .add(FlinkStreamExecRuleSets.STREAM_EXEC_LOGICAL_OPT_RULES)
        .setTargetTraits(Array(FlinkConventions.LOGICAL))
        .build())

    programs.addLast(
      TOPN,
      FlinkHepRuleSetProgramBuilder.newBuilder
      .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
      .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
      .add(FlinkStreamExecRuleSets.STREAM_EXEC_TOPN_RULES)
      .build())

    programs.addLast(
      LAST_ROW,
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(FlinkStreamExecRuleSets.STREAM_EXEC_LAST_ROW_RULES)
        .build())

    // optimize the physical Flink plan
    programs.addLast(
      PHYSICAL,
      FlinkVolcanoProgramBuilder.newBuilder
        .add(FlinkStreamExecRuleSets.STREAM_EXEC_OPT_RULES)
        .setTargetTraits(Array(FlinkConventions.STREAMEXEC))
        .build())

    // decorate the optimized plan
    programs.addLast(
      DECORATE,
      FlinkDecorateProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(FlinkStreamExecRuleSets.STREAM_EXEC_DECORATE_RULES)
        .build())

    // agg split
    programs.addLast(
      AGG_SPLIT,
      FlinkDecorateProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.TOP_DOWN)
        .add(FlinkStreamExecRuleSets.STREAM_EXEC_AGG_SPLIT_RULES)
        .build())

    // decorate the optimized plan
    programs.addLast(
      DECORATE_2,
      FlinkDecorateProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(FlinkStreamExecRuleSets.STREAM_EXEC_DECORATE_RULES)
        .build())

    // analyse micro batch
    programs.addLast(MICRO_BATCH, new FlinkMicroBatchAnalyseProgram)

    //use two stage agg optimize the plan
    programs.addLast(
      POST,
      FlinkDecorateProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(FlinkStreamExecRuleSets.POST)
        .build())
    programs
  }
}
