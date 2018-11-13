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
import org.apache.calcite.tools.RuleSets
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.rules.FlinkBatchExecRuleSets
import org.apache.flink.table.plan.rules.logical.{JoinDependentFilterPushdownRule, SkewedJoinRule}

/**
  * Defines a sequence of programs to optimize flink batch exec table plan.
  */
object FlinkBatchPrograms {
  val QUERY_REWRITE = "query_rewrite"
  val DECORRELATE = "decorrelate"
  val NORMALIZATION = "normalization"
  val FPD = "filter_pushdown"
  val JOIN_REORDER = "join_reorder"
  val JOIN_REWRITE = "join_rewrite"
  val PPD = "project_pushdown"
  val WINDOW = "window"
  val LOGICAL = "logical"
  val PHYSICAL = "physical"
  val POST_PHYSICAL = "post"
  val RUNTIME_FILTER = "runtime_filter"

  def buildPrograms(): FlinkChainedPrograms[BatchOptimizeContext] = {
    val programs = new FlinkChainedPrograms[BatchOptimizeContext]()

    // convert queries before query decorrelation
    programs.addLast(
      QUERY_REWRITE,
      FlinkGroupProgramBuilder.newBuilder[BatchOptimizeContext]
        // rewrite RelTable before rewriting sub-queries
        .addProgram(FlinkHepRuleSetProgramBuilder.newBuilder
          .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
          .add(FlinkBatchExecRuleSets.TABLE_REF_RULES)
          .build(), "convert table references before rewriting sub-queries to semi-join")
        .addProgram(FlinkHepRuleSetProgramBuilder.newBuilder
          .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
          .add(FlinkBatchExecRuleSets.SEMI_JOIN_RULES)
          .build(), "rewrite sub-queries to semi-join")
        .addProgram(FlinkHepRuleSetProgramBuilder.newBuilder
          .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
          .add(FlinkBatchExecRuleSets.TABLE_SUBQUERY_RULES)
          .build(), "sub-queries remove")
        // convert RelOptTableImpl (which exists in SubQuery before) to FlinkRelOptTable
        .addProgram(FlinkHepRuleSetProgramBuilder.newBuilder
          .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
          .add(FlinkBatchExecRuleSets.TABLE_REF_RULES)
          .build(), "convert table references after sub-queries removed")
        .addProgram(FlinkHepRuleSetProgramBuilder.newBuilder
          .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
          .add(FlinkBatchExecRuleSets.REWRITE_RELNODE_RULES)
          .build(), "relnode rewrite")
        .build()
    )

    // decorrelate
    programs.addLast(
      DECORRELATE,
      FlinkGroupProgramBuilder.newBuilder[BatchOptimizeContext]
        .addProgram(new FlinkDecorrelateProgram, "decorrelate")
        .addProgram(new FlinkCorrelateVariablesValidationProgram, "correlate variables validation")
        .build()
    )

    // normalize the logical plan
    programs.addLast(
      NORMALIZATION,
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(FlinkBatchExecRuleSets.BATCH_EXEC_NORM_RULES)
        .build())

    // filter push down
    programs.addLast(
      FPD,
      FlinkGroupProgramBuilder.newBuilder[BatchOptimizeContext]
        .addProgram(
          FlinkGroupProgramBuilder.newBuilder[BatchOptimizeContext]
            .addProgram(FlinkHepRuleSetProgramBuilder.newBuilder
              .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
              .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
              .add(RuleSets.ofList(JoinDependentFilterPushdownRule.INSTANCE))
              .build(), "join dependent filter push down")
            .addProgram(FlinkHepRuleSetProgramBuilder.newBuilder
              .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
              .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
              .add(FlinkBatchExecRuleSets.FILTER_PREPARE_RULES)
              .build(), "filter rules")
            .setIterations(5)
            .build())
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(FlinkBatchExecRuleSets.FILTER_TABLESCAN_PUSHDOWN_RULES)
            .build(), "push filter to table scan")
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(FlinkBatchExecRuleSets.PRUNE_EMPTY_RULES)
            .build(), "prune empty results")
        .build()
    )

    // join reorder
    programs.addLast(
      JOIN_REORDER,
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(FlinkBatchExecRuleSets.BATCH_EXEC_JOIN_REORDER)
        .build())

    // join rewrite after join order
    programs.addLast(
      JOIN_REWRITE,
      FlinkGroupProgramBuilder.newBuilder[BatchOptimizeContext]
        // skewed join
        .addProgram(FlinkHepRuleSetProgramBuilder.newBuilder
          .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
          .add(RuleSets.ofList(SkewedJoinRule.INSTANCE))
          .build(), "skewed join")
        // Deal with join condition equality transfer
        // It is a deterministic optimization, so it is added to the rule base.
        .addProgram(FlinkHepRuleSetProgramBuilder.newBuilder
          .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
          .add(FlinkBatchExecRuleSets.JOIN_COND_EQUAL_TRANSFER_RULES)
          .build(), "join condition equality transfer")
        .build()
    )

    // project push down
    programs.addLast(
      PPD,
      FlinkGroupProgramBuilder.newBuilder[BatchOptimizeContext]
        .addProgram(FlinkHepRuleSetProgramBuilder.newBuilder
          .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
          .add(FlinkBatchExecRuleSets.PROJECT_RULES)
          .build(), "project rules")
        .addProgram(FlinkHepRuleSetProgramBuilder.newBuilder
          .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
          .add(FlinkBatchExecRuleSets.PROJECT_TABLESCAN_PUSHDOWN_RULES)
          .build(), "push project to table scan")
        .build()
    )

    programs.addLast(
      WINDOW,
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(FlinkBatchExecRuleSets.BATCH_EXEC_WINDOW_RULES)
        .build())

    // optimize the logical Flink plan
    programs.addLast(
        LOGICAL,
          FlinkVolcanoProgramBuilder.newBuilder
      .add(FlinkBatchExecRuleSets.BATCH_EXEC_LOGICAL_OPT_RULES)
      .setTargetTraits(Array(FlinkConventions.LOGICAL))
      .build())

    // optimize the physical Flink plan
    programs.addLast(
        PHYSICAL,
          FlinkVolcanoProgramBuilder.newBuilder
      .add(FlinkBatchExecRuleSets.BATCH_EXEC_OPT_RULES)
      .setTargetTraits(Array(FlinkConventions.BATCHEXEC))
      .build())

    programs.addLast(
      POST_PHYSICAL,
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(FlinkBatchExecRuleSets.BATCH_EXEC_POST_PHYSICAL_RULES)
        .build())

    programs.addLast(
      RUNTIME_FILTER,
      FlinkGroupProgramBuilder.newBuilder[BatchOptimizeContext]
          .addProgram(FlinkHepRuleSetProgramBuilder.newBuilder
              .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
              .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
              .add(FlinkBatchExecRuleSets.RUNTIME_FILTER_RULES)
              .build(), "runtime filter insert and push down")
          .addProgram(FlinkHepRuleSetProgramBuilder.newBuilder
              .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
              .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
              .add(FlinkBatchExecRuleSets.RUNTIME_FILTER_REMOVE_RULES)
              .build(), "runtime filter remove useless")
          .build()
    )

    programs
  }
}
