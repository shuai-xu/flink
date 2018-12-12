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

package org.apache.flink.table.plan.metadata

import org.apache.flink.table.api.TableConfig._
import org.apache.flink.table.functions.sql.internal.{SqlRuntimeFilterBuilderFunction, SqlRuntimeFilterFunction}
import org.apache.flink.table.plan.metadata.SelectivityEstimator._
import org.apache.flink.table.plan.schema.{FlinkTable, TableSourceTable}
import org.apache.flink.table.plan.stats._
import org.apache.flink.table.plan.util.{FlinkRelOptUtil, FlinkRexUtil, PartitionPredicateExtractor, RexNodeExtractor}
import org.apache.flink.table.sources.PartitionableTableSource

import org.apache.calcite.avatica.util.DateTimeUtils
import org.apache.calcite.plan.{RelOptPredicateList, RelOptUtil}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFamily}
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.metadata.RelMdUtil
import org.apache.calcite.rex._
import org.apache.calcite.sql.`type`.SqlTypeFamily
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.apache.calcite.sql.{SqlKind, SqlOperator}
import org.apache.calcite.util.{ImmutableBitSet, TimeString}

import java.lang.{Double => JDouble}

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Estimates selectivity of rows meeting a filter predicate on a RelNode.
  *
  * <p>For some unsupported cases, we use the default selectivity values
  * referred to [[org.apache.calcite.rel.metadata.RelMdUtil.guessSelectivity]].
  *
  * <p><strong>NOTES:</strong>
  * In current stage, we don't have advanced statistics such as sketches or histograms,
  * we have to assume uniform distribution.
  * We should update estimation methods in the future if advanced statistics is ready.
  *
  * <p>Part of the code is referred from Apache Spark.
  *
  * @param rel RelNode
  * @param mq  Metadata query
  */
class SelectivityEstimator(rel: RelNode, mq: FlinkRelMetadataQuery)
    extends RexVisitorImpl[Option[Double]](true) {

  private val rexBuilder = rel.getCluster.getRexBuilder
  private val tableConfig = FlinkRelOptUtil.getTableConfig(rel)
  private val maxCnfNodeCount = FlinkRelOptUtil.getMaxCnfNodeCount(rel)

  private[flink] def getDefaultSelectivity(key: String, defaultValue: Double): Option[Double] = {
    val v = tableConfig.getParameters.getDouble(key, defaultValue)
    val selectivity = if (0.0 <= v && v <= 1.0) v else defaultValue
    Some(selectivity)
  }

  // these default values is referred to RelMdUtil#guessSelectivity
  private[flink] val defaultComparisonSelectivity =
    getDefaultSelectivity(SQL_CBO_SELECTIVITY_COMPARISON_DEFAULT, 0.5)
  private[flink] val defaultEqualsSelectivity =
    getDefaultSelectivity(SQL_CBO_SELECTIVITY_EQUALS_DEFAULT, 0.15)
  private[flink] val defaultIsNullSelectivity =
    getDefaultSelectivity(SQL_CBO_SELECTIVITY_ISNULL_DEFAULT, 0.1)
  private[flink] val defaultIsNotNullSelectivity = Some(1.0 - defaultIsNullSelectivity.get)
  private[flink] val defaultLikeSelectivity =
    getDefaultSelectivity(SQL_CBO_SELECTIVITY_LIKE_DEFAULT, 0.05)
  private[flink] val defaultSelectivity = getDefaultSelectivity(SQL_CBO_SELECTIVITY_DEFAULT, 0.25)

  /**
    * Returns a percentage of rows meeting a filter predicate on TableScan node.
    *
    * @param predicate predicate whose selectivity is to be estimated against scan's output.
    * @return estimated selectivity (between 0.0 and 1.0),
    *         or None if no reliable estimate can be determined.
    */
  def evaluate(predicate: RexNode): Option[Double] = {
    try {
      if (predicate == null) {
        Some(1.0)
      } else {
        val rexSimplify = new RexSimplify(
          rexBuilder, RelOptPredicateList.EMPTY, true, RexUtil.EXECUTOR)
        val simplifiedPredicate = rexSimplify.simplify(predicate)
        if (simplifiedPredicate.isAlwaysTrue) {
          Some(1.0)
        } else if (simplifiedPredicate.isAlwaysFalse) {
          Some(0.0)
        } else {
          simplifiedPredicate.accept(this)
        }
      }
    } catch {
      // if found unsupported operations, fallback
      case _: Throwable => None
    }
  }

  /**
    * Get partition field names from PartitionableTableSource.
    *
    * @return partition field names if the TableSource in scan is PartitionableTableSource,
    *         otherwise None.
    */
  private def getPartitionFieldNames(scan: TableScan): Option[Array[String]] = {
    val table = scan.getTable.unwrap(classOf[FlinkTable])
    table match {
      case t: TableSourceTable =>
        t.tableSource match {
          case p: PartitionableTableSource =>
            Some(p.getPartitionFieldNames)
          case _ => None
        }
      case _ => None
    }
  }

  override def visitCall(call: RexCall): Option[Double] = {
    call.getOperator match {
      case AND =>
        val predicates = splitAndPredicate(call)
        // predicates.size == 1 indicates that
        // the RexInputRef of each sub-condition in this AND call are the same.
        if (predicates.size == 1) {
          estimateAndPredicate(call)
        } else {
          val selectivity = predicates.map(estimateOperand)
          Some(selectivity.product)
        }

      case OR =>
        val predicates = splitOrPredicate(call)
        if (predicates.size == 1) {
          predicates.head match {
            case c: RexCall if c.getOperator == IN => estimateSinglePredicate(c)
            case _ => estimateOrPredicate(call)
          }
        } else {
          val selectivity = predicates.map(estimateOperand)
          Some(math.min(1.0, selectivity.sum - selectivity.product))
        }

      case NOT =>
        val operands = call.getOperands
        val selectivity = estimateOperand(operands.head)
        Some(1.0 - selectivity)

      case _ =>
        estimateSinglePredicate(call)
    }
  }

  private def estimateOperand(operand: RexNode): Double = {
    val subSelectivity = operand.accept(this)
    if (subSelectivity != null) subSelectivity.getOrElse(1.0) else 1.0
  }

  /**
    * Returns a percentage of rows meeting a single condition in Filter node.
    *
    * We will do partition pruning with those supported partition-pruning predicates,
    * and the rowCount in [[org.apache.flink.table.plan.stats.TableStats]] should exclude the
    * rows of the pruned partitions. So we should ignore partition predicate here.
    *
    * @param singlePredicate predicate whose selectivity is to be estimated against scan's output.
    * @return an optional double value to show the percentage of rows meeting a given condition.
    *         It returns None if the condition is not supported.
    */
  private def estimateSinglePredicate(singlePredicate: RexCall): Option[Double] = {
    if (isSupportedPartitionPredicate(singlePredicate)) {
      return estimatePartitionPredicate(singlePredicate)
    }

    val operands = singlePredicate.getOperands
    singlePredicate.getOperator match {
      case EQUALS =>
        estimateComparison(EQUALS, operands.head, operands.last)

      case NOT_EQUALS =>
        val selectivity = estimateComparison(EQUALS, operands.head, operands.last)
        Some(1.0 - selectivity.getOrElse(1.0))

      case GREATER_THAN =>
        estimateComparison(GREATER_THAN, operands.head, operands.last)

      case GREATER_THAN_OR_EQUAL =>
        estimateComparison(GREATER_THAN_OR_EQUAL, operands.head, operands.last)

      case LESS_THAN =>
        estimateComparison(LESS_THAN, operands.head, operands.last)

      case LESS_THAN_OR_EQUAL =>
        estimateComparison(LESS_THAN_OR_EQUAL, operands.head, operands.last)

      case IS_NULL =>
        estimateIsNull(operands.head)

      case IS_NOT_NULL =>
        estimateIsNotNull(operands.head)

      case IN =>
        estimateIn(operands.head, operands.slice(1, operands.size))

      case RelMdUtil.ARTIFICIAL_SELECTIVITY_FUNC =>
        Option(RelMdUtil.getSelectivityValue(singlePredicate))

      case LIKE =>
        defaultLikeSelectivity

      // RuntimeFilter should not break Cost's estimate.
      // (Because it may be useless, just advance Join's filtering only).
      case _: SqlRuntimeFilterBuilderFunction | _: SqlRuntimeFilterFunction =>
        Some(1.0D)

      case _ =>
        defaultSelectivity
    }
  }

  /**
    * Decomposes a predicate into a list of expressions that are AND'ed together,
    * and the expressions with same RexInputRefs will be converted into an AND.
    * e.g.
    * input predicate: `a > 10 AND b < 40.0 AND a < 20 AND CAST(b AS INTEGER) > 30`
    * output expressions: `a > 10 AND a < 20` and `b < 40.0` and `CAST(b AS INTEGER) > 30`
    */
  private def splitAndPredicate(call: RexCall): Seq[RexNode] = {
    val cnf = FlinkRexUtil.toCnf(rexBuilder, maxCnfNodeCount, call)
    val conjunctions = RelOptUtil.conjunctions(cnf)

    val combinations = new mutable.HashMap[String, mutable.ListBuffer[RexNode]]()
    val nonCombinations = new mutable.ListBuffer[RexNode]()

    val visitor = new RexVisitorImpl[Unit](true) {
      override def visitCall(call: RexCall): Unit = {
        call.getOperator match {
          case AND => throw new RuntimeException("This should not happen.")
          // TODO supports more operators
          case GREATER_THAN | GREATER_THAN_OR_EQUAL | LESS_THAN | LESS_THAN_OR_EQUAL =>
            val (left, right) = (call.operands.get(0), call.operands.get(1))
            (left, right) match {
              case (i: RexInputRef, _: RexLiteral) =>
                combinations.getOrElseUpdate(i.toString, mutable.ListBuffer[RexNode]()) += call
              case (_: RexLiteral, i: RexInputRef) =>
                combinations.getOrElseUpdate(i.toString, mutable.ListBuffer[RexNode]()) += call
              case _ => nonCombinations += call
            }
          case _ => nonCombinations += call
        }
      }
    }

    conjunctions.foreach(_.accept(visitor))

    val combined = combinations.values.map { r =>
      RexUtil.composeConjunction(rexBuilder, r, false)
    }.toSeq

    combined ++ nonCombinations
  }

  /**
    * Decomposes a predicate into a list of expressions that are OR'ed together,
    * and the expressions with same RexInputRefs connected with EQUALS will be converted into an IN.
    * e.g.
    * input predicate: `a = 10 or a = 20 OR a > 40.0 OR a < 0 OR b > 30`
    * output expressions: `a IN (10, 20)` and `(a > 40 OR a < 0)` and `b > 30`
    */
  private def splitOrPredicate(call: RexCall): Seq[RexNode] = {
    val combinations = new mutable.HashMap[String, mutable.ListBuffer[RexNode]]()
    val nonCombinations = new mutable.ListBuffer[RexNode]()

    val visitor = new RexVisitorImpl[Unit](true) {
      override def visitCall(call: RexCall): Unit = {
        call.getOperator match {
          // TODO supports more operators
          case GREATER_THAN | GREATER_THAN_OR_EQUAL | LESS_THAN | LESS_THAN_OR_EQUAL | EQUALS =>
            val (left, right) = (call.operands.get(0), call.operands.get(1))
            (left, right) match {
              case (i: RexInputRef, _: RexLiteral) =>
                combinations.getOrElseUpdate(i.toString, mutable.ListBuffer[RexNode]()) += call
              case (l: RexLiteral, i: RexInputRef) =>
                val newCall = if (call.getOperator == EQUALS) {
                  call.clone(call.getType, List(i, l))
                } else {
                  call
                }
                combinations.getOrElseUpdate(i.toString, mutable.ListBuffer[RexNode]()) += newCall
              case _ => nonCombinations += call
            }
          case _ => nonCombinations += call
        }
      }
    }

    val disjunctions = RelOptUtil.disjunctions(call)
    disjunctions.foreach(_.accept(visitor))

    val combined = combinations.values.flatMap { r =>
      val (equalsNodes, nonEqualsNodes) = r.partition(_.asInstanceOf[RexCall].getOperator == EQUALS)
      if (equalsNodes.size > 1) {
        val inputRef = equalsNodes.head.asInstanceOf[RexCall].getOperands.head
        val valuesNode = equalsNodes.map(_.asInstanceOf[RexCall].getOperands.last)
        val inNode = rexBuilder.makeCall(IN, List(inputRef) ++ valuesNode)
        if (nonEqualsNodes.isEmpty) {
          Seq(inNode)
        } else {
          Seq(inNode, RexUtil.composeDisjunction(rexBuilder, nonEqualsNodes, false))
        }
      } else {
        Seq(RexUtil.composeDisjunction(rexBuilder, r, false))
      }
    }.toSeq

    combined ++ nonCombinations
  }

  /**
    * Returns a percentage of rows meeting an AND condition with same RexInputRef in Filter node.
    * Each sub-condition in AND is a binary comparison condition,
    * and only binary comparison operator <, <=, >, >= are supported now.
    *
    * The condition is like: `a > 10 AND a < 20`
    *
    * @param andPredicate predicate whose selectivity is to be estimated against scan's output.
    * @return an optional double value to show the percentage of rows meeting a given condition.
    *         It returns None if the condition is not supported.
    */
  private def estimateAndPredicate(andPredicate: RexCall): Option[Double] = {
    val cnf = FlinkRexUtil.toCnf(rexBuilder, maxCnfNodeCount, andPredicate)
    val conjunctions = RelOptUtil.conjunctions(cnf)

    val inputRefs = mutable.HashSet[Int]()
    val supported = conjunctions.forall {
      case c: RexCall =>
        c.getOperator match {
          case GREATER_THAN | GREATER_THAN_OR_EQUAL | LESS_THAN | LESS_THAN_OR_EQUAL =>
            val (left, right) = (c.operands.get(0), c.operands.get(1))
            (left, right) match {
              case (i: RexInputRef, _: RexLiteral) => inputRefs += i.getIndex
              case (_: RexLiteral, i: RexInputRef) => inputRefs += i.getIndex
              case _ => throw new RuntimeException("This should not happen.")
            }
            // TODO support partition predicate and non-numeric type
            !isSupportedPartitionPredicate(c) &&
              canConvertToNumericType(left.getType) &&
              canConvertToNumericType(right.getType)
          case _ => throw new RuntimeException("This should not happen.")
        }
      case _ => throw new RuntimeException("This should not happen.")
    }
    require(inputRefs.size == 1)

    if (!supported) {
      val selectivity = conjunctions.map(estimateOperand)
      return Some(selectivity.product)
    }

    val predicateIntervals = conjunctions.map {
      case c: RexCall =>
        val (left, right) = (c.operands.get(0), c.operands.get(1))
        c.getOperator match {
          case GREATER_THAN =>
            (left, right) match {
              case (i: RexInputRef, l: RexLiteral) =>
                ValueInterval(literalToDouble(l), null, includeLower = false, includeUpper = false)
              case (l: RexLiteral, _: RexInputRef) =>
                ValueInterval(null, literalToDouble(l), includeLower = false, includeUpper = false)
              case _ => throw new RuntimeException("This should not happen.")
            }
          case GREATER_THAN_OR_EQUAL =>
            (left, right) match {
              case (_: RexInputRef, l: RexLiteral) =>
                ValueInterval(literalToDouble(l), null, includeLower = true, includeUpper = false)
              case (l: RexLiteral, _: RexInputRef) =>
                ValueInterval(null, literalToDouble(l), includeLower = false, includeUpper = true)
              case _ => throw new RuntimeException("This should not happen.")
            }
          case LESS_THAN =>
            (left, right) match {
              case (_: RexInputRef, l: RexLiteral) =>
                ValueInterval(null, literalToDouble(l), includeLower = false, includeUpper = false)
              case (l: RexLiteral, _: RexInputRef) =>
                ValueInterval(literalToDouble(l), null, includeLower = false, includeUpper = false)
              case _ => throw new RuntimeException("This should not happen.")
            }
          case LESS_THAN_OR_EQUAL =>
            (left, right) match {
              case (_: RexInputRef, l: RexLiteral) =>
                ValueInterval(null, literalToDouble(l), includeLower = false, includeUpper = true)
              case (l: RexLiteral, _: RexInputRef) =>
                ValueInterval(literalToDouble(l), null, includeLower = true, includeUpper = false)
              case _ => throw new RuntimeException("This should not happen.")
            }
          case _ => throw new RuntimeException("This should not happen.")
        }
      case _ => throw new RuntimeException("This should not happen.")
    }

    estimateAndNumericComparison(inputRefs.head, predicateIntervals)
  }

  /**
    * Returns a percentage of rows meeting an AND condition and
    * each sub-condition is a binary numeric comparison expression.
    * This method evaluate expression for Numeric/Boolean/Date/Time/Timestamp columns.
    */
  private def estimateAndNumericComparison(
      inputRefIndex: Int,
      intervals: Seq[ValueInterval]): Option[Double] = {
    require(intervals.nonEmpty)
    val default = Some(math.pow(defaultComparisonSelectivity.get, intervals.size))

    val columnInterval = mq.getColumnInterval(rel, inputRefIndex)
    if (columnInterval == null) {
      return default
    }

    // convert values in columnInterval to double type
    val convertedInterval = convertValueInterval(columnInterval, SqlTypeFamily.NUMERIC)
    val andInterval = intervals.foldLeft(convertedInterval)(ValueInterval.intersect)

    columnInterval match {
      case ValueInterval.empty => Some(0.0)
      case ValueInterval.infinite => default
      case _: LeftSemiInfiniteValueInterval => default
      case _: RightSemiInfiniteValueInterval => default
      case v: FiniteValueInterval =>
        andInterval match {
          case ValueInterval.empty => Some(0.0)
          case lv: FiniteValueInterval =>
            // TODO not take include min/max into consideration now
            val (min, max) = (comparableToDouble(v.lower), comparableToDouble(v.upper))
            val (litMin, litMax) = (comparableToDouble(lv.lower), comparableToDouble(lv.upper))
            Some((litMax - litMin) / (max - min))
          case _ =>
            // predicate like `a > 10 AND a > 20` had been simplified in `evaluate` method
            throw new RuntimeException("This should not happen.")
        }
    }
  }

  /**
    * Returns a percentage of rows meeting an OR condition with same RexInputRef in Filter node.
    * Each sub-condition in OR is a binary comparison condition,
    * and only binary comparison operator <, <=, >, >= are supported now.
    *
    * The condition is like: `a > 20 OR a < 10`
    *
    * @param orPredicate predicate whose selectivity is to be estimated against scan's output.
    * @return an optional double value to show the percentage of rows meeting a given condition.
    *         It returns None if the condition is not supported.
    */
  private def estimateOrPredicate(orPredicate: RexCall): Option[Double] = {
    val disjunctions = RelOptUtil.disjunctions(orPredicate)
    val result = disjunctions.map(estimateOperand)
    Some(math.min(1.0, result.sum))
  }

  /**
    * Returns whether a predicate is supported partition-pruning predicate.
    *
    * @param singlePredicate predicate whose selectivity is to be estimated against scan's output.
    * @return true if the TableSource in scan is PartitionableTableSource and the columns in the
    *         predicate are all partition columns, otherwise false.
    */
  private def isSupportedPartitionPredicate(singlePredicate: RexCall): Boolean = {
    rel match {
      case ts: TableScan =>
        val partitionFieldNames = getPartitionFieldNames(ts)
        partitionFieldNames match {
          // partitionable table
          case Some(names) if names.nonEmpty =>
            val (convertedPredicates, unconvertedRexNodes) =
              RexNodeExtractor.extractConjunctiveConditions(
                singlePredicate,
                maxCnfNodeCount,
                ts.getRowType.getFieldNames,
                ts.getCluster.getRexBuilder,
                FlinkRelOptUtil.getFunctionCatalog(ts))
            val (_, nonPartitionPredicates) =
              PartitionPredicateExtractor.extractPartitionPredicates(convertedPredicates, names)
            unconvertedRexNodes.isEmpty && nonPartitionPredicates.isEmpty

          // non-partitionable table
          case _ => false
        }
      case _ => false
    }
  }

  /**
    * Returns a percentage of rows meeting a partition expression.
    *
    * @param partitionPredicate partition predicate whose selectivity is to be estimated against
    *                           scan's output.
    * @return an optional double value to show the percentage of rows meeting a given condition.
    *         It returns None if the condition is not supported.
    */
  private def estimatePartitionPredicate(partitionPredicate: RexCall): Option[Double] = {
    assert(this.rel.isInstanceOf[TableScan])
    val table = rel.asInstanceOf[TableScan].getTable.unwrap(classOf[FlinkTable])
    val partitionableTableSource = table match {
      case t: TableSourceTable =>
        t.tableSource match {
          case p: PartitionableTableSource => p
          case _ => throw new RuntimeException("This should not happen.")
        }
      case _ => throw new RuntimeException("This should not happen.")
    }

    // TODO deal with filter partial push-down
    if (partitionableTableSource.isFilterPushedDown) {
      // Ignore any predicates on partition columns because we have already
      // accounted for these in the Table row count.
      Some(1.0)
    } else {
      // We do not use PartitionableTableSource#getAllPartitions method to get the number of
      // partitions, because this method may involve some heavy work(e.g. reads metadata using I/O).
      // We assume the number of partitions is 100 here.
      val numOfPartitions = 100
      partitionPredicate.getOperator match {
        case EQUALS => Some(1.0 / numOfPartitions)
        case NOT_EQUALS => Some(1.0 - 1.0 / numOfPartitions)
        case IS_NULL => Some(0.0)
        case IS_NOT_NULL => Some(1.0)
        case IN =>
          val inSet = partitionPredicate.getOperands.subList(1, partitionPredicate.getOperands.size)
          checkInSet(inSet)
          val sizeOfInSet = inSet.size()
          Some(Math.min(1.0, sizeOfInSet * 1.0 / numOfPartitions))
        case _ => Some(5 * (1.0 / numOfPartitions))
      }
    }
  }

  /**
    * Returns a percentage of rows meeting a binary comparison expression containing two columns.
    *
    * We will use default comparison selectivity(0.5), in the following cases:
    * 1. the type is not supported
    * 2. the operator is not supported
    * 3. the specified column's stats is empty
    *
    * @param op    a binary comparison operator, including =, <=>, <, <=, >, >=
    * @param left  the left RexInputRef
    * @param right the right RexInputRef
    * @return an optional double value to show the percentage of rows meeting a given condition.
    *         It returns None if no statistics collected for a given column.
    */
  private def estimateComparison(op: SqlOperator, left: RexNode, right: RexNode): Option[Double] = {
    if (!isSupportedComparisonType(left.getType) || !isSupportedComparisonType(right.getType)) {
      val default = op match {
        case EQUALS => defaultEqualsSelectivity
        case _ => defaultComparisonSelectivity
      }
      return default
    }

    op match {
      case EQUALS => (left, right) match {
        case (i: RexInputRef, l: RexLiteral) => estimateEquals(i, l)
        case (l: RexLiteral, i: RexInputRef) => estimateEquals(i, l)
        case (l: RexInputRef, r: RexInputRef) => estimateComparison(EQUALS, l, r)
        case _ => defaultEqualsSelectivity
      }
      case LESS_THAN => (left, right) match {
        case (i: RexInputRef, l: RexLiteral) => estimateComparison(LESS_THAN, i, l)
        case (l: RexLiteral, i: RexInputRef) => estimateComparison(GREATER_THAN, i, l)
        case (l: RexInputRef, r: RexInputRef) => estimateComparison(LESS_THAN, l, r)
        case _ => defaultComparisonSelectivity
      }
      case LESS_THAN_OR_EQUAL => (left, right) match {
        case (i: RexInputRef, l: RexLiteral) => estimateComparison(LESS_THAN_OR_EQUAL, i, l)
        case (l: RexLiteral, i: RexInputRef) => estimateComparison(GREATER_THAN_OR_EQUAL, i, l)
        case (l: RexInputRef, r: RexInputRef) => estimateComparison(LESS_THAN_OR_EQUAL, l, r)
        case _ => defaultComparisonSelectivity
      }
      case GREATER_THAN => (left, right) match {
        case (i: RexInputRef, l: RexLiteral) => estimateComparison(GREATER_THAN, i, l)
        case (l: RexLiteral, i: RexInputRef) => estimateComparison(LESS_THAN, i, l)
        case (l: RexInputRef, r: RexInputRef) => estimateComparison(GREATER_THAN, l, r)
        case _ => defaultComparisonSelectivity
      }
      case GREATER_THAN_OR_EQUAL => (left, right) match {
        case (i: RexInputRef, l: RexLiteral) => estimateComparison(GREATER_THAN_OR_EQUAL, i, l)
        case (l: RexLiteral, i: RexInputRef) => estimateComparison(LESS_THAN_OR_EQUAL, i, l)
        case (l: RexInputRef, r: RexInputRef) => estimateComparison(GREATER_THAN_OR_EQUAL, l, r)
        case _ => defaultComparisonSelectivity
      }
      case _ => defaultComparisonSelectivity
    }
  }

  /**
    * Returns a percentage of rows meeting an equality (=) expression.
    *
    * @param inputRef a RexInputRef
    * @param literal  a literal value (or constant)
    * @return an optional double value to show the percentage of rows meeting a given condition.
    *         It returns None if no statistics collected for a given column.
    */
  private def estimateEquals(inputRef: RexInputRef, literal: RexLiteral): Option[Double] = {
    if (literal.isNull) {
      return estimateIsNull(inputRef)
    }
    val columnInterval = mq.getColumnInterval(rel, inputRef.getIndex)
    if (columnInterval == null) {
      return defaultEqualsSelectivity
    }
    val convertedInterval = convertValueInterval(columnInterval, inputRef.getType)
    convertedInterval match {
      case ValueInterval.infinite => defaultEqualsSelectivity
      case ValueInterval.empty => Some(0.0)
      case _ =>
        if (ValueInterval.contains(convertedInterval, literalToComparable(literal))) {
          val bitSetOfInputRef = ImmutableBitSet.of(inputRef.getIndex)
          val ndv = mq.getDistinctRowCount(rel, bitSetOfInputRef, null)
          if (ndv == null) {
            defaultEqualsSelectivity
          } else {
            // there is no possible that ndv < 1.0 because of the protection in RelMetadataQuery.
            Some(1.0 / ndv)
          }
        } else {
          Some(0.0)
        }
    }
  }

  /**
    * Returns a percentage of rows meeting a binary comparison expression.
    *
    * @param op       a binary comparison operator, including <, <=, >, >=
    * @param inputRef a RexInputRef
    * @param literal  a literal value (or constant)
    * @return an optional double value to show the percentage of rows meeting a given condition.
    *         It returns None if no statistics collected for a given column.
    */
  private def estimateComparison(
      op: SqlOperator,
      inputRef: RexInputRef,
      literal: RexLiteral): Option[Double] = {
    if (literal.isNull) {
      throw new IllegalArgumentException("Numeric comparison does not support null literal here.")
    }
    if (canConvertToNumericType(inputRef.getType)) {
      estimateNumericComparison(op, inputRef, literal)
    } else {
      // TODO: It is difficult to support binary comparisons for non-numeric type
      // without advanced statistics like histogram.
      defaultComparisonSelectivity
    }
  }

  /**
    * Returns a percentage of rows meeting a binary numeric comparison expression.
    * This method evaluate expression for Numeric/Boolean/Date/Time/Timestamp columns.
    *
    * @param op       a binary comparison operator, including <, <=, >, >=
    * @param inputRef a RexInputRef
    * @param literal  a literal value (or constant)
    * @return an optional double value to show the percentage of rows meeting a given condition.
    *         It returns None if no statistics collected for a given column.
    */
  private def estimateNumericComparison(
      op: SqlOperator,
      inputRef: RexInputRef,
      literal: RexLiteral): Option[Double] = {
    val columnInterval = mq.getColumnInterval(rel, inputRef.getIndex)
    if (columnInterval == null) {
      return defaultComparisonSelectivity
    }
    columnInterval match {
      case ValueInterval.infinite => defaultComparisonSelectivity
      case ValueInterval.empty => Some(0.0)
      case _ =>
        val (min, includeMin) = columnInterval match {
          case hasLower: WithLower => (comparableToDouble(hasLower.lower), hasLower.includeLower)
          case _ => (null, true)
        }
        val (max, includeMax) = columnInterval match {
          case hasUpper: WithUpper => (comparableToDouble(hasUpper.upper), hasUpper.includeUpper)
          case _ => (null, true)
        }
        val lit = literalToDouble(literal)
        val (noOverlap, completeOverlap) = op match {
          case LESS_THAN => (
            greaterThanOrEqualTo(min, lit),
            if (includeMax) lessThan(max, lit) else lessThanOrEqualTo(max, lit))
          case LESS_THAN_OR_EQUAL => (
            if (includeMin) greaterThan(min, lit) else greaterThanOrEqualTo(min, lit),
            lessThanOrEqualTo(max, lit))
          case GREATER_THAN => (
            lessThanOrEqualTo(max, lit),
            if (includeMin) greaterThan(min, lit) else greaterThanOrEqualTo(min, lit))
          case GREATER_THAN_OR_EQUAL => (
            if (includeMax) lessThan(max, lit) else lessThanOrEqualTo(max, lit),
            greaterThanOrEqualTo(min, lit))
        }

        val selectivity = if (noOverlap) {
          0.0
        } else if (completeOverlap) {
          1.0
        } else if (min != null && max != null) {
          val bitSetOfInputRef = ImmutableBitSet.of(inputRef.getIndex)
          val ndv = mq.getDistinctRowCount(rel, bitSetOfInputRef, null)
          op match {
            // there is no possible that ndv < 1.0 because of the protection in RelMetadataQuery.
            case LESS_THAN =>
              if (doubleEquals(lit, max)) {
                if (includeMax) {
                  Option(ndv).map(v => 1.0 - (1.0 / v))
                    .getOrElse(defaultComparisonSelectivity.get)
                } else {
                  1.0
                }
              } else {
                // TODO not take includeMin into consideration now
                (lit - min) / (max - min)
              }
            case LESS_THAN_OR_EQUAL =>
              if (doubleEquals(lit, min)) {
                if (includeMin) {
                  // The boundary value is the only satisfying value.
                  Option(ndv).map(1.0 / _).getOrElse(defaultComparisonSelectivity.get)
                } else {
                  0.0
                }
              } else {
                // TODO not take includeMax into consideration now
                (lit - min) / (max - min)
              }
            case GREATER_THAN =>
              if (doubleEquals(lit, min)) {
                if (includeMin) {
                  Option(ndv).map(v => 1.0 - (1.0 / v))
                    .getOrElse(defaultComparisonSelectivity.get)
                } else {
                  1.0
                }
              } else {
                // TODO not take includeMin into consideration now
                (max - lit) / (max - min)
              }
            case GREATER_THAN_OR_EQUAL =>
              if (doubleEquals(lit, max)) {
                if (includeMax) {
                  Option(ndv).map(1.0 / _).getOrElse(defaultComparisonSelectivity.get)
                } else {
                  0.0
                }
              } else {
                // TODO not take includeMax into consideration now
                (max - lit) / (max - min)
              }
          }
        } else {
          defaultComparisonSelectivity.get
        }
        Some(selectivity)
    }
  }

  /**
    * Returns a percentage of rows meeting a binary comparison expression containing two columns.
    * In SQL queries, we also see predicate expressions involving two columns
    * such as "column-1 (op) column-2" where column-1 and column-2 belong to same table.
    * Note that, if column-1 and column-2 belong to different tables, then it is a join
    * operator's work, NOT a filter operator's work.
    *
    * @param op    a binary comparison operator, including =, <, <=, >, >=
    * @param left  the left RexInputRef
    * @param right the right RexInputRef
    * @return an optional double value to show the percentage of rows meeting a given condition.
    *         It returns None if no statistics collected for a given column.
    */
  private def estimateComparison(
      op: SqlOperator,
      left: RexInputRef,
      right: RexInputRef): Option[Double] = {
    if (canConvertToNumericType(left.getType) && canConvertToNumericType(right.getType)) {
      estimateNumericComparison(op, left, right)
    } else {
      // TODO: It is difficult to support binary comparisons for non-numeric type
      // without advanced statistics like histogram.
      op match {
        case EQUALS => defaultEqualsSelectivity
        case _ => defaultComparisonSelectivity
      }
    }
  }

  /**
    * Returns a percentage of rows meeting a binary numeric comparison expression
    * containing two columns.
    * This method evaluate expression for Numeric/Boolean/Date/Time/Timestamp columns.
    *
    * @param op    a binary comparison operator, including =, <, <=, >, >=
    * @param left  the left RexInputRef
    * @param right the right RexInputRef
    * @return an optional double value to show the percentage of rows meeting a given condition.
    *         It returns None if no statistics collected for a given column.
    */
  private def estimateNumericComparison(
      op: SqlOperator,
      left: RexInputRef,
      right: RexInputRef): Option[Double] = {
    val selectivityWithoutStats = op match {
      case EQUALS => defaultEqualsSelectivity
      case _ => defaultComparisonSelectivity
    }
    val leftInterval = mq.getColumnInterval(this.rel, left.getIndex)
    val rightInterval = mq.getColumnInterval(this.rel, right.getIndex)
    if (leftInterval == null ||
      leftInterval == ValueInterval.infinite ||
      rightInterval == null ||
      rightInterval == ValueInterval.infinite) {
      return selectivityWithoutStats
    } else if (leftInterval == ValueInterval.empty || rightInterval == ValueInterval.empty) {
      return Some(0.0)
    }

    val leftNullCount = mq.getColumnNullCount(this.rel, left.getIndex)
    val rightNullCount = mq.getColumnNullCount(this.rel, right.getIndex)
    if (leftNullCount == null || rightNullCount == null) {
      return selectivityWithoutStats
    }

    val bitSetOfLeftInputRef = ImmutableBitSet.of(left.getIndex)
    val leftNdv = mq.getDistinctRowCount(this.rel, bitSetOfLeftInputRef, null)
    val bitSetOfRightInputRef = ImmutableBitSet.of(right.getIndex)
    val rightNdv = mq.getDistinctRowCount(this.rel, bitSetOfRightInputRef, null)
    if (op.equals(EQUALS) &&
      (!(leftInterval.isInstanceOf[FiniteValueInterval]
        && rightInterval.isInstanceOf[FiniteValueInterval])) ||
      leftNdv == null || rightNdv == null) {
      return defaultEqualsSelectivity
    }
    // left interval
    val (leftMin, leftIncludeMin) = leftInterval match {
      case hasLower: WithLower => (comparableToDouble(hasLower.lower), hasLower.includeLower)
      case _ => (null, true)
    }
    val (leftMax, leftIncludeMax) = leftInterval match {
      case hasUpper: WithUpper => (comparableToDouble(hasUpper.upper), hasUpper.includeUpper)
      case _ => (null, true)
    }
    val (rightMin, rightIncludeMin) = rightInterval match {
      case hasLower: WithLower => (comparableToDouble(hasLower.lower), hasLower.includeLower)
      case _ => (null, true)
    }
    val (rightMax, rightIncludeMax) = rightInterval match {
      case hasUpper: WithUpper => (comparableToDouble(hasUpper.upper), hasUpper.includeUpper)
      case _ => (null, true)
    }
    // determine the overlapping degree between predicate interval and column's interval
    val (noOverlap, completeOverlap) = op match {
      // Left < Right or Left <= Right
      // - no overlap:
      //      rightMin           rightMax     leftMin       leftMax
      // --------+------------------+------------+-------------+------->
      // - complete overlap: (If null values exists, we set it to partial overlap.)
      //      leftMin            leftMax      rightMin      rightMax
      // --------+------------------+------------+-------------+------->
      case LESS_THAN =>
        (
          greaterThanOrEqualTo(leftMin, rightMax),
          if (leftIncludeMax && rightIncludeMin) {
            lessThan(leftMax, rightMin)
          } else {
            lessThanOrEqualTo(leftMax, rightMin)
          })
      case LESS_THAN_OR_EQUAL =>
        (
          if (leftIncludeMin && rightIncludeMax) {
            greaterThan(leftMin, rightMax)
          } else {
            greaterThanOrEqualTo(leftMin, rightMax)
          },
          lessThanOrEqualTo(leftMax, rightMin))
      // Left > Right or Left >= Right
      // - no overlap:
      //      leftMin            leftMax      rightMin      rightMax
      // --------+------------------+------------+-------------+------->
      // - complete overlap: (If null values exists, we set it to partial overlap.)
      //      rightMin           rightMax     leftMin       leftMax
      // --------+------------------+------------+-------------+------->
      case GREATER_THAN =>
        (
          lessThanOrEqualTo(leftMax, rightMin),
          if (leftIncludeMin && rightIncludeMax) {
            greaterThan(leftMin, rightMax)
          } else {
            greaterThanOrEqualTo(leftMin, rightMax)
          })
      case GREATER_THAN_OR_EQUAL =>
        (
          if (leftIncludeMax && rightIncludeMin) {
            lessThan(leftMax, rightMin)
          } else {
            lessThanOrEqualTo(leftMax, rightMin)
          },
          greaterThanOrEqualTo(leftMin, rightMax))
      // Left = Right
      // - no overlap:
      //      leftMin            leftMax      rightMin      rightMax
      // --------+------------------+------------+-------------+------->
      //      rightMin           rightMax     leftMin       leftMax
      // --------+------------------+------------+-------------+------->
      // - complete overlap:
      //      leftMin            leftMin
      //      rightMin           rightMax
      // --------+------------------+------->
      case EQUALS =>
        // now, min/max/ndv is not null and ndv >= 0
        val isLeftSmallerThanRight = if (leftIncludeMax && rightIncludeMin) {
          leftMax < rightMin
        } else {
          leftMax <= rightMin
        }
        val isLeftBiggerThanRight = if (rightIncludeMax && leftIncludeMin) {
          rightMax < leftMin
        } else {
          rightMax <= leftMin
        }
        (
          isLeftSmallerThanRight || isLeftBiggerThanRight,
          (leftMin == rightMin) && (leftMax == rightMax)
            && (leftIncludeMin == rightIncludeMin)
            && (leftIncludeMax == rightIncludeMax)
            && (leftNdv == rightNdv))
    }
    val allNotNull = (leftNullCount == 0) && (rightNullCount == 0)
    val selectivity = if (noOverlap) {
      0.0
    } else if (completeOverlap && allNotNull) {
      1.0
    } else {
      // For partial overlap, we use an empirical value 1/3 as suggested by the book
      // "Database Systems, the complete book".
      1.0 / 3.0
    }
    Some(selectivity)
  }

  /**
    * Returns a percentage of rows meeting "IS NULL" condition.
    *
    * We will use default is_null selectivity(0.1), if the column stats is empty.
    *
    * @param input a RexNode
    * @return an optional double value to show the percentage of rows meeting a given condition
    *         It returns None if no statistics collected for a given column.
    */
  private def estimateIsNull(input: RexNode): Option[Double] = {
    val inputRef = convertToRexInputRef(input)
    val nullCount = mq.getColumnNullCount(this.rel, inputRef.getIndex)
    if (nullCount == null) {
      return defaultIsNullSelectivity
    }

    val rowCount = mq.getRowCount(this.rel)
    if (rowCount == null) {
      defaultIsNullSelectivity
    } else {
      // there is no possible that rowCount is 0 because of the protection in RelMetadataQuery.
      Some(Math.min(nullCount / rowCount, 1.0))
    }
  }

  /**
    * Returns a percentage of rows meeting "IS NOT NULL" condition.
    *
    * We will use default is_not_null selectivity(0.9), if the column stats is empty.
    *
    * @param input a RexNode
    * @return an optional double value to show the percentage of rows meeting a given condition
    *         It returns None if no statistics collected for a given column.
    */
  private def estimateIsNotNull(input: RexNode): Option[Double] = {
    val inputRef = convertToRexInputRef(input)
    val nullCount = mq.getColumnNullCount(this.rel, inputRef.getIndex)
    if (nullCount == null) {
      return defaultIsNotNullSelectivity
    }

    val rowCount = mq.getRowCount(this.rel)
    val selectivity = if (rowCount == 0) {
      0.0
    } else {
      1.0 - (nullCount / rowCount)
    }
    Some(selectivity)
  }

  /**
    * Returns a percentage of rows meeting "IN" operator expression.
    *
    * @param input a RexNode
    * @param inSet a set of literal values
    * @return an optional double value to show the percentage of rows meeting a given condition
    *         It returns None if no statistics exists for a given column.
    */
  private def estimateIn(input: RexNode, inSet: Seq[RexNode]): Option[Double] = {
    val inputRef = convertToRexInputRef(input)
    checkInSet(inSet)

    val defaultInSelectivity = Some(Math.min(1.0, defaultEqualsSelectivity.get * inSet.size))

    if (!isSupportedComparisonType(input.getType)) {
      return defaultInSelectivity
    }
    val columnInterval = mq.getColumnInterval(this.rel, inputRef.getIndex)
    if (columnInterval == null) {
      val ndv = mq.getDistinctRowCount(rel, ImmutableBitSet.of(inputRef.getIndex), null)
      return if (ndv == null) {
        defaultInSelectivity
      } else {
        Some(inSet.size / ndv)
      }
    }

    val interval = convertValueInterval(columnInterval, inputRef.getType)
    interval match {
      case ValueInterval.infinite => defaultInSelectivity
      case ValueInterval.empty => Some(0.0)
      case _ =>
        val validInSet = inSet.filter(
          v => ValueInterval.contains(interval, literalToComparable(v.asInstanceOf[RexLiteral])))

        if (validInSet.isEmpty) {
          Some(0.0)
        } else {
          val bitSetOfInputRef = ImmutableBitSet.of(inputRef.getIndex)
          val ndv = mq.getDistinctRowCount(rel, bitSetOfInputRef, null)
          if (ndv == null) {
            defaultInSelectivity
          } else {
            // there is no possible that ndv < 1.0 because of the protection in RelMetadataQuery.
            // newNdv should not be greater than the old ndv. For example, column has only 2 values
            // 1 and 6. The predicate column IN (1, 2, 3, 4, 5). validSet.size is 5.
            val newNdv = Math.min(ndv, validInSet.size)
            // return the filter selectivity. Without advanced statistics such as histograms,
            // we have to assume uniform distribution.
            Some(Math.min(1.0, newNdv * 1.0 / ndv))
          }
        }
    }
  }

  /**
    * Checks whether all values are literal. If contains non-literal values, throw an exception.
    */
  private def checkInSet(inSet: Seq[RexNode]): Unit = {
    if (inSet.exists(!_.isA(SqlKind.LITERAL))) {
      throw new IllegalArgumentException(s"${
        inSet.mkString(",")
      } contain some non-literal values.")
    }
  }

  /**
    * If a rexNode is RexInputRef type, convert it to RexInputRef; else throw an exception.
    */
  private def convertToRexInputRef(rexNode: RexNode): RexInputRef = {
    rexNode match {
      case i: RexInputRef => i
      case _ => throw new IllegalArgumentException(s"$rexNode is not a RexInputRef.")
    }
  }

  private def doubleEquals(v1: Double, v2: Double): Boolean = Math.abs(v1 - v2) < 1e-6

}

object SelectivityEstimator {

  /**
    * Convert ValueInterval,
    * for Numeric/Boolean/Date/Time/Timestamp, min/max will be converted to Double,
    * for CHARACTER, min/max will be converted to String.
    */
  def convertValueInterval(interval: ValueInterval, dataType: RelDataType): ValueInterval = {
    require(interval != null && dataType != null)
    convertValueInterval(interval, dataType.getFamily)
  }

  /**
    * Convert ValueInterval,
    * for Numeric/Boolean/Date/Time/Timestamp, min/max will be converted to Double,
    * for CHARACTER, min/max will be converted to String.
    */
  def convertValueInterval(
      interval: ValueInterval,
      typeFamily: RelDataTypeFamily): ValueInterval = {
    require(interval != null && typeFamily != null)
    interval match {
      case ValueInterval.empty | ValueInterval.infinite => interval
      case _ =>
        val (lower, includeLower) = interval match {
          case li: WithLower => (li.lower, li.includeLower)
          case _ => (null, false)
        }
        val (upper, includeUpper) = interval match {
          case ui: WithUpper => (ui.upper, ui.includeUpper)
          case _ => (null, false)
        }
        typeFamily match {
          case SqlTypeFamily.NUMERIC | SqlTypeFamily.BOOLEAN | SqlTypeFamily.DATE |
               SqlTypeFamily.TIME | SqlTypeFamily.TIMESTAMP =>
            ValueInterval(
              comparableToDouble(lower),
              comparableToDouble(upper),
              includeLower,
              includeUpper)
          case SqlTypeFamily.CHARACTER =>
            ValueInterval(
              lower.toString,
              upper.toString,
              includeLower,
              includeUpper)
          case _ => throw new UnsupportedOperationException(s"Unsupported typeFamily: $typeFamily")
        }
    }
  }

  /**
    * Get literal value as Comparable.
    *
    * The class may be different between literal value and min/max value,
    * and different classes can't be compared. So
    * for Numeric/Boolean/Date/Time/Timestamp, literal will be converted to Double uniformly,
    * for CHARACTER, literal will be converted to String.
    *
    * e.g. min/max values are Double: min=10.0 and max=100.0,
    * however the literal values are Integer: select * from tb where a in (20, 30, 40)
    */
  def literalToComparable(literal: RexLiteral): Comparable[_] = {
    if (!literal.isNull) {
      literal.getType.getFamily match {
        case SqlTypeFamily.NUMERIC | SqlTypeFamily.BOOLEAN | SqlTypeFamily.DATE |
             SqlTypeFamily.TIME | SqlTypeFamily.TIMESTAMP => literalToDouble(literal)
        case SqlTypeFamily.CHARACTER => literal.getValueAs(classOf[String])
        case _ => throw new UnsupportedOperationException(
          s"Can't get value as comparable from literal type: ${literal.getType}")
      }
    } else {
      null
    }
  }

  def comparableToDouble(value: Any): JDouble = {
    value match {
      case null => null
      case v: Number => v.doubleValue()
      case v: Boolean => if (v) 1.0 else 0.0
      // Returns days since 1970-01-01 for Date type value
      case v: java.sql.Date => JDouble.valueOf(v.getTime / DateTimeUtils.MILLIS_PER_DAY)
      // Returns milliseconds since 1970-01-01 00:00:00 for Timestamp type value
      case v: java.sql.Timestamp => JDouble.valueOf(v.getTime)
      // Returns milliseconds since midnight for Time type value
      case v: java.sql.Time => JDouble.valueOf(new TimeString(v.toString).getMillisOfDay)
      case _ =>
        throw new UnsupportedOperationException(
          s"Can't convert comparable type: ${value.getClass} to Double")
    }
  }

  def literalToDouble(literal: RexLiteral): JDouble = {
    if (!literal.isNull) {
      literal.getType.getFamily match {
        case SqlTypeFamily.NUMERIC =>
          literal.getValue3.toString.toDouble
        case SqlTypeFamily.BOOLEAN =>
          if (RexLiteral.booleanValue(literal)) 1.0 else 0.0
        case SqlTypeFamily.DATE | SqlTypeFamily.TIME | SqlTypeFamily.TIMESTAMP =>
          literal.getValue2.toString.toDouble
        case _ =>
          throw new UnsupportedOperationException(
            s"Can't get value as Double from literal type: ${literal.getType}")
      }
    } else {
      null
    }
  }

  def lessThan(left: Comparable[_], right: Comparable[_]): Boolean = {
    if (left == null || right == null) {
      false
    } else {
      left.asInstanceOf[Comparable[Any]].compareTo(right.asInstanceOf[Comparable[Any]]) < 0
    }
  }

  def lessThanOrEqualTo(left: Comparable[_], right: Comparable[_]): Boolean = {
    if (left == null || right == null) {
      false
    } else {
      left.asInstanceOf[Comparable[Any]].compareTo(right.asInstanceOf[Comparable[Any]]) <= 0
    }
  }

  def greaterThan(left: Comparable[_], right: Comparable[_]): Boolean = {
    if (left == null || right == null) {
      false
    } else {
      left.asInstanceOf[Comparable[Any]].compareTo(right.asInstanceOf[Comparable[Any]]) > 0
    }
  }

  def greaterThanOrEqualTo(left: Comparable[_], right: Comparable[_]): Boolean = {
    if (left == null || right == null) {
      false
    } else {
      left.asInstanceOf[Comparable[Any]].compareTo(right.asInstanceOf[Comparable[Any]]) >= 0
    }
  }

  /**
    * Currently, Numeric/Boolean/String/Date/Time/Timestamp are supported.
    */
  def isSupportedComparisonType(relType: RelDataType): Boolean = {
    relType.getFamily match {
      case SqlTypeFamily.NUMERIC | SqlTypeFamily.BOOLEAN | SqlTypeFamily.CHARACTER |
           SqlTypeFamily.DATE | SqlTypeFamily.TIME | SqlTypeFamily.TIMESTAMP => true
      case _ => false
    }
  }

  /**
    * Currently, value of Numeric/Boolean/Date/Time/Timestamp column can be converted to a number.
    */
  def canConvertToNumericType(relType: RelDataType): Boolean = {
    relType.getFamily match {
      case SqlTypeFamily.NUMERIC | SqlTypeFamily.BOOLEAN |
           SqlTypeFamily.DATE | SqlTypeFamily.TIME | SqlTypeFamily.TIMESTAMP => true
      case _ => false
    }
  }

}
