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
package org.apache.flink.table.runtime.aggregate

import java.util.Comparator

import org.apache.calcite.rel.`type`._
import org.apache.calcite.rel.{RelCollation, RelFieldCollation}
import org.apache.calcite.rel.RelFieldCollation.Direction
import org.apache.flink.api.common.functions.{Comparator => FlinkComparator}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.typeutils.{TypeComparator, TypeSerializer}
import org.apache.flink.table.api.TableException
import org.apache.flink.table.calcite.FlinkPlannerImpl
import org.apache.flink.table.codegen.{CodeGenUtils, GeneratedSorter, SortCodeGenerator}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.runtime.sort.RecordComparator
import org.apache.flink.table.types.{BaseRowType, DataTypes, InternalType}
import org.apache.flink.table.typeutils.TypeUtils

import scala.collection.JavaConversions._
import scala.collection.mutable
/**
 * Class represents a collection of helper methods to build the sort logic.
 * It encapsulates as well the implementation for ordering and generic interfaces
 */
object SortUtil {

  def directionToOrder(direction: Direction): Order = {
    direction match {
      case Direction.ASCENDING | Direction.STRICTLY_ASCENDING => Order.ASCENDING
      case Direction.DESCENDING | Direction.STRICTLY_DESCENDING => Order.DESCENDING
      case _ => throw new IllegalArgumentException("Unsupported direction.")
    }
  }

  def getKeysAndOrders(fieldCollations: Seq[RelFieldCollation])
  : (Array[Int], Array[Boolean], Array[Boolean]) = {

    val fieldMappingDirections = fieldCollations
        .map(c => (c.getFieldIndex, directionToOrder(c.getDirection)))
    val keys = fieldMappingDirections.map(_._1)
    val orders = fieldMappingDirections.map(_._2 == Order.ASCENDING)

    val nullsIsLast = fieldCollations.map(_.nullDirection).map {
      case RelFieldCollation.NullDirection.LAST => true
      case RelFieldCollation.NullDirection.FIRST => false
      case RelFieldCollation.NullDirection.UNSPECIFIED =>
        throw new TableException(s"Do not support UNSPECIFIED for null order.")
    }.toArray

    deduplicationSortKeys(keys.toArray, orders.toArray, nullsIsLast)
  }

  def deduplicationSortKeys(
      keys: Array[Int],
      orders: Array[Boolean],
      nullsIsLast: Array[Boolean]): (Array[Int], Array[Boolean], Array[Boolean]) = {
    val keySet = new mutable.HashSet[Int]
    val keyBuffer = new mutable.ArrayBuffer[Int]
    val orderBuffer = new mutable.ArrayBuffer[Boolean]
    val nullsIsLastBuffer = new mutable.ArrayBuffer[Boolean]
    for (i <- keys.indices) {
      if (keySet.add(keys(i))) {
        keyBuffer += keys(i)
        orderBuffer += orders(i)
        nullsIsLastBuffer += nullsIsLast(i)
      }
    }
    (keyBuffer.toArray, orderBuffer.toArray, nullsIsLastBuffer.toArray)
  }

  /**
    * Creates a GeneratedSorter for the provided field collations and input type.
    *
    * @param inputType the row type of the input.
    * @param fieldCollations the field collations
    *
    * @return A GeneratedSorter for the provided sort collations and input type.
    */
  def createSorter(
      inputType: BaseRowType,
      fieldCollations: Seq[RelFieldCollation]): GeneratedSorter = {

    val (sortFields, sortDirections, nullsIsLast) = getKeysAndOrders(fieldCollations)
    createSorter(inputType.getFieldTypes, sortFields, sortDirections, nullsIsLast)
  }

  def createSorter(
      fieldTypes: Array[InternalType],
      sortFields: Array[Int],
      sortDirections: Array[Boolean],
      nullsIsLast: Array[Boolean]): GeneratedSorter = {
    // sort code gen
    val (comparators, serializers) = TypeUtils.flattenComparatorAndSerializer(
      fieldTypes.length, sortFields, sortDirections, fieldTypes)
    val codeGen = new SortCodeGenerator(sortFields, sortFields.map((key) =>
      fieldTypes(key)).map(DataTypes.internal), comparators, sortDirections, nullsIsLast)

    val comparator = codeGen.generateRecordComparator("StreamExecSortComparator")
    val computor = codeGen.generateNormalizedKeyComputer("StreamExecSortComputor")

    GeneratedSorter(computor, comparator, serializers, comparators)
  }

  /**
   * Returns the direction of the first sort field.
   *
   * @param collationSort The list of sort collations.
   * @return The direction of the first sort field.
   */
  def getFirstSortDirection(collationSort: RelCollation): Direction = {
    collationSort.getFieldCollations.get(0).direction
  }

  /**
   * Returns the first sort field.
   *
   * @param collationSort The list of sort collations.
   * @param rowType The row type of the input.
   * @return The first sort field.
   */
  def getFirstSortField(collationSort: RelCollation, rowType: RelDataType): RelDataTypeField = {
    val idx = collationSort.getFieldCollations.get(0).getFieldIndex
    rowType.getFieldList.get(idx)
  }

  /** Returns the default null direction if not specified. */
  def getNullDefaultOrders(ascendings: Array[Boolean]): Array[Boolean] = {
    ascendings.map { asc =>
      FlinkPlannerImpl.defaultNullCollation.last(!asc)
    }
  }

  def compareTo(o1: RelCollation, o2: RelCollation): Int = {
    val comp= o1.compareTo(o2)
    if (comp == 0) {
      val collations1 = o1.getFieldCollations
      val collations2 = o2.getFieldCollations
      for (index <- 0 until collations1.length) {
        val collation1 = collations1(index)
        val collation2 = collations2(index)
        val direction = collation1.direction.shortString.compareTo(
          collation2.direction.shortString)
        if (direction == 0) {
          val nullDirec = collation1.nullDirection.nullComparison.compare(
            collation2.nullDirection.nullComparison)
          if (nullDirec != 0) {
            return nullDirec
          }
        } else {
          return direction
        }
      }
    }
    comp
  }
}

/**
 * Wrapper for RecordComparator to a Java Comparator object
 */
class CollectionBaseRowComparator(
    private val rowComp: RecordComparator) extends Comparator[BaseRow] with Serializable {

  override def compare(arg0: BaseRow, arg1: BaseRow):Int = {
    rowComp.compare(arg0, arg1)
  }
}

class LazyBaseRowComparator(
    private val name: String,
    private val code: String,
    private val serializers: Array[TypeSerializer[_]],
    private val comparators: Array[TypeComparator[_]])
  extends FlinkComparator[BaseRow]
  with Serializable {

  @transient
  private var comparator: RecordComparator = _

  override def compare(o1: BaseRow, o2: BaseRow): Int = {
    if (comparator == null) {
      val clazz = CodeGenUtils.compile(Thread.currentThread().getContextClassLoader, name, code)
      comparator = clazz.newInstance()
      comparator.init(serializers, comparators)
    }
    comparator.compare(o1, o2)
  }

  override def equals(other: Any): Boolean = other match {
    case that: LazyBaseRowComparator =>
      this.name == that.name && this.code == that.code &&
        this.serializers.sameElements(that.serializers) &&
        this.comparators.sameElements(that.comparators)
    case _ => false
  }
}
