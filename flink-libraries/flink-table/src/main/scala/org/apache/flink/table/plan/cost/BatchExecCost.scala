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

package org.apache.flink.table.plan.cost

import org.apache.calcite.plan.{RelOptCost, RelOptUtil}

/**
  * This class is based on Apache Calcite's `org.apache.calcite.plan.volcano.VolcanoCost` and has
  * an adapted cost comparison method `isLe(other: RelOptCost)` that takes io, cpu, network
  * and memory into account.
  */
class BatchExecCost(
  override val rowCount: Double,
  override val cpu: Double,
  override val io: Double,
  override val network: Double,
  override val memory: Double)
  extends AbstractFlinkCost(rowCount, cpu, io, network, memory) {

  // The ratio to convert memory cost into CPU cost.
  val MEMORY_TO_CPU_RATIO = 1.0
  // The ratio to convert io cost into CPU cost.
  val IO_TO_CPU_RATIO = 2.0
  // The ratio to convert network cost into CPU cost.
  val NETWORK_TO_CPU_RATIO = 4.0

  def isInfinite: Boolean = {
    (this eq BatchExecCost.Infinity) ||
      (this.rowCount == Double.PositiveInfinity) ||
      (this.cpu == Double.PositiveInfinity) ||
      (this.io == Double.PositiveInfinity) ||
      (this.network == Double.PositiveInfinity) ||
      (this.memory == Double.PositiveInfinity)
  }

  def isLe(other: RelOptCost): Boolean = {
    val that: BatchExecCost = other.asInstanceOf[BatchExecCost]
    val cost1 = normalizeCost(this.memory, this.network, this.io)
    val cost2 = normalizeCost(that.memory, that.network, that.io)
    (this eq that) ||
      (this.cpu < that.cpu) ||
      (this.cpu == that.cpu && cost1 < cost2) ||
      (this.cpu == that.cpu && cost1 == cost2 && this.rowCount < that.rowCount)
  }

  private def normalizeCost(memory: Double, network: Double, io: Double): Double = {
    memory * MEMORY_TO_CPU_RATIO + network * NETWORK_TO_CPU_RATIO + io * IO_TO_CPU_RATIO
  }

  def equals(other: RelOptCost): Boolean = {
    (this eq other) ||
      other.isInstanceOf[BatchExecCost] &&
        (this.rowCount == other.asInstanceOf[BatchExecCost].rowCount) &&
        (this.cpu == other.asInstanceOf[BatchExecCost].cpu) &&
        (this.io == other.asInstanceOf[BatchExecCost].io) &&
        (this.network == other.asInstanceOf[BatchExecCost].network) &&
        (this.memory == other.asInstanceOf[BatchExecCost].memory)
  }

  def isEqWithEpsilon(other: RelOptCost): Boolean = {
    if (!other.isInstanceOf[BatchExecCost]) {
      return false
    }
    val that: BatchExecCost = other.asInstanceOf[BatchExecCost]
    (this eq that) ||
      ((Math.abs(this.rowCount - that.rowCount) < RelOptUtil.EPSILON) &&
        (Math.abs(this.cpu - that.cpu) < RelOptUtil.EPSILON) &&
        (Math.abs(this.io - that.io) < RelOptUtil.EPSILON) &&
        (Math.abs(this.network - that.network) < RelOptUtil.EPSILON) &&
        (Math.abs(this.memory - that.memory) < RelOptUtil.EPSILON))
  }

  def minus(other: RelOptCost): RelOptCost = {
    if (this eq BatchExecCost.Infinity) {
      return this
    }
    val that: BatchExecCost = other.asInstanceOf[BatchExecCost]
    new BatchExecCost(
      this.rowCount - that.rowCount,
      this.cpu - that.cpu,
      this.io - that.io,
      this.network - that.network,
      this.memory - that.memory)
  }

  def multiplyBy(factor: Double): RelOptCost = {
    if (this eq BatchExecCost.Infinity) {
      return this
    }
    new BatchExecCost(
      rowCount * factor,
      cpu * factor,
      io * factor,
      network * factor,
      memory * factor)
  }

  def divideBy(cost: RelOptCost): Double = {
    val that: BatchExecCost = cost.asInstanceOf[BatchExecCost]
    var d: Double = 1
    var n: Double = 0
    if ((this.rowCount != 0) && !this.rowCount.isInfinite &&
      (that.rowCount != 0) && !that.rowCount.isInfinite) {
      d *= this.rowCount / that.rowCount
      n += 1
    }
    if ((this.cpu != 0) && !this.cpu.isInfinite && (that.cpu != 0) && !that.cpu.isInfinite) {
      d *= this.cpu / that.cpu
      n += 1
    }
    if ((this.io != 0) && !this.io.isInfinite && (that.io != 0) && !that.io.isInfinite) {
      d *= this.io / that.io
      n += 1
    }
    if ((this.network != 0) && !this.network.isInfinite &&
      (that.network != 0) && !that.network.isInfinite) {
      d *= this.network / that.network
      n += 1
    }
    if ((this.memory != 0) && !this.memory.isInfinite &&
      (that.memory != 0) && !that.memory.isInfinite) {
      d *= this.memory / that.memory
      n += 1
    }
    if (n == 0) {
      return 1.0
    }
    Math.pow(d, 1 / n)
  }

  def plus(other: RelOptCost): RelOptCost = {
    val that: BatchExecCost = other.asInstanceOf[BatchExecCost]
    if ((this eq BatchExecCost.Infinity) || (that eq BatchExecCost.Infinity)) {
      return BatchExecCost.Infinity
    }
    new BatchExecCost(
      this.rowCount + that.rowCount,
      this.cpu + that.cpu,
      this.io + that.io,
      this.network + that.network,
      this.memory + that.memory)
  }

}

object BatchExecCost {

  private[flink] val Infinity = new BatchExecCost(
    Double.PositiveInfinity,
    Double.PositiveInfinity,
    Double.PositiveInfinity,
    Double.PositiveInfinity,
    Double.PositiveInfinity) {
    override def toString: String = "{inf}"
  }

  private[flink] val Huge = new BatchExecCost(
    Double.MaxValue, Double.MaxValue, Double.MaxValue, Double.MaxValue, Double.MaxValue) {
    override def toString: String = "{huge}"
  }

  private[flink] val Zero = new BatchExecCost(0.0, 0.0, 0.0, 0.0, 0.0) {
    override def toString: String = "{0}"
  }

  private[flink] val Tiny = new BatchExecCost(1.0, 1.0, 0.0, 0.0, 0.0) {
    override def toString = "{tiny}"
  }

  val FACTORY: BatchExecCostFactory = new BatchExecCostFactory

  val BASE_CPU_COST: Int = 1

  /**
    * Hash cpu cost per field (for now we don't distinguish between fields of different types)
    * involves the cost of the following operations:
    * compute hash value, probe hash table, walk hash chain and compare with each element,
    * add to the end of hash chain if no match found
    */
  val HASH_CPU_COST: Int = 8 * BASE_CPU_COST

  /**
    * Serialize and deserialize cost, note it's a very expensive operation
    */
  val SERIALIZE_DESERIALIZE_CPU_COST: Int = 160 * BASE_CPU_COST

  /**
    * Cpu cost of random partition.
    */
  val RANDOM_CPU_COST: Int = 1 * BASE_CPU_COST

  /**
    * Cpu cost of singleton exchange
    */
  val SINGLETON_CPU_COST: Int = 1 * BASE_CPU_COST

  /**
    * Cpu cost of comparing one field with another (ignoring data types for now)
    */
  val COMPARE_CPU_COST: Int = 4 * BASE_CPU_COST

  /**
    * Cpu cost for a function evaluation
    */
  val FUNC_CPU_COST: Int = 12 * BASE_CPU_COST

  /**
    * Cpu cost of range partition, including cost of sample and cost of assign range index
    */
  val RANGE_PARTITION_CPU_COST: Int = 12 * BASE_CPU_COST

  /**
    * Default data size of a worker to process.
    * Note: only used in estimates cost of RelNode.
    * It is irrelevant to decides the parallelism of operators.
    */
  val SQL_DEFAULT_PARALLELISM_WORKER_PROCESS_SIZE = 1024 * 1024 * 1024

}
