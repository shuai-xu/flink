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

package org.apache.flink.table.plan.nodes.physical.batch

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.sampling.IntermediateSampleData
import org.apache.flink.api.java.typeutils.{TupleTypeInfo, TypeExtractor}
import org.apache.flink.runtime.io.network.DataExchangeMode
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, PartitionTransformation, StreamTransformation, TwoInputTransformation}
import org.apache.flink.streaming.runtime.partitioner._
import org.apache.flink.table.api.types.{BaseRowType, DataTypes}
import org.apache.flink.table.api.{BatchTableEnvironment, TableConfig, TableEnvironment}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.{CodeGeneratorContext, GeneratedSorter, ProjectionCodeGenerator, SortCodeGenerator}
import org.apache.flink.table.dataformat.{BaseRow, BinaryRow, GenericRow}
import org.apache.flink.table.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.plan.batch.BatchExecRelVisitor
import org.apache.flink.table.plan.nodes.common.CommonExchange
import org.apache.flink.table.plan.util.SortUtil
import org.apache.flink.table.runtime.operator.range._
import org.apache.flink.table.typeutils.{BaseRowTypeInfo, TypeUtils}
import org.apache.flink.table.util.{BinaryHashPartitioner, FlinkRelOptUtil}

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.{RelDistribution, RelNode, RelWriter}

import scala.collection.JavaConverters._

/**
  * This RelNode represents a change of partitioning of the input elements.
  *
  * This does not create a physical transformation If its relDistribution' type is not range,
  * it only affects how upstream operations are connected to downstream operations.
  *
  * But if the type is range, this relNode will create some physical transformation because it
  * need calculate the data distribution. To calculate the data distribution, the received stream
  * will split in two process stream. For the first process stream, it will go through the sample
  * and statistics to calculate the data distribution in pipeline mode. For the second process
  * stream will been bocked. After the first process stream has been calculated successfully,
  * then the two process stream  will union together. Thus it can partitioner the record based
  * the data distribution. Then The RelNode will create the following transformations.
  *
  * +---------------------------------------------------------------------------------------------+
  * |                                                                                             |
  * | +-----------------------------+                                                             |
  * | | StreamTransformation        | ------------------------------------>                       |
  * | +-----------------------------+                                     |                       |
  * |                 |                                                   |                       |
  * |                 |                                                   |                       |
  * |                 |forward & PIPELINED                                |                       |
  * |                \|/                                                  |                       |
  * | +--------------------------------------------+                      |                       |
  * | | OneInputTransformation[LocalSample, n]     |                      |                       |
  * | +--------------------------------------------+                      |                       |
  * |                      |                                              |forward & BATCH        |
  * |                      |forward & PIPELINED                           |                       |
  * |                     \|/                                             |                       |
  * | +--------------------------------------------------+                |                       |
  * | |OneInputTransformation[SampleAndHistogram, 1]     |                |                       |
  * | +--------------------------------------------------+                |                       |
  * |                        |                                            |                       |
  * |                        |broadcast & PIPELINED                       |                       |
  * |                        |                                            |                       |
  * |                       \|/                                          \|/                      |
  * | +---------------------------------------------------+------------------------------+        |
  * | |               TwoInputTransformation[AssignRangeId, n]                           |        |
  * | +----------------------------------------------------+-----------------------------+        |
  * |                                       |                                                     |
  * |                                       |custom & PIPELINED                                   |
  * |                                      \|/                                                    |
  * | +---------------------------------------------------+------------------------------+        |
  * | |               OneInputTransformation[RemoveRangeId, n]                           |        |
  * | +----------------------------------------------------+-----------------------------+        |
  * |                                                                                             |
  * +---------------------------------------------------------------------------------------------+
  */
class BatchExecExchange(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    relNode: RelNode,
    relDistribution: RelDistribution)
  extends CommonExchange(cluster, traitSet, relNode, relDistribution)
  with RowBatchExecRel {

  private val SIP_NAME = "RangePartition: LocalSample"
  private val SIC_NAME = "RangePartition: SampleAndHistogram"
  private val ARI_NAME = "RangePartition: PreparePartition"
  private val PR_NAME = "RangePartition: Partition"
  private val TOTAL_SAMPLE_SIZE = 655360
  private val TOTAL_RANGES_NUM = 65536
  // TODO reuse PartitionTransformation
  // currently, an Exchange' input transformation will be reused if it is reusable,
  // and different PartitionTransformation objects will be created which have same input.
  // cache input transformation to reuse
  private var reusedInput: Option[StreamTransformation[BaseRow]] = None
  // cache sampleAndHistogram transformation to reuse when distribution is RANGE
  private var reusedSampleAndHistogram: Option[StreamTransformation[Array[Array[AnyRef]]]] = None
  // the required exchange mode for reusable ExchangeBatchExec
  // if it's None, use value from getDataExchangeMode
  private var requiredExchangeMode: Option[DataExchangeMode] = None

  override def copy(
      traitSet: RelTraitSet,
      newInput: RelNode,
      newDistribution: RelDistribution): BatchExecExchange = {
    val exchange = new BatchExecExchange(
      cluster,
      traitSet,
      newInput,
      newDistribution)
    exchange.requiredExchangeMode = requiredExchangeMode
    exchange
  }

  override def isBarrierNode: Boolean = {
    val tableConfig = FlinkRelOptUtil.getTableConfig(this)
    val exchangeMode = getDataExchangeModeForDeadlockBreakup(tableConfig)
    if (exchangeMode eq DataExchangeMode.BATCH) {
      return true
    }
    distribution.getType match {
      case RelDistribution.Type.RANGE_DISTRIBUTED => true
      case _ => false
    }
  }

  override def accept[R](visitor: BatchExecRelVisitor[R]): R = visitor.visit(this)

  override def explainTerms(pw: RelWriter): RelWriter =
    super.explainTerms(pw)
      .itemIf("exchange_mode", requiredExchangeMode.orNull,
        requiredExchangeMode.contains(DataExchangeMode.BATCH))

  def setRequiredDataExchangeMode(exchangeMode: DataExchangeMode): Unit = {
    require(exchangeMode != null)
    requiredExchangeMode = Some(exchangeMode)
  }

  private[flink] def getDataExchangeModeForDeadlockBreakup(
      tableConfig: TableConfig): DataExchangeMode = {
    requiredExchangeMode match {
      case Some(mode) if mode eq DataExchangeMode.BATCH => mode
      case _ => getDataExchangeModeForExternalShuffle(tableConfig)
    }
  }

  private def getDataExchangeModeForExternalShuffle(tableConfig: TableConfig): DataExchangeMode = {
    if (tableConfig.enableBatchExternalShuffle) {
      DataExchangeMode.BATCH
    } else {
      DataExchangeMode.PIPELINED
    }
  }

  /**
    * Currently, PartitionTransformation wont been reused,
    * its input transformation will been reused if this is reusable.
    */
  override def translateToPlan(tableEnv: BatchTableEnvironment): StreamTransformation[BaseRow] = {
    translateToPlanInternal(tableEnv)
  }

  /**
    * Internal method, translates the [[BatchExecRel]] node into a Batch operator.
    *
    * @param tableEnv The [[BatchTableEnvironment]] of the translated Table.
    */
  def translateToPlanInternal(
      tableEnv: BatchTableEnvironment): StreamTransformation[BaseRow] = {
    val input = reusedInput match {
      case Some(transformation) => transformation
      case None =>
        val input = getInput.asInstanceOf[RowBatchExecRel].translateToPlan(tableEnv)
        reusedInput = Some(input)
        input
    }

    val exchangeMode = getDataExchangeModeForDeadlockBreakup(tableEnv.getConfig)

    val inputType = input.getOutputType.asInstanceOf[BaseRowTypeInfo[_]]
    val outputRowType = FlinkTypeFactory.toInternalBaseRowTypeInfo(getRowType, classOf[BinaryRow])

    relDistribution.getType match {
      case RelDistribution.Type.ANY =>
        val transformation = new PartitionTransformation(
          input,
          null, // Let StreamGraph choose specific partitioner
          exchangeMode)
        transformation.setOutputType(outputRowType.asInstanceOf[BaseRowTypeInfo[BaseRow]])
        transformation

      case RelDistribution.Type.SINGLETON =>
        val transformation = new PartitionTransformation(
          input,
          new GlobalPartitioner[BaseRow],
          exchangeMode)
        transformation.setOutputType(outputRowType.asInstanceOf[BaseRowTypeInfo[BaseRow]])
        transformation

      case RelDistribution.Type.RANGE_DISTRIBUTED =>
        getRangePartitionPlan(inputType, tableEnv, input)

      case RelDistribution.Type.RANDOM_DISTRIBUTED =>
        val transformation = new PartitionTransformation(
          input,
          new RebalancePartitioner[BaseRow],
          exchangeMode)
        transformation.setOutputType(outputRowType.asInstanceOf[BaseRowTypeInfo[BaseRow]])
        transformation

      case RelDistribution.Type.BROADCAST_DISTRIBUTED =>
        val transformation = new PartitionTransformation(
          input,
          new BroadcastPartitioner[BaseRow],
          exchangeMode)
        transformation.setOutputType(outputRowType.asInstanceOf[BaseRowTypeInfo[BaseRow]])
        transformation

      case _ => // hash shuffle
        // TODO Eliminate duplicate keys
        val keys = relDistribution.getKeys.asScala
        val partitioner = new BinaryHashPartitioner(
          inputType.asInstanceOf[BaseRowTypeInfo[_ <: BaseRow]],
          keys.map(_.intValue()).toArray)
        val transformation = new PartitionTransformation(
          input,
          partitioner,
          exchangeMode)
        transformation.setOutputType(outputRowType.asInstanceOf[BaseRowTypeInfo[BaseRow]])
        transformation
    }
  }

  /**
    * The RelNode with range-partition distribution will create the following transformations.
    *
    * ------------------------- BATCH --------------------------> [B, m] -->...
    * /                                                            /
    * [A, n] --> [LocalSample, n] --> [SampleAndHistogram, 1] --BROADCAST-<
    * \                                                            \
    * ------------------------- BATCH --------------------------> [C, o] -->...
    *
    * Transformations of LocalSample and SampleAndHistogram can be reused.
    * The streams except the sample and histogram process stream will been blocked,
    * so the the sample and histogram process stream does not care about requiredExchangeMode.
    */
  protected def getRangePartitionPlan(
      inputType: BaseRowTypeInfo[_],
      tableEnvironment: TableEnvironment,
      input: StreamTransformation[BaseRow]): StreamTransformation[BaseRow] = {
    val tableEnv = tableEnvironment.asInstanceOf[BatchTableEnvironment]
    val fieldCollations = relDistribution.asInstanceOf[FlinkRelDistribution].getFieldCollations.get

    val (keys, orders, nullsIsLast) = SortUtil.getKeysAndOrders(fieldCollations.asScala)
    val types = inputType.getFieldTypes

    val reservedResSpec = resource.getReservedResourceSpec
    val preferResSpec = resource.getPreferResourceSpec

    val sampleAndHistogram = reusedSampleAndHistogram match {
      case Some(transformation) => transformation
      case None =>
        // 1. Fixed size sample in each partitions.
        val localSampleOutRowType = DataTypes.internal(
          new BaseRowTypeInfo(classOf[GenericRow], keys.map(types(_)): _ *))
            .asInstanceOf[BaseRowType]

        val localSampleProjection = ProjectionCodeGenerator.generateProjection(
          CodeGeneratorContext(tableEnv.getConfig),
          "LocalSample",
          DataTypes.internal(inputType).asInstanceOf[BaseRowType],
          localSampleOutRowType,
          keys,
          reusedOutRecord = false)

        val isdType = TypeExtractor.getForClass(classOf[IntermediateSampleData[BaseRow]])
        val localSample = new OneInputTransformation(
          input,
          SIP_NAME,
          new LocalSampleOperator(localSampleProjection, TOTAL_SAMPLE_SIZE),
          isdType,
          input.getParallelism)
        localSample.setResources(reservedResSpec, preferResSpec)

        // 2. Fixed size sample in a single coordinator
        // and use sampled data to build range boundaries.
        val sampleType = DataTypes.internal(
          new BaseRowTypeInfo(classOf[BinaryRow], keys.map(types(_)): _*))
            .asInstanceOf[BaseRowType]
        val ctx = CodeGeneratorContext(tableEnv.getConfig)
        val copyToBinaryRow = ProjectionCodeGenerator.generateProjection(
          ctx,
          "CopyToBinaryRow",
          localSampleOutRowType,
          sampleType,
          localSampleOutRowType.getFieldTypes.indices.toArray,
          reusedOutRecord = false)
        val boundariesType = TypeExtractor.getForClass(classOf[Array[Array[AnyRef]]])
        val newKeyIndexes = keys.indices.toArray
        val (comparators, serializers) = TypeUtils.flattenComparatorAndSerializer(
          keys.length, newKeyIndexes, orders, keys.map(types(_)))
        val generator = new SortCodeGenerator(
          newKeyIndexes,
          sampleType.getFieldTypes,
          comparators,
          orders,
          nullsIsLast)
        val sampleAndHistogram = new OneInputTransformation(
          localSample,
          SIC_NAME,
          new SampleAndHistogramOperator(
            TOTAL_SAMPLE_SIZE,
            copyToBinaryRow,
            GeneratedSorter(
              generator.generateNormalizedKeyComputer("SampleAndHistogramComputer"),
              generator.generateRecordComparator("SampleAndHistogramComparator"),
              serializers, comparators),
            new KeyExtractor(
              newKeyIndexes,
              orders,
              keys.map((i) => DataTypes.internal(types(i))),
              comparators),
            TOTAL_RANGES_NUM),
          boundariesType,
          1)
        sampleAndHistogram.setResources(reservedResSpec, preferResSpec)
        reusedSampleAndHistogram = Some(sampleAndHistogram)
        sampleAndHistogram
    }

    // 3. Add broadcast shuffle
    val broadcast = new PartitionTransformation(
      sampleAndHistogram,
      new BroadcastPartitioner[Array[Array[AnyRef]]],
      getDataExchangeModeForExternalShuffle(tableEnv.getConfig))

    // 4. add batch dataExchange
    val batchExchange = new PartitionTransformation(
      input,
      new ForwardPartitioner[BaseRow],
      DataExchangeMode.BATCH)

    // 4. Take range boundaries as broadcast input and take the tuple of partition id and
    // record as output.
    // TwoInputTransformation, it must be binaryRow.
    val binaryType = new BaseRowTypeInfo(classOf[BinaryRow], types: _*)
    val preparePartition = {
      val (comparators, _) = TypeUtils.flattenComparatorAndSerializer(
        binaryType.getArity, keys, orders, binaryType.getFieldTypes)
      new TwoInputTransformation(
        broadcast,
        batchExchange,
        ARI_NAME,
        new AssignRangeIndexOperator(new KeyExtractor(keys, orders,
          binaryType.getFieldTypes.map(DataTypes.internal), comparators)),
        new TupleTypeInfo(BasicTypeInfo.INT_TYPE_INFO, binaryType),
        input.getParallelism)
    }
    preparePartition.setResources(reservedResSpec, preferResSpec)

    // 5. Add shuffle according range partition.
    val rangePartition = new PartitionTransformation(
      preparePartition,
      new CustomPartitionerWrapper(
        new IdPartitioner(TOTAL_RANGES_NUM),
        new FirstIntFieldKeyExtractor),
      getDataExchangeModeForExternalShuffle(tableEnv.getConfig))

    // 6. Remove the partition id.
    val removeIdTransformation = new OneInputTransformation(
      rangePartition,
      PR_NAME,
      new RemoveRangeIndexOperator(),
      binaryType.asInstanceOf[BaseRowTypeInfo[BaseRow]],
      resultPartitionCount)
    removeIdTransformation.setResources(reservedResSpec, preferResSpec)
    removeIdTransformation
  }
}

