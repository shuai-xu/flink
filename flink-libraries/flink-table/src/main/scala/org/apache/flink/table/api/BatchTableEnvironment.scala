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

package org.apache.flink.table.api

import _root_.java.util.{ArrayList => JArrayList, List => JList, Map => JMap, Set => JSet}

import org.apache.calcite.plan.{Context, ConventionTraitDef, RelOptPlanner}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelCollationTraitDef, RelNode}
import org.apache.calcite.sql.SqlExplainLevel
import org.apache.calcite.sql2rel.SqlToRelConverter
import org.apache.calcite.sql2rel.SqlToRelConverter.Config
import org.apache.flink.annotation.{InterfaceStability, VisibleForTesting}
import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.accumulators.SerializedListAccumulator
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.graph.{StreamGraph, StreamGraphGenerator}
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation}
import org.apache.flink.table.api.types.{BaseRowType, DataType, DataTypes}
import org.apache.flink.table.calcite.{FlinkRelBuilder, FlinkTypeFactory}
import org.apache.flink.table.catalog.ExternalCatalog
import org.apache.flink.table.codegen.CodeGeneratorContext
import org.apache.flink.table.dataformat.{BaseRow, BinaryRow}
import org.apache.flink.table.descriptors.{BatchTableDescriptor, ConnectorDescriptor}
import org.apache.flink.table.expressions.{Expression, TimeAttribute}
import org.apache.flink.table.plan.`trait`.FlinkRelDistributionTraitDef
import org.apache.flink.table.plan.cost.{BatchExecCost, FlinkCostFactory}
import org.apache.flink.table.plan.logical.SinkNode
import org.apache.flink.table.plan.nodes.physical.batch.RowBatchExecRel
import org.apache.flink.table.plan.optimize.BatchOptimizeContext
import org.apache.flink.table.plan.schema._
import org.apache.flink.table.plan.stats.FlinkStatistic
import org.apache.flink.table.plan.util.{DeadlockBreakupProcessor, SameRelObjectShuttle, SubplanReuseContext, SubplanReuseShuttle}
import org.apache.flink.table.plan.{LogicalNodeBlock, LogicalNodeBlockPlanBuilder}
import org.apache.flink.table.resource.batch.RunningUnitKeeper
import org.apache.flink.table.runtime.operator.AbstractStreamOperatorWithMetrics
import org.apache.flink.table.sinks._
import org.apache.flink.table.sources.{BatchTableSource, _}
import org.apache.flink.table.typeutils.{BaseRowTypeInfo, TypeUtils}
import org.apache.flink.table.util.{ExecResourceUtil, FlinkRelOptUtil, PlanUtil, RowConverters}
import org.apache.flink.table.util.PlanUtil._
import org.apache.flink.table.util.{ExecResourceUtil, FlinkRelOptUtil, PlanUtil}
import org.apache.flink.util.{AbstractID, Preconditions}

import _root_.scala.collection.JavaConversions._
import _root_.scala.collection.JavaConverters._
import _root_.scala.collection.mutable
import _root_.scala.collection.mutable.ArrayBuffer

/**
 *
 * @param config The [[TableConfig]] of this [[BatchTableEnvironment]].
 */
@InterfaceStability.Evolving
class BatchTableEnvironment(
    private[flink] val streamEnv: StreamExecutionEnvironment,
    config: TableConfig)
    extends TableEnvironment(config) {

  private val DEFAULT_JOB_NAME = "Flink Exec Job"
  private val ruKeeper = new RunningUnitKeeper(this)

  /** Fetch [[RunningUnitKeeper]] bond with this table env. */
  private [table] def getRUKeeper(): RunningUnitKeeper = ruKeeper

  // the builder for Calcite RelNodes, Calcite's representation of a relational expression tree.
  override protected lazy val relBuilder: FlinkRelBuilder = FlinkRelBuilder.create(
    frameworkConfig, config, getTypeFactory, Array(
      ConventionTraitDef.INSTANCE,
      FlinkRelDistributionTraitDef.INSTANCE,
      RelCollationTraitDef.INSTANCE))

  // prefix for unique table names.
  override private[flink] val tableNamePrefix = "_DataStreamTable_"

  private val transformations = new ArrayBuffer[StreamTransformation[_]]

  private val queryPlans = new ArrayBuffer[String]()

  /**
   * `expand` is set as false, and each sub-query becomes a [[org.apache.calcite.rex.RexSubQuery]].
   */
  override protected def getSqlToRelConverterConfig: Config =
    SqlToRelConverter.configBuilder()
        .withTrimUnusedFields(false)
        .withConvertTableAccess(false)
        .withExpand(false)
        .withInSubQueryThreshold(Int.MaxValue)
        .build()

  /**
   * Returns specific FlinkCostFactory of this Environment.
   */
  override protected def getFlinkCostFactory: FlinkCostFactory = BatchExecCost.FACTORY

  /**
   * Triggers the program execution.
   */
  override def execute(): JobExecutionResult = {
    execute(DEFAULT_JOB_NAME)
  }

  /**
    * Triggers the program execution with specific job name.
    * @param jobName name for the job
    */
  override def execute(jobName: String): JobExecutionResult = {
    mergeParameters()
    if (config.getSubsectionOptimization) {
      compile()
    }

    if (transformations.isEmpty) {
      throw new TableException("No table sinks have been created yet. " +
          "A program needs at least one sink that consumes data. ")
    }
    val result = executeInternal(transformations, Option.apply(jobName))
    transformations.clear()
    sinkNodes.clear()
    result
  }

  private[flink] override def collect[T](
      table: Table,
      sink: CollectTableSink[T],
      jobName: Option[String]): Seq[T] = {
    val outType = sink.getOutputType
    val typeSerializer = DataTypes.toTypeInfo(outType).createSerializer(streamEnv.getConfig)
        .asInstanceOf[TypeSerializer[T]]
    val id = new AbstractID().toString
    sink.init(typeSerializer, id)
    val result = translate[T](table, outType, sink)
    val execSink = emitBoundedStreamSink(sink, result)
    val res = executeInternal(ArrayBuffer(execSink.getTransformation), jobName)
    val accResult: JArrayList[Array[Byte]] = res.getAccumulatorResult(id)
    SerializedListAccumulator.deserializeList(accResult, typeSerializer).asScala
  }

  /**
    * Generate a [[StreamGraph]] from this table environment, this will also clear the internal
    * info of [[RunningUnitKeeper]], sink LogicalNodes and [[StreamTransformation]]s.
    * @return A [[StreamGraph]] describing the whole job.
    */
  def generateStreamGraph(): StreamGraph = {
    mergeParameters()
    val context = StreamGraphGenerator.Context.buildBatchProperties(streamEnv)
    ruKeeper.setScheduleConfig(context)
    val streamGraph = StreamGraphGenerator.generate(context, transformations)

    setQueryPlan()

    setupOperatorMetricCollect()
    ruKeeper.clear()
    sinkNodes.clear()
    transformations.clear()
    streamGraph
  }

  private def executeInternal(streamingTransformations: ArrayBuffer[StreamTransformation[_]],
      jobName: Option[String]): JobExecutionResult = {
    val context = StreamGraphGenerator.Context.buildBatchProperties(streamEnv)
    ruKeeper.setScheduleConfig(context)
    jobName match {
      case Some(jn) => context.setJobName(jn)
      case None => context.setJobName(DEFAULT_JOB_NAME)
    }
    val streamGraph = StreamGraphGenerator.generate(context, streamingTransformations)

    setQueryPlan()

    setupOperatorMetricCollect()
    val result = streamEnv.execute(streamGraph)
    dumpPlanWithMetricsIfNeed(streamGraph, result)
    ruKeeper.clear()
    result
  }

  /**
    * Set up operator metric collect to be true.
    */
  @VisibleForTesting
  def setupOperatorMetricCollect(): Unit = {
    if (streamEnv != null && streamEnv.getConfig != null && config.getOperatorMetricCollect) {
      val parameters = new Configuration()
      Option(streamEnv.getConfig.getGlobalJobParameters).foreach(gb =>
        gb.toMap.foreach(kv => parameters.setString(kv._1, kv._2))
      )
      parameters.setString(
        AbstractStreamOperatorWithMetrics.METRICS_CONF_KEY,
        AbstractStreamOperatorWithMetrics.METRICS_CONF_VALUE)
      streamEnv.getConfig.setGlobalJobParameters(parameters)
    }
  }

  /**
    * Set up the [[queryPlans]] concatenated string to [[streamEnv]]. This will clear
    * the [[queryPlans]].
    */
  private def setQueryPlan(): Unit = {
    val queryPlan = queryPlans.mkString("\n")
    require(queryPlan.nonEmpty)
    queryPlans.clear()

    // TODO use a more reasonable way to store queryPlan
    val parameters = new Configuration()
    if (streamEnv.getConfig.getGlobalJobParameters != null) {
      streamEnv.getConfig.getGlobalJobParameters.toMap.asScala.foreach {
        kv => parameters.setString(kv._1, kv._2)
      }
    }
    parameters.setString(TableEnvironment.QUERY_PLAN_KEY, queryPlan)
    streamEnv.getConfig.setGlobalJobParameters(parameters)
  }

  private[flink] override def writeToSink[T](
      table: Table,
      sink: TableSink[T],
      sinkName: String): Unit = {
    // Check the query configuration to be a batch one.

    if (config.getSubsectionOptimization) {
      sinkNodes += SinkNode(table.logicalPlan, sink)
    } else {
      sink match {
        case batchTableSink: BatchTableSink[T] =>
          val outputType = sink.getOutputType
          val result = translate[T](table, outputType, sink)
          transformations.add(emitBoundedStreamSink(batchTableSink, result).getTransformation)
        case compatibleTableSink: BatchCompatibleStreamTableSink[_] =>
          val result = translate[T](
            table,
            compatibleTableSink.getOutputType,
            sink,
            withChangeFlag = true)
          transformations.add(emitBoundedStreamSink(compatibleTableSink, result).getTransformation)
        case _ =>
          throw new TableException("BatchTableSink or CompatibleStreamTableSink" +
            " can be registered in BatchTableEnvironment")
      }
    }
  }

  private[flink] override def compile(): Seq[LogicalNodeBlock] = {
    if (config.getSubsectionOptimization) {
      if (sinkNodes.isEmpty) {
        throw new TableException("No table sinks have been created yet. " +
            "A program needs at least one sink that consumes data. ")
      }
      // build LogicalNodeBlock plan
      val blockPlan = LogicalNodeBlockPlanBuilder.buildLogicalNodeBlockPlan(sinkNodes, this)

      // translates recursively LogicalNodeBlock into BoundedStream
      blockPlan.foreach {
        sinkBlock =>
          val result: DataStream[_] = translateLogicalNodeBlock(sinkBlock)
          sinkBlock.outputNode match {
            case sinkNode: SinkNode =>
              val boundedStream = emitBoundedStreamSink(
                sinkNode.sink.asInstanceOf[TableSink[Any]],
                result.asInstanceOf[DataStream[Any]])
              transformations.add(boundedStream.getTransformation)
            case _ => throw new TableException("SinkNode required here")
          }
      }
      blockPlan
    } else {
      Seq.empty
    }
  }

  private def translateLogicalNodeBlock(block: LogicalNodeBlock): DataStream[_] = {
    block.children.foreach {
      child =>
        if (child.getNewOutputNode.isEmpty) {
          translateLogicalNodeBlock(child)
        }
    }

    val blockLogicalPlan = block.getLogicalPlan
    val logicalPlan = blockLogicalPlan match {
      case n: SinkNode => n.child // ignore sink node
      case o => o
    }

    val table = new Table(this, logicalPlan)

    val relNode = table.getRelNode
    val batchExecPlan = optimize(relNode)
    addQueryPlan(relNode, batchExecPlan)

    val boundedStream: DataStream[_] = blockLogicalPlan match {
      case n: SinkNode =>
        n.sink match {
          case compatibleTableSink: BatchCompatibleStreamTableSink[_] =>
            translate(batchExecPlan, relNode.getRowType, withChangeFlag = true,
              compatibleTableSink.getOutputType, compatibleTableSink)
          case _ =>
            val outputType = n.sink.getOutputType
            translate(batchExecPlan, relNode.getRowType, withChangeFlag = false, outputType, n.sink)
        }
      case _ =>
        val outputType = DataTypes.createRowType(table.getSchema.getTypes: _*)
        translate(batchExecPlan, relNode.getRowType, withChangeFlag = false, outputType, null)
    }

    if (!blockLogicalPlan.isInstanceOf[SinkNode]) {
      val name = createUniqueTableName()
      registerIntermediateBoundedStreamInternal(
        // It is not a SinkNode, so it will be referenced by other blockLogicalPlan.
        // When registering a data collection, we should send a correct type.
        // If no this correct type, it will be converted from Flink's fieldTypes,
        // while STRING_TYPE_INFO will lose the length of varchar.
        relNode.getRowType,
        name,
        boundedStream,
        table.getSchema)
      val newTable = scan(name)
      block.setNewOutputNode(newTable.logicalPlan)
      block.setOutputTableName(name)
    }

    block.setOptimizedPlan(batchExecPlan)
    boundedStream
  }

  private def emitBoundedStreamSink[T](
      sink: TableSink[T], boundedStream: DataStream[T]): DataStreamSink[_] = {
    sink match {
      case sinkBatch: BatchTableSink[T] =>
        val boundedSink = sinkBatch.emitBoundedStream(boundedStream, config, streamEnv.getConfig)
        assignDefaultResourceAndParallelism(boundedStream, boundedSink)
        boundedSink
      case compatible: BatchCompatibleStreamTableSink[T] =>
        val boundedSink = compatible.emitBoundedStream(
          boundedStream.asInstanceOf[DataStream[T]])
        assignDefaultResourceAndParallelism(boundedStream, boundedSink)
        boundedSink
      case _ => throw new TableException("BatchTableSink or " +
          "CompatibleStreamTableSink required to emit batch exec Table")
    }
  }

  private def assignDefaultResourceAndParallelism(
      boundedStream: DataStream[_], boundedSink: DataStreamSink[_]) {
    val sinkTransformation = boundedSink.getTransformation
    val streamTransformation = boundedStream.getTransformation
    val preferredResources = sinkTransformation.getPreferredResources
    if (preferredResources == null) {
      val heapMem = ExecResourceUtil.getSinkMem(getConfig)
      val resource = ExecResourceUtil.getResourceSpec(getConfig, heapMem)
      sinkTransformation.setResources(resource, resource)
    }
    if (!sinkTransformation.isParallelismLocked) {
      val configSinkParallelism = ExecResourceUtil.getSinkParallelism(getConfig)
      if (configSinkParallelism > 0) {
        sinkTransformation.setParallelism(configSinkParallelism)
      } else if (streamTransformation.getParallelism > 0) {
        sinkTransformation.setParallelism(streamTransformation.getParallelism)
      }
    }
  }

  /**
   * Registers a [[DataStream]] as a table under a given name in the [[TableEnvironment]]'s
   * catalog.
   *
   * @param name     The name under which the table is registered in the catalog.
   * @param boundedStream The [[DataStream]] to register as table in the catalog.
   * @tparam T the type of the [[DataStream]].
   */
  protected def registerBoundedStreamInternal[T](
      name: String, boundedStream: DataStream[T]): Unit = {
    val (fieldNames, fieldIdxs) = getFieldInfo(
      DataTypes.of(boundedStream.getTransformation.getOutputType))
    val boundedStreamTable = new DataStreamTable[T](boundedStream, fieldIdxs, fieldNames)
    registerTableInternal(name, boundedStreamTable)
  }

  /**
   * Registers a [[DataStream]] as a table under a given name in the [[TableEnvironment]]'s
   * catalog.
   *
   * @param name           The name under which the table is registered in the catalog.
   * @param boundedStream       The [[DataStream]] to register as table in the catalog.
   * @param fieldNullables The field isNullables attributes of boundedStream.
   * @tparam T the type of the [[DataStream]].
   */
  protected def registerBoundedStreamInternal[T](
      name: String,
      boundedStream: DataStream[T],
      fieldNullables: Array[Boolean]): Unit = {
    val dataType =
      DataTypes.of(boundedStream.getTransformation.getOutputType)
    val (fieldNames, fieldIndexes) = getFieldInfo(dataType)
    val fieldTypes = TableEnvironment.getFieldTypes(dataType)
    val relDataType = getTypeFactory.buildRelDataType(fieldNames, fieldTypes, fieldNullables)
    val boundedStreamTable = new IntermediateBoundedStreamTable[T](
      relDataType, boundedStream, fieldIndexes, fieldNames)
    registerTableInternal(name, boundedStreamTable)
  }

  /**
   * Registers a [[DataStream]] as a table under a given name with field names as specified by
   * field expressions in the [[TableEnvironment]]'s catalog.
   *
   * @param name     The name under which the table is registered in the catalog.
   * @param boundedStream The [[DataStream]] to register as table in the catalog.
   * @param fields   The field expressions to define the field names of the table.
   * @tparam T The type of the [[DataStream]].
   */
  protected def registerBoundedStreamInternal[T](
      name: String, boundedStream: DataStream[T], fields: Array[Expression]): Unit = {

    if (fields.exists(_.isInstanceOf[TimeAttribute])) {
      throw new ValidationException(
        ".rowtime and .proctime time indicators are not allowed in a batch exec environment.")
    }

    val dataType = DataTypes.of(boundedStream.getTransformation.getOutputType)
    val (fieldNames, fieldIndexes) = getFieldInfo[T](dataType, fields)
    val boundedStreamTable = new DataStreamTable[T](boundedStream, fieldIndexes, fieldNames)
    registerTableInternal(name, boundedStreamTable)
  }

  /**
   * Registers a [[DataStream]] as a table under a given name with field names as specified by
   * field expressions in the [[TableEnvironment]]'s catalog.
   *
   * @param name           The name under which the table is registered in the catalog.
   * @param boundedStream       The [[DataStream]] to register as table in the catalog.
   * @param fields         The field expressions to define the field names of the table.
   * @param fieldNullables The field isNullables attributes of boundedStream.
   * @tparam T The type of the [[DataStream]].
   */
  protected def registerBoundedStreamInternal[T](
      name: String,
      boundedStream: DataStream[T],
      fields: Array[Expression],
      fieldNullables: Array[Boolean]): Unit = {

    if (fields.exists(_.isInstanceOf[TimeAttribute])) {
      throw new ValidationException(
        ".rowtime and .proctime time indicators are not allowed in a batch exec environment.")
    }
    val dataType = DataTypes.of(boundedStream.getTransformation.getOutputType)
    val (fieldNames, fieldIndexes) = getFieldInfo(dataType, fields)
    val physicalFieldTypes = TableEnvironment.getFieldTypes(dataType)
    val fieldTypes = fieldIndexes.map(physicalFieldTypes.apply(_))
    val relDataType = getTypeFactory.buildRelDataType(fieldNames, fieldTypes, fieldNullables)
    val boundedStreamTable = new IntermediateBoundedStreamTable[T](
      relDataType, boundedStream, fieldIndexes, fieldNames)
    registerTableInternal(name, boundedStreamTable)
  }

  private def registerIntermediateBoundedStreamInternal[T](
      rowType: RelDataType,
      name: String,
      boundedStream: DataStream[T],
      tableSchema: TableSchema): Unit = {

    val boundedStreamTable = new IntermediateBoundedStreamTable[T](
      rowType,
      boundedStream,
      tableSchema.getColumnNames.map(tableSchema.columnNameToIndex.get(_).get),
      tableSchema.getColumnNames)
    registerTableInternal(name, boundedStreamTable)
  }

  /**
   * Translates a [[Table]] into a [[DataStream]].
   *
   * The transformation involves optimizing the relational expression tree as defined by
   * Table API calls and / or SQL queries and generating corresponding [[DataStream]]
   * operators.
   *
   * @param table      The root node of the relational expression tree.
   * @param resultType The [[DataType]] of the resulting [[DataStream]].
   * @tparam A The type of the resulting [[DataStream]].
   * @return The [[DataStream]] that corresponds to the translated [[Table]].
   */
  protected def translate[A](
      table: Table,
      resultType: DataType,
      sink: TableSink[_],
      withChangeFlag: Boolean = false)
    : DataStream[A] = {
    val relNode = table.getRelNode
    val boundedStreamPlan = optimize(relNode)
    addQueryPlan(relNode, boundedStreamPlan)
    translate(boundedStreamPlan, relNode.getRowType, withChangeFlag, resultType, sink)
  }

  /**
   * Translates a logical [[RelNode]] into a [[DataStream]].
   * Converts to target type if necessary.
   *
   * @param logicalPlan The root node of the relational expression tree.
   * @param logicalType The row type of the result. Since the logicalPlan can lose the
   *                    field naming during optimization we pass the row type separately.
   * @param resultType  The [[DataType]] of the resulting [[DataStream]].
   * @tparam OUT The type of the resulting [[DataStream]].
   * @return The [[DataStream]] that corresponds to the translated [[Table]].
   */
  protected def translate[OUT](
      logicalPlan: RelNode,
      logicalType: RelDataType,
      withChangeFlag: Boolean,
      resultType: DataType,
      sink: TableSink[_]): DataStream[OUT] = {
    TableEnvironment.validateType(resultType)

    logicalPlan match {
      case node: RowBatchExecRel =>
        ruKeeper.buildRUs(node)
        ruKeeper.calculateRelResource(node)
        val plan = node.translateToPlan(this)

        val parTransformation = if (sink != null) {
          createPartitionTransformation(sink, plan)
        } else {
          plan
        }
        val convertTransformation =
          getConversionMapper[BaseRow, OUT](
            parTransformation,
            parTransformation.getOutputType.asInstanceOf[BaseRowTypeInfo[_]],
            logicalType,
            "BoundedStreamSinkConversion",
            withChangeFlag,
            resultType)
        new DataStream(streamEnv, convertTransformation)
      case _ =>
        throw TableException("Cannot generate BoundedStream due to an invalid logical plan. " +
            "This is a bug and should not happen. Please file an issue.")
    }
  }

  /**
    * If the input' outputType is incompatible with the external type, here need create a final
    * converter that maps the internal row type to external type.
    *
    * @param physicalTypeInfo the input of the sink
    * @param relType          the input relDataType with correct field names
    * @param name             name of the map operator. Must not be unique but has to be a
    *                         valid Java class identifier.
    * @param withChangeFlag   Set to true to emit records with change flags.
    * @param resultType       The [[DataType]] of the resulting [[DataStream]].
    */
  private def getConversionMapper[IN, OUT](
    input: StreamTransformation[IN],
    physicalTypeInfo: BaseRowTypeInfo[_],
    relType: RelDataType,
    name: String,
    withChangeFlag: Boolean,
    resultType: DataType): StreamTransformation[OUT] = {

    val (converterOperator, outputTypeInfo) = RowConverters.generateRowConverterOperator[IN, OUT](
      config,
      CodeGeneratorContext(config, supportReference = true),
      physicalTypeInfo,
      relType,
      name,
      None,
      withChangeFlag,
      resultType)
    converterOperator match {
      case None => input.asInstanceOf[StreamTransformation[OUT]]
      case Some(operator) =>
        val transformation = new OneInputTransformation(
          input,
          s"SinkConversion to ${TypeUtils.getExternalClassForType(resultType).getSimpleName}",
          operator,
          outputTypeInfo,
          input.getParallelism)
        val defaultResource = ExecResourceUtil.getDefaultResourceSpec(getConfig)
        transformation.setParallelismLocked(true)
        transformation.setResources(defaultResource, defaultResource)
        transformation
    }
  }

  /**
    * Adds original relNode plan and optimized relNode plan to `queryPlans`.
    */
  private def addQueryPlan(originalNode: RelNode, optimizedNode: RelNode): Unit = {
    val queryPlan =
      s"""
         |== Abstract Syntax Tree ==
         |${FlinkRelOptUtil.toString(originalNode)}
         |== Optimized Logical Plan ==
         |${FlinkRelOptUtil.toString(optimizedNode, detailLevel = SqlExplainLevel.ALL_ATTRIBUTES)}
      """.stripMargin
    queryPlans += queryPlan
  }

  /**
   * Generates the optimized [[RelNode]] tree from the original relational node tree.
   *
   * @param relNode The original [[RelNode]] tree
   * @return The optimized [[RelNode]] tree
   */
  private[flink] def optimize(relNode: RelNode): RelNode = {
    val programs = config.getCalciteConfig.getBatchPrograms
    Preconditions.checkNotNull(programs)

    val optimizedPlan = programs.optimize(relNode, new BatchOptimizeContext {
      override def getContext: Context = getFrameworkConfig.getContext

      override def getRelOptPlanner: RelOptPlanner = getPlanner
    })

    // FIXME refactor
    // Rewrite same rel object to different rel objects.
    val diffObjPlan = optimizedPlan.accept(new SameRelObjectShuttle())
    // reuse sub-plan if enabled
    val reusedPlan = if (config.getSubPlanReuse) {
      val context = new SubplanReuseContext(diffObjPlan, config)
      diffObjPlan.accept(new SubplanReuseShuttle(context))
    } else {
      diffObjPlan
    }
    // breakup deadlock
    val postOptimizedPlan = new DeadlockBreakupProcessor().process(reusedPlan)
    dumpOptimizedPlanIfNeed(postOptimizedPlan)
    postOptimizedPlan
  }

  /**
    * Registers an external [[TableSource]] in this [[TableEnvironment]]'s catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * @param name        The name under which the [[TableSource]] is registered.
    * @param tableSource The [[TableSource]] to register.
    * @param uniqueKeys  The unique keys of table.
    *                    null means miss this information;
    *                    empty set means does not exist unique key of the table;
    *                    non-empty set means set of the unique keys.
    */
  def registerTableSource(
      name: String,
      tableSource: TableSource,
      uniqueKeys: JSet[JSet[String]]): Unit = {
    registerTableSource(name, tableSource, uniqueKeys, null)
  }

  /**
   * Registers an external [[TableSource]] in this [[TableEnvironment]]'s catalog.
   * Registered tables can be referenced in SQL queries.
   *
   * @param name        The name under which the [[TableSource]] is registered.
   * @param tableSource The [[TableSource]] to register.
   * @param uniqueKeys  The unique keys of table.
   *                    null means miss this information;
   *                    empty set means does not exist unique key of the table;
   *                    non-empty set means set of the unique keys.
   * @param skewInfo    statistics of skewedColNames and skewedColValues.
   */
  def registerTableSource(
      name: String,
      tableSource: TableSource,
      uniqueKeys: JSet[JSet[String]],
      skewInfo: JMap[String, JList[AnyRef]] = null): Unit = {
    checkValidTableName(name)

    val statistic = if (uniqueKeys != null || skewInfo != null) {
      FlinkStatistic.of(uniqueKeys, skewInfo)
    } else {
      FlinkStatistic.UNKNOWN
    }

    registerTableSourceInternal(name, tableSource, statistic)
  }

  /**
    * Registers an internal [[BatchTableSource]] in this [[TableEnvironment]]'s catalog without
    * name checking. Registered tables can be referenced in SQL queries.
    *
    * @param name        The name under which the [[TableSource]] is registered.
    * @param tableSource The [[TableSource]] to register.
    */
  override protected def registerTableSourceInternal(
    name: String,
    tableSource: TableSource,
    statistic: FlinkStatistic)
  : Unit = {

    tableSource match {

      // check for proper batch table source
      case batchTableSource: BatchTableSource[_] =>
        // check if a table (source or sink) is registered
        getTable(name) match {

          // table source and/or sink is registered
          case Some(table: TableSourceSinkTable[_]) => table.tableSourceTable match {

            // wrapper contains source
            case Some(_: TableSourceTable) =>
              throw new TableException(s"Table '$name' already exists. " +
                s"Please choose a different name.")

            // wrapper contains only sink (not source)
            case _ =>
              val enrichedTable = new TableSourceSinkTable(
                Some(new BatchTableSourceTable(batchTableSource, statistic)),
                table.tableSinkTable)
              replaceRegisteredTable(name, enrichedTable)
          }

          // no table is registered
          case _ =>
            val newTable = new TableSourceSinkTable(
              Some(new BatchTableSourceTable(batchTableSource, statistic)),
              None)
            registerTableInternal(name, newTable)
        }

      // check for proper dim table source
      case dimTableSource: DimensionTableSource[_] =>
        // check if a table (source or sink) is registered
        getTable(name) match {

          // dimension table source is already registered
          case Some(_: TableSourceTable) =>
            throw new TableException(s"Table '$name' already exists. " +
              s"Please choose a different name.")

          // no table is registered
          case _ =>
            val dimTableSourceTable = if (dimTableSource.isTemporal) {
              new TemporalDimensionTableSourceTable(dimTableSource, statistic)
            } else {
              new DimensionTableSourceTable(dimTableSource, statistic)
            }
            registerTableInternal(name, dimTableSourceTable)
        }

      // not a batch table source
      case _ =>
        throw new TableException("Only BatchTableSource and DimensionTableSource can be " +
          "registered in BatchTableEnvironment.")
    }
  }

  /**
    * Creates a table source and/or table sink from a descriptor.
    *
    * Descriptors allow for declaring the communication to external systems in an
    * implementation-agnostic way. The classpath is scanned for suitable table factories that match
    * the desired configuration.
    *
    * The following example shows how to read from a connector using a JSON format and
    * registering a table source as "MyTable":
    *
    * {{{
    *
    * tableEnv
    *   .connect(
    *     new ExternalSystemXYZ()
    *       .version("0.11"))
    *   .withFormat(
    *     new Json()
    *       .jsonSchema("{...}")
    *       .failOnMissingField(false))
    *   .withSchema(
    *     new Schema()
    *       .field("user-name", "VARCHAR").from("u_name")
    *       .field("count", "DECIMAL")
    *   .registerSource("MyTable")
    * }}}
    *
    * @param connectorDescriptor connector descriptor describing the external system
    */
  def connect(connectorDescriptor: ConnectorDescriptor): BatchTableDescriptor = {
    new BatchTableDescriptor(this, connectorDescriptor)
  }

  override def registerTableSink(
      name: String,
      fieldNames: Array[String],
      fieldTypes: Array[DataType],
      tableSink: TableSink[_]): Unit = {
    checkValidTableName(name)
    if (fieldNames == null) throw TableException("fieldNames must not be null.")
    if (fieldTypes == null) throw TableException("fieldTypes must not be null.")
    if (fieldNames.length == 0) throw new TableException("fieldNames must not be empty.")
    if (fieldNames.length != fieldTypes.length) {
      throw new TableException("Same number of field names and types required.")
    }

    // configure and register
    val configuredSink = tableSink.configure(fieldNames, fieldTypes)
    registerTableSinkInternal(name, configuredSink)
  }

  /**
    * Registers an external [[TableSink]] with already configured field names and field types in
    * this [[TableEnvironment]]'s catalog.
    * Registered sink tables can be referenced in SQL DML statements.
    *
    * @param name The name under which the [[TableSink]] is registered.
    * @param configuredSink The configured [[TableSink]] to register.
    */
  def registerTableSink(name: String, configuredSink: TableSink[_]): Unit = {
    registerTableSinkInternal(name, configuredSink)
  }

  private def registerTableSinkInternal(name: String, configuredSink: TableSink[_]): Unit = {
    // validate
    checkValidTableName(name)
    if (configuredSink.getFieldNames == null || configuredSink.getFieldTypes == null) {
      throw new TableException("Table sink is not configured.")
    }
    if (configuredSink.getFieldNames.length == 0) {
      throw new TableException("Field names must not be empty.")
    }
    if (configuredSink.getFieldNames.length != configuredSink.getFieldTypes.length) {
      throw new TableException("Same number of field names and types required.")
    }

    // register
    configuredSink match {

      // check for proper batch table sink
      case _: BatchTableSink[_] | _: BatchCompatibleStreamTableSink[_] =>

        // check if a table (source or sink) is registered
        getTable(name) match {

          // table source and/or sink is registered
          case Some(table: TableSourceSinkTable[_]) => table.tableSinkTable match {

            // wrapper contains sink
            case Some(_: TableSinkTable[_]) =>
              throw new TableException(s"Table '$name' already exists. " +
                s"Please choose a different name.")

            // wrapper contains only source (not sink)
            case _ =>
              val enrichedTable = new TableSourceSinkTable(
                table.tableSourceTable,
                Some(new TableSinkTable(configuredSink)))
              replaceRegisteredTable(name, enrichedTable)
          }

          // no table is registered
          case _ =>
            val newTable = new TableSourceSinkTable(
              None,
              Some(new TableSinkTable(configuredSink)))
            registerTableInternal(name, newTable)
        }

      // not a batch table sink
      case _ =>
        throw new TableException("Only BatchTableSink can be registered in BatchTableEnvironment.")
    }
  }

  /**
    * Merge global job parameters and table config parameters,
    * and set the merged result to GlobalJobParameters
    */
  private def mergeParameters(): Unit = {
    if (streamEnv != null && streamEnv.getConfig != null) {
      val parameters = new Configuration()
      if (config != null && config.getParameters != null) {
        parameters.addAll(config.getParameters)
      }

      if (streamEnv.getConfig.getGlobalJobParameters != null) {
        streamEnv.getConfig.getGlobalJobParameters.toMap.asScala.foreach {
          kv => parameters.setString(kv._1, kv._2)
        }
      }

      streamEnv.getConfig.setGlobalJobParameters(parameters)
    }
  }

  /**
   * Returns the AST of the specified Table API and SQL queries and the execution plan to compute
   * the result of the given [[Table]].
   *
   * @param table    The table for which the AST and execution plan will be returned.
   * @param extended Flag to include detailed optimizer estimates.
   */
  private[flink] def explain(table: Table, extended: Boolean): String = {
    val ast = table.getRelNode
    val optimizedPlan = optimize(ast)
    val fieldTypes = ast.getRowType.getFieldList.asScala
      .map(field => FlinkTypeFactory.toInternalType(field.getType))
    val boundedStream = translate[BinaryRow](
      optimizedPlan,
      ast.getRowType,
      withChangeFlag = false,
      new BaseRowType(classOf[BinaryRow], fieldTypes: _*),
      null)
    val streamGraph = StreamGraphGenerator.generate(
      StreamGraphGenerator.Context.buildBatchProperties(streamEnv),
      ArrayBuffer(boundedStream.getTransformation))

    val sqlPlan = PlanUtil.explainPlan(streamGraph)

    s"== Abstract Syntax Tree ==" +
        System.lineSeparator +
        s"${FlinkRelOptUtil.toString(ast)}" +
        System.lineSeparator +
        s"== Optimized Logical Plan ==" +
        System.lineSeparator +
        s"${FlinkRelOptUtil.toString(optimizedPlan)}" +
        System.lineSeparator +
        s"== Physical Execution Plan ==" +
        System.lineSeparator +
        s"$sqlPlan"
  }

  def explain(table: Table): String = explain(table: Table, extended = false)

  /**
   * Explain the whole plan only when subsection optimization is supported, and returns the AST
   * of the specified Table API and SQL queries and the execution plan.
   *
   * @param extended Flag to include detailed optimizer estimates.
   */
  def explain(extended: Boolean = false): String = {
    if (!config.getSubsectionOptimization) {
      throw new TableException("Can not explain due to subsection optimization is not supported, " +
          "please check your TableConfig.")
    }

    val blockPlan = compile()

    val sb = new StringBuilder
    sb.append("== Abstract Syntax Tree ==")
    sb.append(System.lineSeparator)
    sinkNodes.foreach { sink =>
      val table = new Table(this, sink.children.head)
      val ast = table.getRelNode
      sb.append(FlinkRelOptUtil.toString(ast))
      sb.append(System.lineSeparator)
    }

    sb.append("== Optimized Logical Plan ==")
    sb.append(System.lineSeparator)
    val visitedBlocks = mutable.Set[LogicalNodeBlock]()

    def visitBlock(block: LogicalNodeBlock, isSinkBlock: Boolean): Unit = {
      if (!visitedBlocks.contains(block)) {
        block.children.foreach(visitBlock(_, isSinkBlock = false))
        if (isSinkBlock) {
          sb.append("[[Sink]]")
        } else {
          sb.append(s"[[IntermediateTable=${block.getOutputTableName}]]")
        }
        sb.append(System.lineSeparator)
        sb.append(FlinkRelOptUtil.toString(block.getOptimizedPlan))
        sb.append(System.lineSeparator)
        visitedBlocks += block
      }
    }

    blockPlan.foreach(visitBlock(_, isSinkBlock = true))

    val streamGraph = StreamGraphGenerator.generate(
      StreamGraphGenerator.Context.buildBatchProperties(streamEnv), transformations)
    transformations.clear()
    val sqlPlan = PlanUtil.explainPlan(streamGraph)
    sb.append("== Physical Execution Plan ==")
    sb.append(System.lineSeparator)
    sb.append(sqlPlan)
    sb.toString()
  }

  /**
   * Dump stream graph plan with accumulated operator metrics if config enabled.
   *
   * @param streamGraph streamGraph
   * @param jobResult   job result of stream graph
   */
  private[this] def dumpPlanWithMetricsIfNeed(
      streamGraph: StreamGraph,
      jobResult: JobExecutionResult): Unit = {
    val dumpFilePath = config.getDumpFileOfPlanWithMetrics
    if (config.getOperatorMetricCollect && dumpFilePath != null) {
      streamGraph.dumpPlanWithMetrics(dumpFilePath, jobResult)
    }
  }

  /**
   * Dump optimized plan if config enabled.
   *
   * @param optimizedNode optimized plan
   */
  private[this] def dumpOptimizedPlanIfNeed(optimizedNode: RelNode): Unit = {
    val dumpFilePath = config.getDumpFileOfOptimizedPlan
    if (config.getOptimizedPlanCollect && dumpFilePath != null) {
      dumpRelNode(optimizedNode, dumpFilePath)
    }
  }

  override def registerExternalCatalog(name: String, externalCatalog: ExternalCatalog): Unit = {
    registerExternalCatalogInternal(name, externalCatalog, false)
  }
}
