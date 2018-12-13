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

import org.apache.flink.annotation.{InterfaceStability, VisibleForTesting}
import org.apache.flink.api.common.{ExecutionMode, JobExecutionResult}
import org.apache.flink.api.common.accumulators.SerializedListAccumulator
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.graph.{StreamGraph, StreamGraphGenerator}
import org.apache.flink.streaming.api.transformations.StreamTransformation
import org.apache.flink.table.api.types.{BaseRowType, DataType, DataTypes}
import org.apache.flink.table.calcite.{FlinkRelBuilder, FlinkTypeFactory}
import org.apache.flink.table.catalog.{ExternalCatalog, ReadableCatalog}
import org.apache.flink.table.dataformat.BinaryRow
import org.apache.flink.table.descriptors.{BatchTableDescriptor, ConnectorDescriptor}
import org.apache.flink.table.expressions.{Expression, TimeAttribute}
import org.apache.flink.table.plan.`trait`.FlinkRelDistributionTraitDef
import org.apache.flink.table.plan.cost.{FlinkBatchCost, FlinkCostFactory}
import org.apache.flink.table.plan.logical.SinkNode
import org.apache.flink.table.plan.nodes.calcite.Sink
import org.apache.flink.table.plan.nodes.physical.batch.{BatchExecRel, BatchExecSink}
import org.apache.flink.table.plan.optimize.BatchOptimizeContext
import org.apache.flink.table.plan.schema._
import org.apache.flink.table.plan.stats.FlinkStatistic
import org.apache.flink.table.plan.util.{DeadlockBreakupProcessor, SameRelObjectShuttle, SubplanReuseContext, SubplanReuseShuttle, FlinkRelOptUtil}
import org.apache.flink.table.plan.{RelNodeBlock, RelNodeBlockPlanBuilder}
import org.apache.flink.table.resource.batch.RunningUnitKeeper
import org.apache.flink.table.runtime.AbstractStreamOperatorWithMetrics
import org.apache.flink.table.sinks._
import org.apache.flink.table.sources.{BatchTableSource, _}
import org.apache.flink.table.util._
import org.apache.flink.table.util.PlanUtil._
import org.apache.flink.util.{AbstractID, Preconditions}

import org.apache.calcite.plan.{Context, ConventionTraitDef, RelOptPlanner}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelCollationTraitDef, RelNode}
import org.apache.calcite.sql.SqlExplainLevel
import org.apache.calcite.sql2rel.SqlToRelConverter
import org.apache.calcite.sql2rel.SqlToRelConverter.Config

import _root_.java.util.{ArrayList => JArrayList, List => JList, Map => JMap, Set => JSet}

import _root_.scala.collection.JavaConversions._
import _root_.scala.collection.JavaConverters._
import _root_.scala.collection.mutable
import _root_.scala.collection.mutable.ArrayBuffer

/**
 *  A session to construct between [[Table]] and [[DataStream]], its main function is:
  *
  *  1. Get a table from [[DataStream]], or through registering a [[TableSource]];
  *  2. Transform current already construct table to [[DataStream]];
  *  3. Add [[TableSink]] to the [[Table]].
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
  override protected def createRelBuilder: FlinkRelBuilder = FlinkRelBuilder.create(
    frameworkConfig,
    config,
    getTypeFactory,
    Array(
      ConventionTraitDef.INSTANCE,
      FlinkRelDistributionTraitDef.INSTANCE,
      RelCollationTraitDef.INSTANCE),
    catalogManager
  )

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
  override protected def getFlinkCostFactory: FlinkCostFactory = FlinkBatchCost.FACTORY

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
    if (config.getSubsectionOptimization) {
      compile()
    }

    if (transformations.isEmpty) {
      throw new TableException("No table sinks have been created yet. " +
          "A program needs at least one sink that consumes data. ")
    }
    val result = executeInternal(transformations, Option.apply(jobName))
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
    val stream = translate(table, sink)
    val res = executeInternal(ArrayBuffer(stream.getTransformation), jobName)
    val accResult: JArrayList[Array[Byte]] = res.getAccumulatorResult(id)
    SerializedListAccumulator.deserializeList(accResult, typeSerializer).asScala
  }

  /**
    * Generate a [[StreamGraph]] from this table environment, this will also clear the internal
    * info of [[RunningUnitKeeper]], sink LogicalNodes and [[StreamTransformation]]s.
    * @return A [[StreamGraph]] describing the whole job.
    */
  def generateStreamGraph(): StreamGraph = {
    val streamGraph = generateStreamGraph(transformations, None)
    sinkNodes.clear()
    streamGraph
  }

  private def generateStreamGraph(streamingTransformations: ArrayBuffer[StreamTransformation[_]],
    jobName: Option[String]): StreamGraph = {
    mergeParameters()
    val context = StreamGraphGenerator.Context.buildBatchProperties(streamEnv)
    if (getConfig.isAllDataExchangeModeBatch) {
      context.getExecutionConfig.setExecutionMode(ExecutionMode.BATCH)
    } else {
      context.getExecutionConfig.setExecutionMode(ExecutionMode.PIPELINED)
    }

    ruKeeper.setScheduleConfig(context)
    jobName match {
      case Some(jn) => context.setJobName(jn)
      case None => context.setJobName(DEFAULT_JOB_NAME)
    }
    val streamGraph = StreamGraphGenerator.generate(context, streamingTransformations)
    setQueryPlan()

    setupOperatorMetricCollect()
    ruKeeper.clear()

    streamingTransformations.clear()
    streamGraph
  }

  private def executeInternal(streamingTransformations: ArrayBuffer[StreamTransformation[_]],
      jobName: Option[String]): JobExecutionResult = {
    val streamGraph = generateStreamGraph(streamingTransformations, jobName)

    val result = streamEnv.execute(streamGraph)
    dumpPlanWithMetricsIfNeed(streamGraph, result)
    result
  }

  /**
    * Set up operator metric collect to be true.
    */
  @VisibleForTesting
  private[flink] def setupOperatorMetricCollect(): Unit = {
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

  override private[table] def writeToSink[T](
      table: Table,
      sink: TableSink[T],
      sinkName: String): Unit = {
    val sinkNode = SinkNode(table.logicalPlan, sink, sinkName)
    if (config.getSubsectionOptimization) {
      sinkNodes += sinkNode
    } else {
      val stream = translate(table, sink, sinkName)
      transformations.add(stream.getTransformation)
    }
  }

  private[flink] override def compile(): Seq[RelNodeBlock] = {
    if (config.getSubsectionOptimization) {
      if (sinkNodes.isEmpty) {
        throw new TableException("No table sinks have been created yet. " +
            "A program needs at least one sink that consumes data. ")
      }
      // build RelNodeBlock plan
      val blockPlan = RelNodeBlockPlanBuilder.buildRelNodeBlockPlan(sinkNodes, this)

      // translates recursively RelNodeBlock into BoundedStream
      blockPlan.foreach {
        sinkBlock =>
          val boundedStream: DataStream[_] = translateRelNodeBlock(sinkBlock)
          sinkBlock.outputNode match {
            case _: Sink =>
              transformations.add(boundedStream.getTransformation)
            case _ => throw new TableException("SinkNode required here")
          }
      }
      blockPlan
    } else {
      Seq.empty
    }
  }

  private def translateRelNodeBlock(block: RelNodeBlock): DataStream[_] = {
    block.children.foreach {
      child =>
        if (child.getNewOutputNode.isEmpty) {
          translateRelNodeBlock(child)
        }
    }

    val originTree = block.getPlan
    val optimizedTree = optimize(originTree)
    addQueryPlan(originTree, optimizedTree)

    val boundedStream: DataStream[_] = optimizedTree match {
      case n: BatchExecSink[_] =>
        val outputType = n.sink.getOutputType
        translate(n, outputType)
      case _ =>
        val outputType = FlinkTypeFactory.toDataType(originTree.getRowType)
        val stream = translate(optimizedTree, outputType)
        val name = createUniqueTableName()
        registerIntermediateBoundedStreamInternal(
          // It is not a SinkNode, so it will be referenced by other RelNodeBlock.
          // When registering a data collection, we should send a correct type.
          // If no this correct type, it will be converted from Flink's fieldTypes,
          // while STRING_TYPE_INFO will lose the length of varchar.
          originTree.getRowType,
          name,
          stream)
        val newTable = scan(name)
        block.setNewOutputNode(newTable.getRelNode)
        block.setOutputTableName(name)
        stream
    }

    block.setOptimizedPlan(optimizedTree)
    boundedStream
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
      boundedStream: DataStream[T]): Unit = {

    val boundedStreamTable = new IntermediateBoundedStreamTable[T](
      rowType,
      boundedStream,
      rowType.getFieldList.map(_.getIndex).toArray[Int],
      rowType.getFieldList.map(_.getName).toArray[String])
    registerTableInternal(name, boundedStreamTable)
  }

  /**
    * Translates a [[Table]] into a [[DataStream]], emit the [[DataStream]] into a [[TableSink]]
    * of a specified type and generated a new [[DataStream]].
    *
    * The transformation involves optimizing the relational expression tree as defined by
    * Table API calls and / or SQL queries and generating corresponding [[DataStream]]
    * operators.
    *
    * @param table The root node of the relational expression tree.
    * @param sink  The [[TableSink]] to emit the [[Table]] into.
    * @return The generated [[DataStream]] operators after emit the [[DataStream]] translated by
    *         [[Table]] into a [[TableSink]].
    */
  protected def translate[A](table: Table, sink: TableSink[A]): DataStream[_] = {
    val sinkName = createUniqueTableName()
    translate(table, sink, sinkName)
  }

  private def translate[A](
      table: Table,
      sink: TableSink[A],
      sinkName: String)
    : DataStream[_] = {
    val sinkNode = SinkNode(table.logicalPlan, sink, sinkName)
    val sinkTable = new Table(this, sinkNode)
    val originTree = sinkTable.getRelNode
    val optimizedTree = optimize(originTree)
    addQueryPlan(originTree, optimizedTree)
    optimizedTree match {
      case batchExecSink: BatchExecSink[A] =>
        translate(batchExecSink, sink.getOutputType)
      case _ =>
        throw new TableException(
          s"Cannot generate BoundedStream due to an invalid logical plan. " +
            "This is a bug and should not happen. Please file an issue.")

    }
  }

  /**
   * Translates a logical [[RelNode]] into a [[DataStream]].
   * Converts to target type if necessary.
   *
   * @param logicalPlan The root node of the relational expression tree.
   * @param resultType  The [[DataType]] of the resulting [[DataStream]].
   * @return The [[DataStream]] that corresponds to the translated [[Table]].
   */
  protected def translate[OUT](
      logicalPlan: RelNode,
      resultType: DataType): DataStream[OUT] = {
    TableEnvironment.validateType(resultType)

    logicalPlan match {
      case node: BatchExecRel[OUT] =>
        ruKeeper.buildRUs(node)
        ruKeeper.calculateRelResource(node)
        val plan = node.translateToPlan(this)
        new DataStream(streamEnv, plan)
      case _ =>
        throw new TableException("Cannot generate BoundedStream due to an invalid logical plan. " +
            "This is a bug and should not happen. Please file an issue.")
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
      val context = new SubplanReuseContext(config.isTableSourceReuseDisabled, diffObjPlan)
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
    if (fieldNames == null) throw new TableException("fieldNames must not be null.")
    if (fieldTypes == null) throw new TableException("fieldTypes must not be null.")
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
    val boundedStream = translate(
      optimizedPlan,
      new BaseRowType(classOf[BinaryRow], fieldTypes: _*))
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
    val visitedBlocks = mutable.Set[RelNodeBlock]()

    def visitBlock(block: RelNodeBlock, isSinkBlock: Boolean): Unit = {
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

  override def registerCatalog(name: String, catalog: ReadableCatalog): Unit = {
    registerCatalogInternal(name, catalog, false)
  }
}
