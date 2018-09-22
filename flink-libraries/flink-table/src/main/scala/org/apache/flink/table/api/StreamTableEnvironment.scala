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

import _root_.java.lang.{Boolean => JBool}
import _root_.java.util

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeField, RelDataTypeFieldImpl, RelRecordType}
import org.apache.calcite.rex.RexBuilder
import org.apache.calcite.sql.SqlCall
import org.apache.calcite.sql2rel.SqlToRelConverter
import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSource}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.graph.StreamGraphGenerator
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation}
import org.apache.flink.table.calcite.{FlinkChainContext, FlinkRelBuilder, FlinkTypeFactory}
import org.apache.flink.table.codegen.CodeGeneratorContext
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.errorcode.TableErrors
import org.apache.flink.table.expressions._
import org.apache.flink.table.plan.`trait`._
import org.apache.flink.table.plan.cost.{DataSetCost, FlinkCostFactory, FlinkRelMetadataQuery}
import org.apache.flink.table.plan.logical.{LogicalNode, LogicalRelNode, SinkNode}
import org.apache.flink.table.plan.nodes.calcite.{LogicalLastRow, LogicalWatermarkAssigner}
import org.apache.flink.table.plan.nodes.physical.stream.{StreamExecRel, _}
import org.apache.flink.table.plan.optimize.StreamOptimizeContext
import org.apache.flink.table.plan.schema._
import org.apache.flink.table.plan.stats.FlinkStatistic
import org.apache.flink.table.plan.util.UpdatingPlanChecker
import org.apache.flink.table.plan.{LogicalNodeBlock, LogicalNodeBlockPlanBuilder, MiniBatchHelper}
import org.apache.flink.table.runtime.operator.AbstractProcessStreamOperator
import org.apache.flink.table.sinks._
import org.apache.flink.table.sources._
import org.apache.flink.table.sqlgen.SqlGenVisitor
import org.apache.flink.table.types.{BaseRowType, DataType, DataTypes, InternalType}
import org.apache.flink.table.typeutils.{BaseRowTypeInfo, TypeCheckUtils, TypeUtils}
import org.apache.flink.table.util.{PlanUtil, RelTraitUtil, StateUtil, WatermarkUtils}
import org.apache.flink.util.Preconditions

import _root_.scala.collection.JavaConversions._
import _root_.scala.collection.JavaConverters._
import _root_.scala.collection.mutable
import _root_.scala.collection.mutable.ArrayBuffer


/**
  * The base class for stream TableEnvironments.
  *
  * A TableEnvironment can be used to:
  * - convert [[DataStream]] to a [[Table]]
  * - register a [[DataStream]] as a table in the catalog
  * - register a [[Table]] in the catalog
  * - scan a registered table to obtain a [[Table]]
  * - specify a SQL query on registered tables to obtain a [[Table]]
  * - convert a [[Table]] into a [[DataStream]]
  *
  * @param execEnv The [[StreamExecutionEnvironment]] which is wrapped in this
  *                [[StreamTableEnvironment]].
  * @param config The [[TableConfig]] of this [[StreamTableEnvironment]].
  */
abstract class StreamTableEnvironment(
    val execEnv: StreamExecutionEnvironment,
    config: TableConfig)
  extends TableEnvironment(config) {

  // prefix  for unique table names.
  override private[flink] val tableNamePrefix = "_DataStreamTable_"

  override def queryConfig: StreamQueryConfig = this.streamQueryConfig

  private val DEFAULT_JOB_NAME = "Flink Streaming Job"

  private var streamQueryConfig: StreamQueryConfig = new StreamQueryConfig

  private var isConfigMerged: Boolean = false

  // the builder for Calcite RelNodes, Calcite's representation of a relational expression tree.
  override protected lazy val relBuilder: FlinkRelBuilder = FlinkRelBuilder.create(
    frameworkConfig, config, Array(
      ConventionTraitDef.INSTANCE,
      FlinkRelDistributionTraitDef.INSTANCE,
      UpdateAsRetractionTraitDef.INSTANCE,
      AccModeTraitDef.INSTANCE))

  /**
    * `inSubQueryThreshold` is set to Integer.MAX_VALUE which forces usage of OR
    * for `in` or `not in` clause.
    */
  override protected def getSqlToRelConverterConfig: SqlToRelConverter.Config =
    SqlToRelConverter.configBuilder()
      .withTrimUnusedFields(false)
      .withConvertTableAccess(false)
      .withInSubQueryThreshold(Integer.MAX_VALUE)
      .withExpand(false)
      .build()

  /**
    * Triggers the program execution.
    */
  override def execute(): JobExecutionResult = {
    execute(DEFAULT_JOB_NAME)
  }

  /**
    * Triggers the program execution with jobName.
    * @param jobName The job name.
    */
  override def execute(jobName: String): JobExecutionResult = {
    mergeParameters()
    if (config.getSubsectionOptimization) {
      compile()
    }
    execEnv.execute(jobName)
  }

  def setQueryConfig(queryConfig: StreamQueryConfig): Unit = streamQueryConfig = queryConfig

  /**
    * compile the whole plan into a [[DataStream]].
    */
  def compile(): Seq[LogicalNodeBlock] = {

    mergeParameters()

    //TODO get query config from user
    val streamQueryConfig = queryConfig

    val result = if (config.getSubsectionOptimization) {
      if (sinkNodes.isEmpty) {
        throw new TableException(TableErrors.INST.sqlCompileNoSinkTblError())
      }

      // build LogicalNodeBlock plan
      val blockPlan = LogicalNodeBlockPlanBuilder.buildLogicalNodeBlockPlan(sinkNodes, this)

      // infer updateAsRetraction property for each block
      blockPlan.foreach {
        sinkBlock =>
          val retractionFromSink = sinkBlock.outputNode match {
            case n: SinkNode => n.sink.isInstanceOf[RetractStreamTableSink[_]]
            case _ => false
          }
          sinkBlock.setUpdateAsRetraction(retractionFromSink)
          inferUpdateAsRetraction(sinkBlock, streamQueryConfig, retractionFromSink)
      }

      // propagate updateAsRetraction property to all input blocks
      blockPlan.foreach(propagateUpdateAsRetraction)
      // clear the intermediate result
      blockPlan.foreach(resetIntermediateResult)

      // translates recursively LogicalNodeBlock into DataStream
      blockPlan.foreach {
        sinkBlock =>
          val result: DataStream[_] = translateLogicalNodeBlock(sinkBlock, streamQueryConfig)
          sinkBlock.outputNode match {
            case sinkNode: SinkNode => emitDataStream(
              sinkNode.sink.asInstanceOf[TableSink[Any]],
              result.asInstanceOf[DataStream[Any]])
            case _ => throw new TableException(TableErrors.INST.sqlCompileSinkNodeRequired())
          }
      }
      blockPlan

    } else {
      Seq.empty
    }
    if (queryConfig.isMiniBatchEnabled) {
      if (queryConfig.getMiniBatchTriggerTime <= 0L) {
        throw new RuntimeException(TableErrors.INST.sqlCompileMiniBatchTriggerTimeError())
      }
      MiniBatchHelper.assignTriggerTimeEqually(execEnv, queryConfig.getMiniBatchTriggerTime)
    }
     result
  }

  /**
    * Registers an external [[StreamTableSource]] in this [[TableEnvironment]]'s catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * @param name        The name under which the [[TableSource]] is registered.
    * @param tableSource The [[TableSource]] to register.
    */
  override def registerTableSource(name: String, tableSource: TableSource): Unit = {
    checkValidTableName(name)

    // check if event-time is enabled
    tableSource match {
      case tableSource: TableSource if TableSourceUtil.hasRowtimeAttribute(tableSource) &&
          execEnv.getStreamTimeCharacteristic != TimeCharacteristic.EventTime =>

        throw TableException(
          s"A rowtime attribute requires an EventTime time characteristic in stream environment. " +
            s"But is: ${execEnv.getStreamTimeCharacteristic}")
      case _ => // ok
    }

    tableSource match {
      case streamTableSource: StreamTableSource[_] =>
        registerTableInternal(name,
          new StreamTableSourceTable(streamTableSource))
      case dimensionTableSource: DimensionTableSource[_] =>
        if (dimensionTableSource.isTemporal) {
          registerTableInternal(name, new TemporalDimensionTableSourceTable(dimensionTableSource))
        } else {
          registerTableInternal(name, new DimensionTableSourceTable(dimensionTableSource))
        }
      case _ =>
        throw new TableException("Only StreamTableSource can be registered in " +
            "StreamTableEnvironment")
    }
  }

  /**
    * Registers an external [[TableSink]] with given field names and types in this
    * [[TableEnvironment]]'s catalog.
    * Registered sink tables can be referenced in SQL DML statements.
    *
    * Example:
    *
    * {{{
    *   // create a table sink and its field names and types
    *   val fieldNames: Array[String] = Array("a", "b", "c")
    *   val fieldTypes: Array[InternalType] = Array(STRING, INT, LONG)
    *   val tableSink: StreamTableSink = new YourTableSinkImpl(...)
    *
    *   // register the table sink in the catalog
    *   tableEnv.registerTableSink("output_table", fieldNames, fieldsTypes, tableSink)
    *
    *   // use the registered sink
    *   tableEnv.sqlUpdate("INSERT INTO output_table SELECT a, b, c FROM sourceTable")
    * }}}
    *
    * @param name The name under which the [[TableSink]] is registered.
    * @param fieldNames The field names to register with the [[TableSink]].
    * @param fieldTypes The field types to register with the [[TableSink]].
    * @param tableSink The [[TableSink]] to register.
    */
  def registerTableSink(
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

    tableSink match {
      case streamTableSink@(
        _: AppendStreamTableSink[_] |
        _: UpsertStreamTableSink[_] |
        _: RetractStreamTableSink[_]) =>

        val configuredSink = streamTableSink.configure(fieldNames, fieldTypes)
        registerTableInternal(name, new TableSinkTable(configuredSink))
      case _ =>
        throw new TableException(
          "Only AppendStreamTableSink, UpsertStreamTableSink, and RetractStreamTableSink can be " +
            "registered in StreamTableEnvironment.")
    }
  }

  /**
    * Returns specific FlinkCostFactory of this Environment.
    * Currently we use DataSetCostFactory, and will change this later.
    */
  override protected def getFlinkCostFactory: FlinkCostFactory = DataSetCost.FACTORY

  /**
    * Writes a [[Table]] to a [[TableSink]].
    *
    * Internally, the [[Table]] is translated into a [[DataStream]] and handed over to the
    * [[TableSink]] to write it.
    *
    * @param table The [[Table]] to write.
    * @param sink The [[TableSink]] to write the [[Table]] to.
    * @param queryConfig The configuration for the query to generate.
    * @tparam T The expected type of the [[DataStream]] which represents the [[Table]].
    */
  override private[flink] def writeToSink[T](
      table: Table,
      sink: TableSink[T],
      queryConfig: QueryConfig,
      sinkName: String): Unit = {

    // Check query configuration
    val streamQueryConfig = queryConfig match {
      case streamConfig: StreamQueryConfig => streamConfig
      case _ =>
        throw new TableException("StreamQueryConfig required to configure stream query.")
    }

    if (config.getSubsectionOptimization) {
      sinkNodes += SinkNode(table.logicalPlan, sink)
    } else {
      val (result: DataStream[T], _) =
        translate(table, sink, streamQueryConfig)
      emitDataStream(sink, result)
    }
  }

  override def getSqlText(): String = {
    if (config.getSubsectionOptimization) {
      getSqlText(sinkNodes, new SqlGenVisitor(this))
    } else {
      ""
    }
  }

  private def getSqlText(nodes: Seq[LogicalNode], visitor: SqlGenVisitor): String = {
    val visited = new util.IdentityHashMap[LogicalNode, Boolean]()
    val buffer = new StringBuffer()

    def visitNode(node: LogicalNode): Unit = {
      for(child <- node.children) {
        if (!visited.containsKey(child)) {
          visitNode(child)
          visited.put(child, true)
        }
      }
      val currSql = visitor.visit(node)
      if (currSql._3) {
        buffer.append(currSql._2)
      }
    }

    for(sinkNode <- nodes) {
      if (!visited.containsKey(sinkNode)) {
        visited.put(sinkNode, true)
        visitNode(sinkNode)
      }
    }

    val functionSignatures = visitor.exprVisitor.functionMap.map {
      case (name, (data, Some(comment))) =>
        s"-- $comment\n" +
        s"CREATE FUNCTION $name AS '$data';"
      case (name, (data, _)) =>
        s"CREATE FUNCTION $name AS '$data';"
    }.mkString("\n")

    functionSignatures + "\n" + buffer.toString
  }

  /**
    * Registers a [[DataStream]] as a table under a given name in the [[TableEnvironment]]'s
    * catalog.
    *
    * @param name The name under which the table is registered in the catalog.
    * @param dataStream The [[DataStream]] to register as table in the catalog.
    * @tparam T the type of the [[DataStream]].
    */
  protected def registerDataStreamInternal[T](
    name: String,
    dataStream: DataStream[T]): Unit = {

    val streamType = DataTypes.of(dataStream.getType)
    // get field names and types for all non-replaced fields
    val (fieldNames, fieldIndexes) = getFieldInfo(streamType)

    val dataStreamTable = new DataStreamTable[T](dataStream, false, false, fieldIndexes, fieldNames)
    registerTableInternal(name, dataStreamTable)
  }

  /**
    * Registers a [[DataStream]] with [[BaseRow]] type as a table under a given name with field
    * names as specified by field expressions in the [[TableEnvironment]]'s catalog.
    *
    * @param name The name under which the table is registered in the catalog.
    * @param isAccRetract True if input data contain retraction messages.
    * @param dataStream The [[DataStream]] with [[BaseRow]] type to register as table
    *                   in the catalog.
    * @param fields The field expressions to define the field names of the table.
    * @param monotonicity the monotonicity of each field.
    */
  private def registerBaseRowDataStreamInternal(
      name: String,
      producesUpdates: Boolean,
      isAccRetract: Boolean,
      dataStream: DataStream[BaseRow],
      rowType: RelDataType,
      fields: Array[Expression],
      uniqueKeys: util.Set[_ <: util.Set[String]],
      monotonicity: RelModifiedMonotonicity): Unit = {

    val streamType = DataTypes.of(dataStream.getType)

    if (fields.exists(_.isInstanceOf[RowtimeAttribute])
        && execEnv.getStreamTimeCharacteristic != TimeCharacteristic.EventTime) {
      throw TableException(
        s"A rowtime attribute requires an EventTime time characteristic in stream environment. " +
          s"But is: ${execEnv.getStreamTimeCharacteristic}")
    }


    val (fieldNames, fieldIndexes) = getFieldInfo(streamType, fields)

    // validate and extract time attributes
    val (rowtime, proctime) = validateAndExtractTimeAttributes(streamType, fields)

    // check if event-time is enabled
    if (rowtime.isDefined && execEnv.getStreamTimeCharacteristic != TimeCharacteristic.EventTime) {
      throw TableException(
        s"A rowtime attribute requires an EventTime time characteristic in stream environment. " +
          s"But is: ${execEnv.getStreamTimeCharacteristic}")
    }

    // adjust field indexes and field names
    val indexesWithIndicatorFields = adjustFieldIndexes(fieldIndexes, rowtime, proctime)
    val namesWithIndicatorFields = adjustFieldNames(fieldNames, rowtime, proctime)

    val statistic = FlinkStatistic.of(uniqueKeys, monotonicity)

    val dataStreamTable = new IntermediateDataStreamTable(
      rowType,
      dataStream,
      producesUpdates,
      isAccRetract,
      indexesWithIndicatorFields,
      namesWithIndicatorFields,
      statistic)
    registerTableInternal(name, dataStreamTable)
  }


  /**
    * Registers a dummy [[DataStream]] with row type as a table under a given name with field names
    * as specified by field expressions in the [[TableEnvironment]]'s catalog.
    *
    * @param name The name under which the table is registered in the catalog.
    * @param isAccRetract True if input data contain retraction messages.
    * @param fields The field expressions to define the field names of the table.
    */
  private def registerDummyDataStreamTableInternal(
      name: String,
      produceUpdates: Boolean,
      isAccRetract: Boolean,
      rowType: RelDataType,
      fields: Array[Expression],
      uniqueKeys: util.Set[_ <: util.Set[String]],
      monotonicity: RelModifiedMonotonicity): Unit = {

    val inputType = FlinkTypeFactory.toInternalBaseRowTypeInfo(rowType, classOf[BaseRow])

    // validate and extract time attributes
    val (rowtime, proctime) = validateAndExtractTimeAttributes(DataTypes.of(inputType), fields)
    if (rowtime.isDefined && execEnv.getStreamTimeCharacteristic != TimeCharacteristic.EventTime) {
      throw TableException(
        s"A rowtime attribute requires an EventTime time characteristic in stream environment. " +
          s"But is: ${execEnv.getStreamTimeCharacteristic}")
    }


    val (fieldNames, fieldIndexes) = getFieldInfo(DataTypes.of(inputType), fields)
    // adjust field indexes and field names
    val indexesWithIndicatorFields = adjustFieldIndexes(fieldIndexes, rowtime, proctime)
    val namesWithIndicatorFields = adjustFieldNames(fieldNames, rowtime, proctime)

    // a dummy source with baseRow type
    val dummySource = new DataStreamSource[BaseRow](execEnv, inputType, null, false, "")

    val statistic = FlinkStatistic.of(uniqueKeys, monotonicity)

    val dataStreamTable = new IntermediateDataStreamTable(
      rowType,
      dummySource,
      produceUpdates,
      isAccRetract,
      indexesWithIndicatorFields,
      namesWithIndicatorFields,
      statistic)
    registerTableInternal(name, dataStreamTable)
  }

  /**
    * Registers a [[DataStream]] as a table under a given name with field names as specified by
    * field expressions in the [[TableEnvironment]]'s catalog.
    *
    * @param name The name under which the table is registered in the catalog.
    * @param dataStream The [[DataStream]] to register as table in the catalog.
    * @param fields The field expressions to define the field names of the table.
    * @tparam T The type of the [[DataStream]].
    */
  protected def registerDataStreamInternal[T](
      name: String,
      dataStream: DataStream[T],
      fields: Array[Expression])
    : Unit = {


    val streamType = DataTypes.of(dataStream.getType)

    if (fields.exists(_.isInstanceOf[RowtimeAttribute])
        && execEnv.getStreamTimeCharacteristic != TimeCharacteristic.EventTime) {
      throw TableException(
        s"A rowtime attribute requires an EventTime time characteristic in stream environment. " +
            s"But is: ${execEnv.getStreamTimeCharacteristic}")
    }


    val (fieldNames, fieldIndexes) = getFieldInfo(streamType, fields)

    // validate and extract time attributes
    val (rowtime, proctime) = validateAndExtractTimeAttributes(streamType, fields)

    // check if event-time is enabled
    if (rowtime.isDefined && execEnv.getStreamTimeCharacteristic != TimeCharacteristic.EventTime) {
      throw TableException(
        s"A rowtime attribute requires an EventTime time characteristic in stream environment. " +
            s"But is: ${execEnv.getStreamTimeCharacteristic}")
    }

    // adjust field indexes and field names
    val indexesWithIndicatorFields = adjustFieldIndexes(fieldIndexes, rowtime, proctime)
    val namesWithIndicatorFields = adjustFieldNames(fieldNames, rowtime, proctime)


    val dataStreamTable = new DataStreamTable(
      dataStream,
      false,
      false,
      indexesWithIndicatorFields,
      namesWithIndicatorFields)
    registerTableInternal(name, dataStreamTable)

  }

  /**
    * Checks for at most one rowtime and proctime attribute.
    * Returns the time attributes.
    *
    * @return rowtime attribute and proctime attribute
    */
  private def validateAndExtractTimeAttributes(
    streamType: DataType,
    exprs: Array[Expression])
  : (Option[(Int, String)], Option[(Int, String)]) = {

    val (isRefByPos, fieldTypes) = DataTypes.internal(streamType) match {
      case c: BaseRowType =>
        // determine schema definition mode (by position or by name)
        (isReferenceByPosition(c, exprs), (0 until c.getArity).map(i => c.getTypeAt(i)).toArray)
      case t: InternalType =>
        (false, Array(t))
    }

    var fieldNames: List[String] = Nil
    var rowtime: Option[(Int, String)] = None
    var proctime: Option[(Int, String)] = None

    def checkRowtimeType(t: InternalType): Unit = {
      if (!(t.equals(DataTypes.LONG) || TypeCheckUtils.isTimePoint(t))) {
        throw new TableException(
          s"The rowtime attribute can only replace a field with a valid time type, " +
          s"such as Timestamp or Long. But was: $t")
      }
    }

    def extractRowtime(idx: Int, name: String, origName: Option[String]): Unit = {
      if (rowtime.isDefined) {
        throw new TableException(
          "The rowtime attribute can only be defined once in a table schema.")
      } else {
        // if the fields are referenced by position,
        // it is possible to replace an existing field or append the time attribute at the end
        if (isRefByPos) {
          // aliases are not permitted
          if (origName.isDefined) {
            throw new TableException(
              s"Invalid alias '${origName.get}' because fields are referenced by position.")
          }
          // check type of field that is replaced
          if (idx < fieldTypes.length) {
            checkRowtimeType(fieldTypes(idx))
          }
        }
        // check reference-by-name
        else {
          val aliasOrName = origName.getOrElse(name)
          DataTypes.internal(streamType) match {
            // both alias and reference must have a valid type if they replace a field
            case ct: BaseRowType if ct.getFieldIndex(aliasOrName) >= 0 =>
              val t = ct.getTypeAt(ct.getFieldIndex(aliasOrName))
              checkRowtimeType(t)
            // alias could not be found
            case _ if origName.isDefined =>
              throw new TableException(s"Alias '${origName.get}' must reference an existing field.")
            case _ => // ok
          }
        }

        rowtime = Some(idx, name)
      }
    }

    def extractProctime(idx: Int, name: String): Unit = {
      if (proctime.isDefined) {
          throw new TableException(
            "The proctime attribute can only be defined once in a table schema.")
      } else {
        // if the fields are referenced by position,
        // it is only possible to append the time attribute at the end
        if (isRefByPos) {

          // check that proctime is only appended
          if (idx < fieldTypes.length) {
            throw new TableException(
              "The proctime attribute can only be appended to the table schema and not replace " +
                s"an existing field. Please move '$name' to the end of the schema.")
          }
        }
        // check reference-by-name
        else {
          DataTypes.internal(streamType) match {
            case ct: BaseRowType if
            ct.getFieldIndex(name) < 0 =>
            case ct: BaseRowType if
              ct.getTypeAt(ct.getFieldIndex(name)).equals(DataTypes.PROCTIME_INDICATOR) =>
            // proctime attribute must not replace a field
            case ct: BaseRowType if ct.getFieldIndex(name) >= 0 =>
              throw new TableException(
                s"The proctime attribute '$name' must not replace an existing field.")
            case _ => // ok
          }
        }
        proctime = Some(idx, name)
      }
    }

    exprs.zipWithIndex.foreach {
      case (RowtimeAttribute(UnresolvedFieldReference(name)), idx) =>
        extractRowtime(idx, name, None)

      case (RowtimeAttribute(Alias(UnresolvedFieldReference(origName), name, _)), idx) =>
        extractRowtime(idx, name, Some(origName))

      case (ProctimeAttribute(UnresolvedFieldReference(name)), idx) =>
        extractProctime(idx, name)

      case (UnresolvedFieldReference(name), _) => fieldNames = name :: fieldNames

      case (Alias(UnresolvedFieldReference(_), name, _), _) => fieldNames = name :: fieldNames

      case (e, _) =>
        throw new TableException(s"Time attributes can only be defined on field references or " +
          s"aliases of valid field references. Rowtime attributes can replace existing fields, " +
          s"proctime attributes can not. " +
          s"But was: $e")
    }

    if (rowtime.isDefined && fieldNames.contains(rowtime.get._2)) {
      throw new TableException(
        "The rowtime attribute may not have the same name as an another field.")
    }

    if (proctime.isDefined && fieldNames.contains(proctime.get._2)) {
      throw new TableException(
        "The proctime attribute may not have the same name as an another field.")
    }

    (rowtime, proctime)
  }

  /**
   * Injects markers for time indicator fields into the field indexes.
   *
   * @param fieldIndexes The field indexes into which the time indicators markers are injected.
   * @param rowtime An optional rowtime indicator
   * @param proctime An optional proctime indicator
   * @return An adjusted array of field indexes.
   */
  private def adjustFieldIndexes(
      fieldIndexes: Array[Int],
      rowtime: Option[(Int, String)],
      proctime: Option[(Int, String)]): Array[Int] = {

    // inject rowtime field
    val withRowtime = rowtime match {
      case Some(rt) =>
        fieldIndexes.patch(rt._1, Seq(DataTypes.ROWTIME_STREAM_MARKER), 0)
      case _ =>
        fieldIndexes
    }

    // inject proctime field
    val withProctime = proctime match {
      case Some(pt) =>
        withRowtime.patch(pt._1, Seq(DataTypes.PROCTIME_STREAM_MARKER), 0)
      case _ =>
        withRowtime
    }

    withProctime
  }

  /**
   * Injects names of time indicator fields into the list of field names.
   *
   * @param fieldNames The array of field names into which the time indicator field names are
   *                   injected.
   * @param rowtime An optional rowtime indicator
   * @param proctime An optional proctime indicator
   * @return An adjusted array of field names.
   */
  private def adjustFieldNames(
      fieldNames: Array[String],
      rowtime: Option[(Int, String)],
      proctime: Option[(Int, String)]): Array[String] = {

    // inject rowtime field
    val withRowtime = rowtime match {
      case Some(rt) => fieldNames.patch(rt._1, Seq(rowtime.get._2), 0)
      case _ => fieldNames
    }

    // inject proctime field
    val withProctime = proctime match {
      case Some(pt) => withRowtime.patch(pt._1, Seq(proctime.get._2), 0)
      case _ => withRowtime
    }

    withProctime
  }


  /**
    * Generates the optimized [[RelNode]] tree from the original relational node tree.
    *
    * @param relNode The root node of the relational expression tree.
    * @param updatesAsRetraction True if the sink requests updates as retraction messages.
    * @param queryConfig The configuration for the query to generate.
    * @return The optimized [[RelNode]] tree
    */
  private[flink] def optimize(
      relNode: RelNode,
      updatesAsRetraction: Boolean,
      queryConfig: QueryConfig = streamQueryConfig,
      isSinkBlock: Boolean = true): RelNode = {

    val programs = config.getCalciteConfig.getStreamPrograms
    Preconditions.checkNotNull(programs)

    val flinkChainContext = getPlanner.getContext.asInstanceOf[FlinkChainContext]

    flinkChainContext.load(Contexts.of(queryConfig))

    val optimizeNode = programs.optimize(relNode, new StreamOptimizeContext() {
      override def getContext: Context = flinkChainContext

      override def getRelOptPlanner: RelOptPlanner = getPlanner

      override def getRexBuilder: RexBuilder = getRelBuilder.getRexBuilder

      override def updateAsRetraction(): Boolean = updatesAsRetraction

      override def isSinkNode: Boolean = isSinkBlock
    })
    flinkChainContext.unload(classOf[QueryConfig])

    optimizeNode
  }

  /**
    * Translates a [[Table]] into a [[DataStream]].
    *
    * The transformation involves optimizing the relational expression tree as defined by
    * Table API calls and / or SQL queries and generating corresponding [[DataStream]] operators.
    *
    * @param table The root node of the relational expression tree.
    * @param queryConfig The configuration for the query to generate.
    * @param updatesAsRetraction Set to true to encode updates as retraction messages.
    * @param withChangeFlag Set to true to emit records with change flags.
    * @param resultType The [[DataType]] of the resulting [[DataStream]].
    * @tparam A The type of the resulting [[DataStream]].
    * @return The [[DataStream]] that corresponds to the translated [[Table]].
    */
  protected def translate[A](
      table: Table,
      queryConfig: StreamQueryConfig,
      updatesAsRetraction: Boolean,
      withChangeFlag: Boolean,
      resultType: DataType): DataStream[A] = {

    mergeParameters()

    val relNode = table.getRelNode
    val dataStreamPlan = optimize(relNode, updatesAsRetraction, queryConfig)

    val rowType = getResultType(relNode, dataStreamPlan)

    translate(dataStreamPlan, rowType, queryConfig, withChangeFlag, null, resultType)
  }

  /**
    * Translates a logical [[RelNode]] into a [[DataStream]].
    *
    * @param logicalPlan The root node of the relational expression tree.
    * @param logicalType The row type of the result. Since the logicalPlan can lose the
    *                    field naming during optimization we pass the row type separately.
    * @param queryConfig     The configuration for the query to generate.
    * @param withChangeFlag Set to true to emit records with change flags.
    * @param resultType         The [[DataType]] of the resulting [[DataStream]].
    * @tparam A The type of the resulting [[DataStream]].
    * @return The [[DataStream]] that corresponds to the translated [[Table]].
    */
  protected def translate[A](
      logicalPlan: RelNode,
      logicalType: RelDataType,
      queryConfig: StreamQueryConfig,
      withChangeFlag: Boolean,
      sink: TableSink[_],
      resultType: DataType): DataStream[A] = {

    // if no change flags are requested, verify table is an insert-only (append-only) table.
    if (!withChangeFlag && !UpdatingPlanChecker.isAppendOnly(logicalPlan)) {
      throw new TableException(
        "Table is not an append-only table. " +
          "Use the toRetractStream() in order to handle add and retract messages.")
    }

    // get BaseRow plan
    val translateStream = translateToBaseRow(logicalPlan, queryConfig)

    val parTransformation = if (sink != null) {
      createPartitionTransformation(sink, translateStream)
    } else {
      translateStream
    }

    val rowtimeFields = logicalType
      .getFieldList.asScala
      .filter(f => FlinkTypeFactory.isRowtimeIndicatorType(f.getType))

    // convert the input type for the conversion mapper
    // the input will be changed in the OutputRowtimeProcessFunction later
    val convType = if (rowtimeFields.size > 1) {
      throw new TableException(
        s"Found more than one rowtime field: [${rowtimeFields.map(_.getName).mkString(", ")}] in " +
          s"the table that should be converted to a DataStream.\n" +
          s"Please select the rowtime field that should be used as event-time timestamp for the " +
          s"DataStream by casting all other fields to TIMESTAMP.")
    } else if (rowtimeFields.size == 1) {

      val origRowType = parTransformation.getOutputType.asInstanceOf[BaseRowTypeInfo[_]]
      val convFieldTypes = origRowType.getFieldTypes.map { t =>
        if (FlinkTypeFactory.isRowtimeIndicatorType(t)) {
          SqlTimeTypeInfo.TIMESTAMP
        } else {
          t
        }
      }
      new BaseRowTypeInfo(classOf[BaseRow], convFieldTypes, origRowType.getFieldNames)
    } else {
      parTransformation.getOutputType
    }

    val optionRowTimeField = if (rowtimeFields.isEmpty) None else Some(rowtimeFields.head.getIndex)
    val ctx = CodeGeneratorContext(config, true)
        .setOperatorBaseClass(classOf[AbstractProcessStreamOperator[_]])
    val (converterOperator, outputType) = generateRowConverterOperator[BaseRow, A](
      ctx,
      convType.asInstanceOf[BaseRowTypeInfo[_]],
      logicalType,
      "DataStreamSinkConversion",
      optionRowTimeField,
      withChangeFlag,
      resultType)

    val convertTransformation = converterOperator match {
      case None => parTransformation
      case Some(operator) => new OneInputTransformation(
        parTransformation,
        s"SinkConversion to ${TypeUtils.getExternalClassForType(resultType).getSimpleName}",
        operator,
        outputType,
        parTransformation.getParallelism
      )
    }
    new DataStream(execEnv, convertTransformation).asInstanceOf[DataStream[A]]
  }

    /**
      * Translates a logical [[RelNode]] plan into a [[DataStream]] of type [[BaseRow]].
      *
      * @param logicalPlan The logical plan to translate.
      * @param queryConfig  The configuration for the query to generate.
      * @return The [[DataStream]] of type [[BaseRow]].
      */
    protected def translateToBaseRow(
        logicalPlan: RelNode,
        queryConfig: StreamQueryConfig): StreamTransformation[BaseRow] = {

      logicalPlan match {
        case node: StreamExecRel =>
          node.translateToPlan(this, queryConfig)
        case _ =>
          throw TableException("Cannot generate DataStream due to an invalid logical plan. " +
            "This is a bug and should not happen. Please file an issue.")
      }
    }

  /**
    * Returns the AST of the specified Table API and SQL queries and the execution plan to compute
    * the result of the given [[Table]].
    *
    * @param table The table for which the AST and execution plan will be returned.
    */
  def explain(table: Table): String = {
    val ast = table.getRelNode
    val optimizedPlan = optimize(ast, updatesAsRetraction = false, queryConfig)
    val transformStream = translateToBaseRow(optimizedPlan, queryConfig)

    val streamGraph = StreamGraphGenerator.generate(
      StreamGraphGenerator.Context.buildStreamProperties(execEnv), ArrayBuffer(transformStream))

    val sqlPlan = PlanUtil.explainPlan(streamGraph)

    s"== Abstract Syntax Tree ==" +
        System.lineSeparator +
        s"${RelOptUtil.toString(ast)}" +
        System.lineSeparator +
        s"== Optimized Logical Plan ==" +
        System.lineSeparator +
        s"${RelOptUtil.toString(optimizedPlan)}" +
        System.lineSeparator +
        s"== Physical Execution Plan ==" +
        System.lineSeparator +
        s"$sqlPlan"
  }

  /**
    * Merge global job parameters and table config parameters,
    * and set the merged result to GlobalJobParameters
    */
  private def mergeParameters(): Unit = {
    if (!isConfigMerged && execEnv != null && execEnv.getConfig != null) {
      val parameters = new Configuration()
      if (config != null && config.getParameters != null) {
        parameters.addAll(config.getParameters)
      }

      if (execEnv.getConfig.getGlobalJobParameters != null) {
        execEnv.getConfig.getGlobalJobParameters.toMap.asScala.foreach {
          kv => parameters.setString(kv._1, kv._2)
        }
      }
      parameters.setBoolean(
        StateUtil.STATE_BACKEND_ON_HEAP,
        StateUtil.isHeapState(execEnv.getStateBackend))
      execEnv.getConfig.setGlobalJobParameters(parameters)
      isConfigMerged = true
    }
  }


  /**
    * Explain the whole plan only when subsection optimization is supported, and returns the AST
    * of the specified Table API and SQL queries and the execution plan.
    */
  def explain(): String = {
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
      sb.append(RelOptUtil.toString(ast))
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
        sb.append(RelOptUtil.toString(block.getOptimizedPlan))
        sb.append(RelTraitUtil.toString(block.getOptimizedPlan))
        sb.append(System.lineSeparator)
        visitedBlocks += block
      }
    }
    blockPlan.foreach(visitBlock(_, isSinkBlock = true))

    val sqlPlan = PlanUtil.explainPlan(execEnv.getStreamGraph)
    sb.append("== Physical Execution Plan ==")
    sb.append(System.lineSeparator)
    sb.append(sqlPlan)
    sb.toString()
  }

  /**
    * Mark Expression to RowtimeAttribute or ProctimeAttribute for time indicators
    */
  private def getExprsWithTimeAttribute(table: Table, rowType: RelDataType): Array[Expression] = {

    for ((name, index) <- table.getSchema.getColumnNames.zipWithIndex) yield {
      val relType = rowType.getFieldList.get(index).asInstanceOf[RelDataTypeFieldImpl].getValue
      val relName = rowType.getFieldNames.get(index)
      val expression = UnresolvedFieldReference(relName)

      relType match {
        case _ if FlinkTypeFactory.isProctimeIndicatorType(relType) =>
          new ProctimeAttribute(expression)
        case _ if FlinkTypeFactory.isRowtimeIndicatorType(relType) =>
          new RowtimeAttribute(expression)
        case _ if !relName.equals(name) => Alias(expression, name)
        case _ => expression
      }
    }
  }

  /**
    * Infer UpdateAsRetraction property for each block.
    *
    * @param block The [[LogicalNodeBlock]] instance.
    * @param queryConfig The configuration for the query to generate.
    * @param retractionFromSink Whether the sink need update as retraction messages.
    */
  private def inferUpdateAsRetraction(
      block: LogicalNodeBlock,
      queryConfig: QueryConfig,
      retractionFromSink: Boolean): Unit = {

    block.children.foreach {
      child =>
        if (child.getNewOutputNode.isEmpty) {
          inferUpdateAsRetraction(child, queryConfig, false)
        }
    }

    val optimizedPlan = block.getLogicalPlan match {
      case n: SinkNode =>
        val table = new Table(this, n.child) // ignore sink node
        val optimizedPlan = optimize(table.getRelNode, retractionFromSink, queryConfig, true)
        block.setOptimizedPlan(optimizedPlan)
        optimizedPlan

      case o =>
        val table = new Table(this, o)
        val optimizedPlan = optimize(table.getRelNode, retractionFromSink, queryConfig, false)
        val produceUpdates = !UpdatingPlanChecker.isAppendOnly(optimizedPlan)

        val name = createUniqueTableName()
        val rowType = optimizedPlan.getRowType
        val fieldExpressions = getExprsWithTimeAttribute(table, rowType)

        val uniqueKeys = getUniqueKeys(optimizedPlan)
        val monotonicity = FlinkRelMetadataQuery
          .reuseOrCreate(relBuilder.getCluster.getMetadataQuery)
          .getRelModifiedMonotonicity(optimizedPlan)

        registerDummyDataStreamTableInternal(
          name, produceUpdates, isAccRetract = false, rowType,
          fieldExpressions, uniqueKeys, monotonicity)
        val newTable = scan(name)
        block.setNewOutputNode(newTable.logicalPlan)
        block.setOutputTableName(name)
        block.setOptimizedPlan(optimizedPlan)
        optimizedPlan
    }
  }

  /**
    * Reset the intermediate result including newOutputNode and outputTableName
    *
    * @param block the [[LogicalNodeBlock]] instance.
    */
  private def resetIntermediateResult(block: LogicalNodeBlock): Unit = {
    block.setNewOutputNode(null)
    block.setOutputTableName(null)

    block.children.foreach {
      child => if (child.getNewOutputNode.nonEmpty) {
        resetIntermediateResult(child)
      }
    }
  }

  /**
    * Propagate updateAsRetraction property to all input blocks
    *
    * @param block The [[LogicalNodeBlock]] instance.
    */
  private def propagateUpdateAsRetraction(block: LogicalNodeBlock): Unit = {

    // process current block
    def shipUpdateAsRetraction(rel: RelNode, updateAsRetraction: Boolean): Unit = {
      rel match {
        case scan: StreamExecScan =>
          val retractionTrait = scan.getTraitSet.getTrait(UpdateAsRetractionTraitDef.INSTANCE)
          if (retractionTrait.sendsUpdatesAsRetractions || updateAsRetraction) {
            val tableName = scan.getTable.getQualifiedName.asScala.last
            val retractionBlocks = block.children.filter(_.getOutputTableName eq tableName)
            Preconditions.checkArgument(retractionBlocks.size <= 1)
            if (retractionBlocks.size == 1) {
              retractionBlocks.head.setUpdateAsRetraction(true)
            }
          }
        case ser: StreamExecRel => ser.getInputs.asScala.foreach(e => {
          if (ser.needsUpdatesAsRetraction(e) || (updateAsRetraction && !ser.consumesRetractions)) {
            shipUpdateAsRetraction(e, true)
          } else {
            shipUpdateAsRetraction(e, false)
          }
        })
      }
    }

    shipUpdateAsRetraction(block.getOptimizedPlan, block.isUpdateAsRetraction)
    block.children.foreach(propagateUpdateAsRetraction)
  }

  /**
    * Translates recursively a logical [[RelNode]] in a [[LogicalNodeBlock]] into a [[DataStream]].
    * Converts to target type if the block contains sink node.
    *
    * @param block The [[LogicalNodeBlock]] instance.
    * @return The [[DataStream]] that corresponds to logical plan of current block.
    */
  private def translateLogicalNodeBlock(
      block: LogicalNodeBlock,
      streamQueryConfig: StreamQueryConfig): DataStream[_] = {

    block.children.foreach {
      child => if (child.getNewOutputNode.isEmpty) {
        translateLogicalNodeBlock(child, queryConfig)
      }
    }

    block.getLogicalPlan match {
      case n: SinkNode =>
        val table = new Table(this, n.child) // ignore sink node
        val (dataStream, optimizedPlan) = try {
          translate(table, n.sink, streamQueryConfig)
        } catch {
          case t: TableException =>
            throw TableException(s"Error happens when translating plan for sink '${n.sink}'", t)
        }

        block.setOptimizedPlan(optimizedPlan)
        dataStream

      case o =>
        val table = new Table(this, o)
        val optimizedPlan = optimize(
          table.getRelNode,
          updatesAsRetraction = block.isUpdateAsRetraction,
          streamQueryConfig,
          isSinkBlock = false)
        val translateStream = translateToBaseRow(optimizedPlan, streamQueryConfig)

        val dataStream = new DataStream(execEnv, translateStream)

        val isAccRetract = optimizedPlan.getTraitSet
          .getTrait(AccModeTraitDef.INSTANCE).getAccMode == AccMode.AccRetract
        val producesUpdates = !UpdatingPlanChecker.isAppendOnly(optimizedPlan)
        val name = createUniqueTableName()
        val rowType = optimizedPlan.getRowType
        val fieldExpressions = getExprsWithTimeAttribute(table, rowType)

        val uniqueKeys = getUniqueKeys(optimizedPlan)
        val monotonicity = FlinkRelMetadataQuery
          .reuseOrCreate(relBuilder.getCluster.getMetadataQuery)
          .getRelModifiedMonotonicity(optimizedPlan)

        registerBaseRowDataStreamInternal(
          name, producesUpdates, isAccRetract, dataStream, rowType,
          fieldExpressions, uniqueKeys, monotonicity)
        val newTable = scan(name)
        block.setNewOutputNode(newTable.logicalPlan)
        block.setOutputTableName(name)

        block.setOptimizedPlan(optimizedPlan)
        dataStream
    }
  }

  /**
    * Translates a [[Table]] into a [[DataStream]].
    *
    * The transformation involves optimizing the relational expression tree as defined by
    * Table API calls and / or SQL queries and generating corresponding [[DataStream]] operators.
    *
    * @param table The root node of the relational expression tree.
    * @tparam T The type of the resulting [[DataStream]].
    * @return A Tuple2 include the [[DataStream]] and the optimized plan that corresponds to the
    *         translated [[Table]].
    */
  private def translate[T](
      table: Table,
      sink: TableSink[T],
      streamQueryConfig: StreamQueryConfig): (DataStream[T], RelNode) = {

    sink match {

      case retractSink: RetractStreamTableSink[_] =>
        // retraction sink can always be used
        val outputType = sink.getOutputType
        // translate the Table into a DataStream and provide the type that the TableSink expects.

        val relNode = table.getRelNode
        val dataStreamPlan = optimize(relNode, updatesAsRetraction = true, streamQueryConfig)
        val resultType = getResultType(relNode, dataStreamPlan)
        val dataStream = translate[T](
          dataStreamPlan,
          resultType,
          streamQueryConfig,
          withChangeFlag = true,
          sink,
          outputType)

        (dataStream, dataStreamPlan)

      case upsertSink: UpsertStreamTableSink[_] =>
        // optimize plan
        val optimizedPlan = optimize(
          table.getRelNode, updatesAsRetraction = false, streamQueryConfig)
        // check for append only table
        val isAppendOnlyTable = UpdatingPlanChecker.isAppendOnly(optimizedPlan)
        upsertSink.setIsAppendOnly(isAppendOnlyTable)
        val outputType = sink.getOutputType
        val resultType = getResultType(table.getRelNode, optimizedPlan)
        // translate the Table into a DataStream and provide the type that the TableSink expects.
        val dataStream = translate[T](
          optimizedPlan,
          resultType,
          streamQueryConfig,
          withChangeFlag = true,
          sink,
          outputType)
        (dataStream, optimizedPlan)

      case _: AppendStreamTableSink[_] =>
        // optimize plan
        val optimizedPlan = optimize(
          table.getRelNode, updatesAsRetraction = false, streamQueryConfig)
        // verify table is an insert-only (append-only) table
        if (!UpdatingPlanChecker.isAppendOnly(optimizedPlan)) {
          throw new TableException(
            "AppendStreamTableSink requires that Table has only insert changes.")
        }

        val accMode = optimizedPlan.getTraitSet.getTrait(AccModeTraitDef.INSTANCE).getAccMode
        if (accMode == AccMode.AccRetract) {
          throw new TableException(
            "AppendStreamTableSink can not be used to output retraction messages.")
        }

        val outputType = sink.getOutputType
        val resultType = getResultType(table.getRelNode, optimizedPlan)
        // translate the Table into a DataStream and provide the type that the TableSink expects.
        val dataStream = translate[T](
          optimizedPlan,
          resultType,
          streamQueryConfig,
          withChangeFlag = false,
          sink,
          outputType)
        (dataStream, optimizedPlan)

      case _ =>
        throw new TableException("Stream Tables can only be emitted by AppendStreamTableSink, " +
                                   "RetractStreamTableSink, or UpsertStreamTableSink.")
    }
  }

  private def getResultType(originRelNode: RelNode, optimizedPlan: RelNode): RelRecordType = {
    // zip original field names with optimized field types
    val fieldTypes = originRelNode.getRowType.getFieldList.asScala
      .zip(optimizedPlan.getRowType.getFieldList.asScala)
      // get name of original plan and type of optimized plan
      .map(x => (x._1.getName, x._2.getType))
      // add field indexes
      .zipWithIndex
      // build new field types
      .map(x => new RelDataTypeFieldImpl(x._1._1, x._2, x._1._2))

    // build a record type from list of field types
    new RelRecordType(
      fieldTypes.toList.asInstanceOf[List[RelDataTypeField]].asJava)
  }

  /**
    * emit [[DataStream]].
    *
    * @param sink The [[TableSink]] to emit the [[DataStream]] to.
    * @param dataStream The [[DataStream]] to emit.
    * @tparam T The expected type of the [[DataStream]] which represents the [[TableSink]].
    */
  private def emitDataStream[T](sink: TableSink[T], dataStream: DataStream[T]): Unit = {
    sink match {

      case retractSink: RetractStreamTableSink[_] =>
        // Give the DataStream to the TableSink to emit it.
        retractSink.asInstanceOf[RetractStreamTableSink[Any]]
          .emitDataStream(dataStream.asInstanceOf[DataStream[JTuple2[JBool, Any]]])

      case upsertSink: UpsertStreamTableSink[_] =>
        // Give the DataStream to the TableSink to emit it.
        upsertSink.asInstanceOf[UpsertStreamTableSink[Any]]
          .emitDataStream(dataStream.asInstanceOf[DataStream[JTuple2[JBool, Any]]])

      case appendSink: AppendStreamTableSink[_] =>
        // Give the DataStream to the TableSink to emit it.
        appendSink.asInstanceOf[AppendStreamTableSink[T]].emitDataStream(dataStream)

      case _ =>
        throw new TableException("Stream Tables can only be emitted by AppendStreamTableSink, " +
                                   "RetractStreamTableSink, or UpsertStreamTableSink.")
    }
  }

  def registerTableWithWatermark(
      tableName: String,
      sourceTable: Table,
      rowtimeField: String,
      watermarkCall: SqlCall): Unit = {
    val offset = WatermarkUtils.getWithOffsetParameters(rowtimeField, watermarkCall)
    val source = sourceTable.getRelNode
    registerTable(
      tableName,
      new Table(
        this,
        new LogicalRelNode(
          new LogicalWatermarkAssigner(
            source.getCluster,
            source.getTraitSet,
            source,
            rowtimeField,
            offset
          )
        )
      )
    )
  }

  def registerTableWithPk(
    tableName: String,
    sourceTable: Table,
    primaryKeys: util.List[String]): Unit = {

    val source = sourceTable.getRelNode
    registerTable(tableName, new Table(this,
      new LogicalRelNode(
      new LogicalLastRow(
        source.getCluster,
        source.getTraitSet,
        source,
        primaryKeys
      ))))
  }

  private def getUniqueKeys(relNode: RelNode): util.Set[_ <: util.Set[String]] = {
    val rowType = relNode.getRowType
    val uniqueKeys = FlinkRelMetadataQuery
      .reuseOrCreate(relBuilder.getCluster.getMetadataQuery)
      .getUniqueKeys(relNode)
    if (uniqueKeys != null) {
      uniqueKeys.map { uniqueKey =>
        val keys = new util.HashSet[String]()
        uniqueKey.asList().asScala.foreach { idx =>
          keys.add(rowType.getFieldNames.get(idx))
        }
        keys
      }
    } else {
      null
    }
  }
}

