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
import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.graph.StreamGraphGenerator
import org.apache.flink.streaming.api.transformations.StreamTransformation
import org.apache.flink.table.api.types.{BaseRowType, DataType, DataTypes, InternalType}
import org.apache.flink.table.calcite.{FlinkChainContext, FlinkRelBuilder}
import org.apache.flink.table.catalog.ReadableCatalog
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.descriptors.{ConnectorDescriptor, StreamTableDescriptor}
import org.apache.flink.table.errorcode.TableErrors
import org.apache.flink.table.expressions._
import org.apache.flink.table.factories.{TableFactoryService, TableFactoryUtil, TableSourceParserFactory}
import org.apache.flink.table.plan.`trait`._
import org.apache.flink.table.plan.cost.{FlinkCostFactory, FlinkStreamCost}
import org.apache.flink.table.plan.logical.{LogicalRelNode, SinkNode}
import org.apache.flink.table.plan.nodes.calcite._
import org.apache.flink.table.plan.nodes.physical.stream._
import org.apache.flink.table.plan.optimize.{FlinkStreamPrograms, StreamOptimizeContext}
import org.apache.flink.table.plan.schema.{TableSourceSinkTable, _}
import org.apache.flink.table.plan.stats.FlinkStatistic
import org.apache.flink.table.plan.subplan.StreamDAGOptimizer
import org.apache.flink.table.plan.util.{FlinkRelOptUtil, SameRelObjectShuttle}
import org.apache.flink.table.sinks.{DataStreamTableSink, _}
import org.apache.flink.table.sources._
import org.apache.flink.table.typeutils.TypeCheckUtils
import org.apache.flink.table.util._
import org.apache.flink.util.Preconditions

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rex.RexBuilder
import org.apache.calcite.sql2rel.SqlToRelConverter

import _root_.java.util

import _root_.scala.collection.JavaConversions._
import _root_.scala.collection.JavaConverters._
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
@InterfaceStability.Evolving
abstract class StreamTableEnvironment(
    val execEnv: StreamExecutionEnvironment,
    config: TableConfig)
  extends TableEnvironment(config) {

  // prefix  for unique table names.
  override private[flink] val tableNamePrefix = "_DataStreamTable_"

  private val DEFAULT_JOB_NAME = "Flink Streaming Job"

  private var isConfigMerged: Boolean = false

  // the builder for Calcite RelNodes, Calcite's representation of a relational expression tree.
  override protected def createRelBuilder: FlinkRelBuilder = FlinkRelBuilder.create(
    frameworkConfig,
    config,
    getTypeFactory,
    Array(
      ConventionTraitDef.INSTANCE,
      FlinkRelDistributionTraitDef.INSTANCE,
      UpdateAsRetractionTraitDef.INSTANCE,
      AccModeTraitDef.INSTANCE),
    catalogManager
  )

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

  private[flink] override def compile(): Unit = {

    mergeParameters()

    if (config.getSubsectionOptimization) {
      if (sinkNodes.isEmpty) {
        throw new TableException(TableErrors.INST.sqlCompileNoSinkTblError())
      }
      val dagOptimizer = new StreamDAGOptimizer(sinkNodes, this)
      // optimize dag
      val sinks = dagOptimizer.getOptimizedDag()
      translateRelNodeDag(sinks)
    }
  }

  private def translateRelNodeDag(sinks: Seq[RelNode]): Unit = {
    // translates sinks RelNodeBlock into transformations
    sinks.foreach {
      case sink: Sink => translate(sink)
      case _ => throw new TableException(TableErrors.INST.sqlCompileSinkNodeRequired())
    }
  }

  /**
    * Registers an internal [[StreamTableSource]] in this [[TableEnvironment]]'s catalog without
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

    // check that event-time is enabled if table source includes rowtime attributes
    tableSource match {
      case tableSource: TableSource if TableSourceUtil.hasRowtimeAttribute(tableSource) &&
        execEnv.getStreamTimeCharacteristic != TimeCharacteristic.EventTime =>

        throw new TableException(
          s"A rowtime attribute requires an EventTime time characteristic in stream environment. " +
            s"But is: ${execEnv.getStreamTimeCharacteristic}")
      case _ => // ok
    }

    tableSource match {

      // check for proper stream table source
      case streamTableSource: StreamTableSource[_] =>
        // register
        getTable(name) match {

          // check if a table (source or sink) is registered
          case Some(table: TableSourceSinkTable[_]) => table.tableSourceTable match {

            // wrapper contains source
            case Some(_: TableSourceTable) =>
              throw new TableException(s"Table '$name' already exists. " +
                s"Please choose a different name.")

            // wrapper contains only sink (not source)
            case Some(_: StreamTableSourceTable[_]) =>
              val enrichedTable = new TableSourceSinkTable(
                Some(new StreamTableSourceTable(streamTableSource)),
                table.tableSinkTable)
              replaceRegisteredTable(name, enrichedTable)
          }

          // no table is registered
          case _ =>
            val newTable = new TableSourceSinkTable(
              Some(new StreamTableSourceTable(streamTableSource)),
              None)
            registerTableInternal(name, newTable)
        }

      // check for proper dimension table source
      case dimensionTableSource: DimensionTableSource[_] =>
        // register
        getTable(name) match {

          // a dimension table is already registered
          case Some(_: TableSourceTable) =>
            throw new TableException(s"Table '$name' already exists. " +
              s"Please choose a different name.")

          // no table is registered
          case _ =>
            val dimTableSourceTable =  if (dimensionTableSource.isTemporal) {
              new TemporalDimensionTableSourceTable(dimensionTableSource)
            } else {
              new DimensionTableSourceTable(dimensionTableSource)
            }
            registerTableInternal(name, dimTableSourceTable)
        }

      // not a stream table source
      case _ =>
        throw new TableException("Only StreamTableSource and DimensionTableSource " +
          "can be registered in StreamTableEnvironment")
    }
  }

  /**
    * Creates a table source and/or table sink from a descriptor.
    *
    * Descriptors allow for declaring the communication to external systems in an
    * implementation-agnostic way. The classpath is scanned for suitable table factories that match
    * the desired configuration.
    *
    * The following example shows how to read from a Kafka connector using a JSON format and
    * registering a table source "MyTable" in append mode:
    *
    * {{{
    *
    * tableEnv
    *   .connect(
    *     new Kafka()
    *       .version("0.11")
    *       .topic("clicks")
    *       .property("zookeeper.connect", "localhost")
    *       .property("group.id", "click-group")
    *       .startFromEarliest())
    *   .withFormat(
    *     new Json()
    *       .jsonSchema("{...}")
    *       .failOnMissingField(false))
    *   .withSchema(
    *     new Schema()
    *       .field("user-name", "VARCHAR").from("u_name")
    *       .field("count", "DECIMAL")
    *       .field("proc-time", "TIMESTAMP").proctime())
    *   .inAppendMode()
    *   .registerSource("MyTable")
    * }}}
    *
    * @param connectorDescriptor connector descriptor describing the external system
    */
  def connect(connectorDescriptor: ConnectorDescriptor): StreamTableDescriptor = {
    new StreamTableDescriptor(this, connectorDescriptor)
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
    if (fieldNames == null) throw new TableException("fieldNames must not be null.")
    if (fieldTypes == null) throw new TableException("fieldTypes must not be null.")
    if (fieldNames.length == 0) throw new TableException("fieldNames must not be empty.")
    if (fieldNames.length != fieldTypes.length) {
      throw new TableException("Same number of field names and types required.")
    }

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
      case _: StreamTableSink[_] =>

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

      // not a stream table sink
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
  override protected def getFlinkCostFactory: FlinkCostFactory = FlinkStreamCost.FACTORY

  /**
    * Writes a [[Table]] to a [[TableSink]].
    *
    * Internally, the [[Table]] is translated into a [[DataStream]] and handed over to the
    * [[TableSink]] to write it.
    *
    * @param table The [[Table]] to write.
    * @param sink The [[TableSink]] to write the [[Table]] to.
    * @tparam T The expected type of the [[DataStream]] which represents the [[Table]].
    */
  override private[table] def writeToSink[T](
      table: Table,
      sink: TableSink[T],
      sinkName: String): Unit = {
    mergeParameters()

    val sinkNode = SinkNode(table.logicalPlan, sink, sinkName)
    if (config.getSubsectionOptimization) {
      sinkNodes += sinkNode
    } else {
      val table = new Table(this, sinkNode)
      val optimizedPlan = optimize(table.getRelNode)
      translate(optimizedPlan)
    }
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
      throw new TableException(
        s"A rowtime attribute requires an EventTime time characteristic in stream environment. " +
            s"But is: ${execEnv.getStreamTimeCharacteristic}")
    }


    val (fieldNames, fieldIndexes) = getFieldInfo(streamType, fields)

    // validate and extract time attributes
    val (rowtime, proctime) = validateAndExtractTimeAttributes(streamType, fields)

    // check if event-time is enabled
    if (rowtime.isDefined && execEnv.getStreamTimeCharacteristic != TimeCharacteristic.EventTime) {
      throw new TableException(
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
  private[flink] def validateAndExtractTimeAttributes(
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
  private[flink] def adjustFieldIndexes(
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
  private[flink] def adjustFieldNames(
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
    * @param updatesAsRetraction True if request updates as retraction messages.
    * @return The optimized [[RelNode]] tree
    */
  private[flink] def optimize(
      relNode: RelNode,
      updatesAsRetraction: Boolean = false,
      isSinkBlock: Boolean = true): RelNode = {

    val programs = config.getCalciteConfig.getStreamPrograms
      .getOrElse(FlinkStreamPrograms.buildPrograms(config.getConf))
    Preconditions.checkNotNull(programs)

    val flinkChainContext = getPlanner.getContext.asInstanceOf[FlinkChainContext]

    val optimizeNode = programs.optimize(relNode, new StreamOptimizeContext() {
      override def getContext: Context = flinkChainContext

      override def getRelOptPlanner: RelOptPlanner = getPlanner

      override def getRexBuilder: RexBuilder = getRelBuilder.getRexBuilder

      override def updateAsRetraction(): Boolean = updatesAsRetraction

      override def isSinkNode: Boolean = isSinkBlock
    })
    // FIXME refactor
    // Rewrite same rel object to different rel objects.
    val diffObjPlan = optimizeNode.accept(new SameRelObjectShuttle())
    diffObjPlan
  }

  /**
    * Translates a [[Table]] into a [[DataStream]].
    *
    * The transformation involves optimizing the relational expression tree as defined by
    * Table API calls and / or SQL queries and generating corresponding [[DataStream]] operators.
    *
    * @param table The root node of the relational expression tree.
    * @param updatesAsRetraction Set to true to encode updates as retraction messages.
    * @param withChangeFlag Set to true to emit records with change flags.
    * @param resultType The [[DataType]] of the resulting [[DataStream]].
    * @tparam A The type of the resulting [[DataStream]].
    * @return The [[DataStream]] that corresponds to the translated [[Table]].
    */
  protected def translateToDataStream[A](
      table: Table,
      updatesAsRetraction: Boolean,
      withChangeFlag: Boolean,
      resultType: DataType): DataStream[A] = {

    mergeParameters()
    val sink = new DataStreamTableSink[A](table, resultType, updatesAsRetraction, withChangeFlag)
    val sinkName = createUniqueTableName()
    val sinkNode = LogicalSink.create(table.getRelNode, sink, sinkName)
    val optimizedPlan = optimize(sinkNode)
    val transformation = translate(optimizedPlan)
    new DataStream(execEnv, transformation).asInstanceOf[DataStream[A]]
  }

    /**
      * Translates a physical [[RelNode]] plan into a [[StreamTransformation]].
      *
      * @param node The logical plan to translate.
      * @return The [[StreamTransformation]] of type [[BaseRow]].
      */
    private def translate(node: RelNode): StreamTransformation[_] = {

      node match {
        case node: StreamExecRel[_] => node.translateToPlan(this)
        case _ =>
          throw new TableException("Cannot generate DataStream due to an invalid logical plan. " +
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
    val optimizedPlan = optimize(ast)
    val transformStream = translate(optimizedPlan)

    val streamGraph = StreamGraphGenerator.generate(
      StreamGraphGenerator.Context.buildStreamProperties(execEnv), ArrayBuffer(transformStream))

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

  /**
    * Merge global job parameters and table config parameters,
    * and set the merged result to GlobalJobParameters
    */
  private def mergeParameters(): Unit = {
    if (!isConfigMerged && execEnv != null && execEnv.getConfig != null) {
      val parameters = new Configuration()
      if (config != null && config.getConf != null) {
        parameters.addAll(config.getConf)
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

  def explain(extended: Boolean = false): String = {
    if (!config.getSubsectionOptimization) {
      throw new TableException("Can not explain due to subsection optimization is not supported, " +
                                 "please check your TableConfig.")
    }

    if (sinkNodes.isEmpty) {
      throw new TableException(TableErrors.INST.sqlCompileNoSinkTblError())
    }

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
    val dagOptimizer = new StreamDAGOptimizer(sinkNodes, this)
    // optimize dag
    val sinks = dagOptimizer.getOptimizedDag()
    sb.append(dagOptimizer.explain())

    // translate relNodes to StreamTransformations
    translateRelNodeDag(sinks)
    val sqlPlan = PlanUtil.explainPlan(execEnv.getStreamGraph)
    sb.append("== Physical Execution Plan ==")
    sb.append(System.lineSeparator)
    sb.append(sqlPlan)
    sb.toString()
  }

  /**
    * Register a table with specific row time field and offset.
    * @param tableName table name
    * @param sourceTable table to register
    * @param rowtimeField row time field
    * @param offset offset to the row time field value
    */
  @VisibleForTesting
  def registerTableWithWatermark(
      tableName: String,
      sourceTable: Table,
      rowtimeField: String,
      offset: Long): Unit = {

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

  /**
    * Register a table with specific list of primary keys.
    * @param tableName table name
    * @param sourceTable table to register
    * @param primaryKeys table primary field name list
    */
  @VisibleForTesting
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

  override def registerTableSourceFromTableMetas(name: String, tableMeta: TableMeta): Unit = {
    // table schema
    val tableSchema: TableSchema = tableMeta.getSchema
    // table properties
    val tableProperties: TableProperties = tableMeta.getProperties.toKeyLowerCase

    if (tableSchema == null || tableProperties == null) {
      throw new TableException("TableSchema or TableProperties should not be null! " +
        "Register Table should with a TableSchema and TableProperties!")
    }

    // table schema
    val richTableSchema = new RichTableSchema(
      tableMeta.getSchema.getColumnNames,
      tableMeta.getSchema.getTypes)
    tableProperties.putSchemaIntoProperties(richTableSchema)

    // table source
    val tableDiscriptor = TableFactoryUtil.getDiscriptorFromTableProperties(tableProperties)
    val tableSource = TableFactoryUtil.findAndCreateTableSource(
      this, tableDiscriptor, this.userClassloader)
    // parser
    val parser = try {
      TableFactoryService
        .find(classOf[TableSourceParserFactory], tableDiscriptor, this.userClassloader)
        .createParser(name, richTableSchema, tableProperties)
    } catch {
      case _: NoMatchingTableFactoryException => null
    }

    val hasParser = parser != null
    val hasComputedColumn = tableSchema.getComputedColumns.size > 0
    val hasPk = tableSchema.getPrimaryKeys.size > 0
    val hasWatermark = tableSchema.getWatermarks.size > 0

    var tempTable = name
    if (hasParser || hasComputedColumn || hasPk || hasWatermark) {
      tempTable = createUniqueTableName()
    }

    // 1. source
    registerTableSource(tempTable, tableSource)

    // 2. parser
    if (hasParser) {
      val tableFunction = parser.getParser
      val tfName = tableFunction.toString
      val functionName = createUniqueAttributeName(tfName)
      this.registerFunction(functionName, tableFunction)
      val tab2 = scan(tempTable)
        .join(new Table(this, s"$functionName(f0)"))
        .select(s"${tableSchema.getColumnNames.mkString(", ")}")

      tempTable = name
      if (hasComputedColumn || hasPk || hasWatermark) {
        tempTable = createUniqueTableName()
      }
      registerTable(tempTable, tab2)
    }

    // 3. computed column
    if (hasComputedColumn) {
      val tab3 = scan(tempTable)
        .select(s"${tableSchema.getColumnNames.mkString(", ")}" + ", " +
          s"${tableSchema.getComputedColumns.map(e =>
            e.expression + s" as ${e.name}").mkString(", ")}")
      tempTable = name
      if (hasWatermark || hasPk) {
        tempTable = createUniqueTableName()
      }
      registerTable(tempTable, tab3)
    }

    // 4. watermark
    if (hasWatermark) {
      val watermark = tableSchema.getWatermarks.head
      val tab4 = scan(tempTable)
      tempTable = name
      if (hasPk) {
        tempTable = createUniqueTableName()
      }
      registerTableWithWatermark(tempTable, tab4, watermark.eventTime, watermark.offset)
    }

    // 5. pk
    if (hasPk) {
      registerTableWithPk(name, scan(tempTable), tableSchema.getPrimaryKeys.toList)
    }
  }

  override def registerCatalog(name: String, catalog: ReadableCatalog): Unit = {
    registerCatalogInternal(name, catalog, true)
  }
}
