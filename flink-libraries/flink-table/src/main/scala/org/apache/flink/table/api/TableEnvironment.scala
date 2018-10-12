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

import _root_.java.lang.reflect.Modifier
import _root_.java.util
import _root_.java.util.concurrent.atomic.AtomicInteger

import org.apache.calcite.config.Lex
import org.apache.calcite.jdbc.CalciteSchema
import org.apache.calcite.plan.{Contexts, RelOptPlanner}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.logical.LogicalTableModify
import org.apache.calcite.schema
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.schema.impl.AbstractTable
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.sql.util.ChainedSqlOperatorTable
import org.apache.calcite.sql.{SqlIdentifier, SqlInsert, SqlOperatorTable, _}
import org.apache.calcite.sql2rel.SqlToRelConverter
import org.apache.calcite.tools._
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.{AtomicType, TypeInformation}
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.java.typeutils.{RowTypeInfo, _}
import org.apache.flink.api.scala.createTuple2TypeInformation
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.{StreamExecutionEnvironment => JavaStreamExecEnv}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment => ScalaStreamExecEnv}
import org.apache.flink.streaming.api.transformations.StreamTransformation
import org.apache.flink.table.api.java.{BatchTableEnvironment => JavaBatchTableEnvironment, StreamTableEnvironment => JavaStreamTableEnv}
import org.apache.flink.table.api.scala.{BatchTableEnvironment => ScalaBatchTableEnvironment, StreamTableEnvironment => ScalaStreamTableEnv}
import org.apache.flink.table.calcite._
import org.apache.flink.table.catalog._
import org.apache.flink.table.codegen._
import org.apache.flink.table.codegen.operator.OperatorCodeGenerator
import org.apache.flink.table.codegen.operator.OperatorCodeGenerator.generatorCollect
import org.apache.flink.table.connector.DefinedDistribution
import org.apache.flink.table.dataformat.{BaseRow, GenericRow}
import org.apache.flink.table.descriptors.{ConnectorDescriptor, TableDescriptor}
import org.apache.flink.table.errorcode.TableErrors
import org.apache.flink.table.expressions.{Alias, Expression, TimeAttribute, UnresolvedFieldReference}
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils._
import org.apache.flink.table.functions.{TableValuedAggregateFunction, CoTableValuedAggregateFunction, AggregateFunction, ScalarFunction, TableFunction}
import org.apache.flink.table.plan.cost.FlinkCostFactory
import org.apache.flink.table.plan.logical.{CatalogNode, LogicalNode, LogicalRelNode}
import org.apache.flink.table.plan.schema._
import org.apache.flink.table.plan.stats.{FlinkStatistic, TableStats}
import org.apache.flink.table.runtime.conversion.InternalTypeConverters.genToExternal
import org.apache.flink.table.runtime.operator.OneInputSubstituteStreamOperator
import org.apache.flink.table.sinks._
import org.apache.flink.table.sources.TableSource
import org.apache.flink.table.types._
import org.apache.flink.table.typeutils.TypeUtils.getCompositeTypes
import org.apache.flink.table.typeutils.{BaseRowTypeInfo, TimeIndicatorTypeInfo, TypeUtils}
import org.apache.flink.table.util.{BaseRowUtil, PartitionUtils}
import org.apache.flink.table.validate.{BuiltInFunctionCatalog, ChainedFunctionCatalog, ExternalFunctionCatalog, FunctionCatalog}
import org.apache.flink.types.Row

import _root_.scala.annotation.varargs
import _root_.scala.collection.JavaConversions._
import _root_.scala.collection.JavaConverters._
import _root_.scala.collection.mutable

/**
  * The abstract base class for batch and stream TableEnvironments.
  *
  * @param config The configuration of the TableEnvironment
  */
abstract class TableEnvironment(val config: TableConfig) {

  val DEFAULT_SCHEMA: String = "hive"

  // the catalog to hold all registered and translated tables
  // we disable caching here to prevent side effects
  private val internalSchema: CalciteSchema = CalciteSchema.createRootSchema(true, false)
  private val rootSchema: SchemaPlus = internalSchema.plus()

  private val typeFactory: FlinkTypeFactory = new FlinkTypeFactory(new FlinkTypeSystem)

  // registered external catalog names -> catalog
  private lazy val externalCatalogs = new mutable.LinkedHashMap[String, ExternalCatalog]

  // Table API/SQL function catalog (built in, does not contain external functions)
  private val functionCatalog: FunctionCatalog = BuiltInFunctionCatalog.withBuiltIns

  // Table API/SQL external function catalogs
  private lazy val externalFunctionCatalogs: Seq[FunctionCatalog] =
    externalCatalogs.values.map(new ExternalFunctionCatalog(_, typeFactory)).toSeq

  // Table API/SQL function catalog which chained external function catalogs
  // and built in function catalog.
  // The chained order:
  //    external function catalogs precede built-in function catalog.
  //    when multiple external function catalogs, the order same as insertion order.
  private[flink] lazy val chainedFunctionCatalog: FunctionCatalog =
    new ChainedFunctionCatalog(externalFunctionCatalogs :+ functionCatalog)

  // the configuration to create a Calcite planner
  protected lazy val frameworkConfig: FrameworkConfig = Frameworks
    .newConfigBuilder
    .defaultSchema(rootSchema)
    .parserConfig(getSqlParserConfig)
    .costFactory(getFlinkCostFactory)
    .operatorTable(getSqlOperatorTable)
    // set the executor to evaluate constant expressions
    .executor(new ExpressionReducer(config))
    .context(FlinkChainContext.chain(Contexts.of(config), Contexts.of(chainedFunctionCatalog)))
    .build

  // the builder for Calcite RelNodes, Calcite's representation of a relational expression tree.
  protected lazy val relBuilder: FlinkRelBuilder = FlinkRelBuilder.create(
    frameworkConfig, config, getTypeFactory)

  // the planner instance used to optimize queries of this TableEnvironment
  private lazy val planner: RelOptPlanner = relBuilder.getPlanner

  // reuse flink planner
  private lazy val flinkPlanner = new FlinkPlannerImpl(
    getFrameworkConfig,
    getPlanner,
    getTypeFactory,
    sqlToRelConverterConfig,
    relBuilder.getCluster)

  // a counter for unique attribute names
  private[flink] val attrNameCntr: AtomicInteger = new AtomicInteger(0)

  // a counter for unique table names
  private[flink] val tableNameCntr: AtomicInteger = new AtomicInteger(0)

  private[flink] val tableNamePrefix = "_TempTable_"

  // sink nodes collection
  private[flink] val sinkNodes = new mutable.MutableList[LogicalNode]

  // the configuration for SqlToRelConverter
  private[flink] lazy val sqlToRelConverterConfig: SqlToRelConverter.Config = {
    val calciteConfig = config.getCalciteConfig
    calciteConfig.getSqlToRelConverterConfig match {
      case Some(c) => c
      case None => getSqlToRelConverterConfig
    }
  }

  /** Returns the table config to define the runtime behavior of the Table API. */
  def getConfig: TableConfig = config

  /** Returns the [[QueryConfig]] depends on the concrete type of this TableEnvironment. */
  private[flink] def queryConfig: QueryConfig = this match {
    case _: BatchTableEnvironment => new BatchQueryConfig
    case _: StreamTableEnvironment => new StreamQueryConfig
    case _ => null
  }

  /**
    * Returns the operator table for this environment including a custom Calcite configuration.
    */
  protected def getSqlOperatorTable: SqlOperatorTable = {
    val calciteConfig = config.getCalciteConfig

    calciteConfig.getSqlOperatorTable match {
      case None =>
        chainedFunctionCatalog.getSqlOperatorTable
      case Some(table) =>
        if (calciteConfig.replacesSqlOperatorTable) {
          table
        } else {
          ChainedSqlOperatorTable.of(chainedFunctionCatalog.getSqlOperatorTable, table)
        }
    }
  }

  /**
    * Returns the SQL parser config for this environment including a custom Calcite configuration.
    */
  protected def getSqlParserConfig: SqlParser.Config = {
    val calciteConfig = config.getCalciteConfig
    calciteConfig.getSqlParserConfig match {

      case None =>
        // we use Java lex because back ticks are easier than double quotes in programming
        // and cases are preserved
        SqlParser
          .configBuilder()
          .setLex(Lex.JAVA)
          .build()

      case Some(sqlParserConfig) =>
        sqlParserConfig
    }
  }

  /**
    * Returns the SqlToRelConverter config.
    */
  protected def getSqlToRelConverterConfig: SqlToRelConverter.Config =
    SqlToRelConverter.configBuilder()
      .withTrimUnusedFields(false)
      .withConvertTableAccess(false)
      .build()

  /**
    * Registers an [[ExternalCatalog]] under a unique name in the TableEnvironment's schema.
    * All tables registered in the [[ExternalCatalog]] can be accessed.
    * Current implementation does not allow registering multiple external catalogs.
    *
    * @param name            The name under which the externalCatalog will be registered
    * @param externalCatalog The externalCatalog to register
    */
  def registerExternalCatalog(name: String, externalCatalog: ExternalCatalog): Unit = ???

  /**
    * Registers an [[ExternalCatalog]] under a unique name in the TableEnvironment's schema.
    * All tables registered in the [[ExternalCatalog]] can be accessed.
    *
    * @param name            The name under which the externalCatalog will be registered
    * @param externalCatalog The externalCatalog to register
    */
  def registerExternalCatalogInternal(
      name: String, externalCatalog: ExternalCatalog, isStreaming: Boolean): Unit = {
    if (rootSchema.getSubSchema(name) != null) {
      throw new ExternalCatalogAlreadyExistException(name)
    }

    if (!externalCatalogs.isEmpty) {
      throw new UnsupportedOperationException(
        "Unsupported registering multiple external catalogs")
    }

    this.externalCatalogs.put(name, externalCatalog)
    // create an external catalog calcite schema, register it on the root schema
    ExternalCatalogSchema.registerCatalog(rootSchema, name, externalCatalog, isStreaming)
  }

  /**
    * Gets a registered [[ExternalCatalog]] by name.
    *
    * @param name The name to look up the [[ExternalCatalog]]
    * @return The [[ExternalCatalog]]
    */
  def getRegisteredExternalCatalog(name: String): ExternalCatalog = {
    this.externalCatalogs.get(name) match {
      case Some(catalog) => catalog
      case None => throw new ExternalCatalogNotExistException(name)
    }
  }

  /**
    * Registers a [[ScalarFunction]] under a unique name. Replaces already existing
    * user-defined functions under this name.
    */
  def registerFunction(name: String, function: ScalarFunction): Unit = {
    // check if class could be instantiated
    checkForInstantiation(function.getClass)

    // register in Table API
    functionCatalog.registerFunction(name, function.getClass)

    // register in SQL API
    functionCatalog.registerSqlFunction(
      createScalarSqlFunction(name, name, function, typeFactory)
    )
  }

  /**
   * Registers an [[AggregateFunction]] under a unique name in the TableEnvironment's catalog.
   * Registered functions can be referenced in Table API and SQL queries.
   *
   * @param name The name under which the function is registered.
   * @param f The AggregateFunction to register.
   * @tparam T The type of the output value.
   * @tparam ACC The type of aggregate accumulator.
   */
  def registerFunction[T, ACC](
      name: String,
      f: AggregateFunction[T, ACC])
  : Unit = {
    val resultType = DataTypes.of(TypeExtractor
        .createTypeInfo(f, classOf[AggregateFunction[T, ACC]], f.getClass, 0))

    val accType = DataTypes.of(TypeExtractor
        .createTypeInfo(f, classOf[AggregateFunction[T, ACC]], f.getClass, 1))

    registerAggregateFunction(name, f, resultType, accType)
  }

  /**
    * Registers an [[TableValuedAggregateFunction]] under a unique name in the
    * TableEnvironment's catalog.
    * Registered functions can be referenced in Table API and SQL queries.
    *
    * @param name The name under which the function is registered.
    * @param f The AggregateFunction to register.
    * @tparam T The type of the output value.
    * @tparam ACC The type of aggregate accumulator.
    */
  def registerFunction[T, ACC](
    name: String,
    f: TableValuedAggregateFunction[T, ACC])
  : Unit = {
    val resultType = DataTypes.of(
      TypeExtractor.createTypeInfo(
        f, classOf[TableValuedAggregateFunction[T, ACC]], f.getClass, 0))

    val accType = DataTypes.of(
      TypeExtractor.createTypeInfo(
        f, classOf[TableValuedAggregateFunction[T, ACC]], f.getClass, 1))

    registerTableValuedAggregateFunction(name, f, resultType, accType)
  }

  /**
    * Registers an [[CoTableValuedAggregateFunction]] under a unique name in the
    * TableEnvironment's catalog.
    * Registered functions can be referenced in Table API and SQL queries.
    *
    * @param name The name under which the function is registered.
    * @param f The AggregateFunction to register.
    * @tparam T The type of the output value.
    * @tparam ACC The type of aggregate accumulator.
    */
  def registerFunction[T, ACC](
    name: String,
    f: CoTableValuedAggregateFunction[T, ACC])
  : Unit = {
    val resultType = DataTypes.of(
      TypeExtractor.createTypeInfo(
        f, classOf[CoTableValuedAggregateFunction[T, ACC]], f.getClass, 0))

    val accType = DataTypes.of(
      TypeExtractor.createTypeInfo(
        f, classOf[CoTableValuedAggregateFunction[T, ACC]], f.getClass, 1))

    registerCoTableValuedAggregateFunction(name, f, resultType, accType)
  }

  /**
   * Registers a [[TableFunction]] under a unique name in the TableEnvironment's catalog.
   * Registered functions can be referenced in Table API and SQL queries.
   *
   * @param name The name under which the function is registered.
   * @param tf The TableFunction to register.
   * @tparam T The type of the output row.
   */
  def registerFunction[T](name: String, tf: TableFunction[T]): Unit = {
    val implicitResultType = UserDefinedFunctionUtils.getImplicitResultType(tf)
    registerTableFunction(name, tf, implicitResultType)
  }

  /**
    * Registers a [[TableFunction]] under a unique name. Replaces already existing
    * user-defined functions under this name.
    */
  def registerTableFunction[T](
      name: String, function: TableFunction[T], implicitResultType: DataType): Unit = {
    // check if class not Scala object
    checkNotSingleton(function.getClass)
    // check if class could be instantiated
    checkForInstantiation(function.getClass)

    // register in Table API
    functionCatalog.registerFunction(name, function.getClass)

    // register in SQL API
    val sqlFunctions =
      createTableSqlFunction(name, name, function, implicitResultType, typeFactory)
    functionCatalog.registerSqlFunction(sqlFunctions)
  }

  /**
    * Registers an [[AggregateFunction]] under a unique name. Replaces already existing
    * user-defined functions under this name.
    */
  def registerAggregateFunction[T, ACC](
      name: String,
      function: AggregateFunction[T, ACC],
      implicitResultType: DataType,
      implicitAccType: DataType): Unit = {
    // check if class not Scala object
    checkNotSingleton(function.getClass)
    // check if class could be instantiated
    checkForInstantiation(function.getClass)

    val resultType = getResultTypeOfAggregateFunction(function, implicitResultType)
    val accType = getAccumulatorTypeOfAggregateFunction(function, implicitAccType)

    // register in Table API
    functionCatalog.registerFunction(name, function.getClass)

    // register in SQL API
    val sqlFunctions = createAggregateSqlFunction(
      name,
      name,
      function,
      resultType,
      accType,
      typeFactory)

    functionCatalog.registerSqlFunction(sqlFunctions)
  }

  /**
    * Registers an [[TableValuedAggregateFunction]] under a unique name. Replaces already existing
    * user-defined functions under this name.
    */
  def registerTableValuedAggregateFunction[T, ACC](
    name: String,
    function: TableValuedAggregateFunction[T, ACC],
    implicitResultType: DataType,
    implicitAccType: DataType): Unit = {
    // check if class not Scala object
    checkNotSingleton(function.getClass)
    // check if class could be instantiated
    checkForInstantiation(function.getClass)

    val resultType = getResultTypeOfAggregateFunction(function, implicitResultType)
    val accType = getAccumulatorTypeOfAggregateFunction(function, implicitAccType)

    // register in Table API
    functionCatalog.registerFunction(name, function.getClass)

    // register in SQL API
    val sqlFunctions = createTableValuedAggregateSqlFunction(
      name,
      name,
      function,
      resultType,
      accType,
      typeFactory)

    functionCatalog.registerSqlFunction(sqlFunctions)
  }

  /**
    * Registers a [[CoTableValuedAggregateFunction]] under a unique name. Replaces already existing
    * user-defined functions under this name.
    */
  def registerCoTableValuedAggregateFunction[T, ACC](
    name: String,
    function: CoTableValuedAggregateFunction[T, ACC],
    implicitResultType: DataType,
    implicitAccType: DataType): Unit = {
    // check if class not Scala object
    checkNotSingleton(function.getClass)
    // check if class could be instantiated
    checkForInstantiation(function.getClass)

    val resultType = getResultTypeOfAggregateFunction(function, implicitResultType)
    val accType = getAccumulatorTypeOfAggregateFunction(function, implicitAccType)

    // register in Table API
    functionCatalog.registerFunction(name, function.getClass)

    // register in SQL API
    val sqlFunctions = createCoTableValuedAggregateSqlFunction(
      name,
      name,
      function,
      resultType,
      accType,
      typeFactory)

    functionCatalog.registerSqlFunction(sqlFunctions)
  }

  /**
    * Registers a [[Table]] under a unique name in the TableEnvironment's catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * @param name The name under which the table will be registered.
    * @param table The table to register.
    */
  def registerTable(name: String, table: Table): Unit = {

    // check that table belongs to this table environment
    if (table.tableEnv != this) {
      throw new TableException(
        "Only tables that belong to this TableEnvironment can be registered.")
    }

    checkValidTableName(name)
    val tableTable = new RelTable(table.getRelNode)
    registerTableInternal(name, tableTable)
  }

  /**
    * Registers an external [[TableSource]] in this [[TableEnvironment]]'s catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * @param name        The name under which the [[TableSource]] is registered.
    * @param tableSource The [[TableSource]] to register.
    */
  def registerTableSource(name: String, tableSource: TableSource): Unit

  /**
    * Gets the statistics of a table.
    * Note: this function returns current statistics of the table directly, does not trigger
    *       statistics gather operation.
    *
    * @param tableName The table name under which the table is registered in [[TableEnvironment]].
    *                  tableName must be a single name(e.g. "MyTable") associated with a table.
    * @return Statistics of a table if the statistics is available, else return null.
    */
  def getTableStats(tableName: String): TableStats = {
    require(tableName != null && tableName.nonEmpty, "tableName must not be null or empty.")
    getTableStats(Array(tableName))
  }

  /**
    * Gets the statistics of a table.
    * Note: this function returns current statistics of the table directly, does not trigger
    *       statistics gather operation.
    *
    * @param tablePath The table name under which the table is registered in [[TableEnvironment]].
    *                  tablePath can be a single name(e.g. Array("MyTable")) associated with a
    *                  table , or can be a nest names (e.g. Array("MyCatalog", "MyDb", "MyTable"))
    *                  associated with a table registered as member of an [[ExternalCatalog]].
    * @return Statistics of a table if the statistics is available, else return null.
    */
  def getTableStats(tablePath: Array[String]): TableStats = {
    val table = getTable(tablePath: _*)
    if (table == null) {
      throw new TableException(s"Table '${tablePath.mkString(".")}' was not found.")
    }
    val tableName = tablePath.last
    val stats = if (tablePath.length == 1) {
      table match {
        case t: FlinkTable =>
          // call statistic instead of getStatistics of FlinkTable to fetch the original statistics.
          if (t.getStatistic == null) {
            None
          } else {
            Option(t.getStatistic.getTableStats)
          }
        case _ => None
      }
    } else {
      // table in external catalog
      val rootCatalog = getRegisteredExternalCatalog(tablePath.head)
      val leafCatalog = tablePath.slice(1, tablePath.length - 1).foldLeft(rootCatalog) {
        case (parentCatalog, name) => parentCatalog.getSubCatalog(name)
      }
      Option(leafCatalog.getTable(tableName).stats)
    }
    stats.orNull
  }

  /**
    *  Alters the statistics of a table.
    *
    * @param tableName The table name under which the table is registered in [[TableEnvironment]].
    *                  tableName must be a single name(e.g. "MyTable") associated with a table.
    * @param tableStats The [[TableStats]] to update.
    */
  def alterTableStats(tableName: String, tableStats: TableStats): Unit = {
    alterTableStats(tableName, Option(tableStats))
  }


  /**
    *  Alters the statistics of a table.
    *
    * @param tablePath The table name under which the table is registered in [[TableEnvironment]].
    *                  tablePath can be a single name(e.g. Array("MyTable")) associated with a
    *                  table , or can be a nest names (e.g. Array("MyCatalog", "MyDb", "MyTable"))
    *                  associated with a table registered as member of an [[ExternalCatalog]].
    * @param tableStats The [[TableStats]] to update.
    */
  def alterTableStats(tablePath: Array[String], tableStats: TableStats): Unit = {
    alterTableStats(tablePath, Option(tableStats))
  }

  /**
    *  Alters the statistics of a table.
    *
    * @param tableName The table name under which the table is registered in [[TableEnvironment]].
    *                  tableName must be a single name(e.g. "MyTable") associated with a table.
    * @param tableStats The [[TableStats]] to update.
    */
  def alterTableStats(tableName: String, tableStats: Option[TableStats]): Unit = {
    require(tableName != null && tableName.nonEmpty, "tableName must not be null or empty.")
    alterTableStats(Array(tableName), tableStats)
  }

  /**
    *  Alters the statistics of a table.
    *
    * @param tablePath The table name under which the table is registered in [[TableEnvironment]].
    *                  tablePath can be a single name(e.g. Array("MyTable")) associated with a
    *                  table , or can be a nest names (e.g. Array("MyCatalog", "MyDb", "MyTable"))
    *                  associated with a table registered as member of an [[ExternalCatalog]].
    * @param tableStats The [[TableStats]] to update.
    */
  def alterTableStats(tablePath: Array[String], tableStats: Option[TableStats]): Unit = {
    val table = getTable(tablePath: _*)
    if (table == null) {
      throw new TableException(s"Table '${tablePath.mkString(".")}' was not found.")
    }

    val tableName = tablePath.last
    if (tablePath.length == 1) {
      // table in calcite root schema
      val statistic = table match {
        // call statistic instead of getStatistics of TableSourceTable
        // to fetch the original statistics.
        case t: TableSourceTable => t.statistic
        case t: FlinkTable => t.getStatistic
        case _ => throw new TableException(
          s"alter TableStats operation is not supported for ${table.getClass}.")
      }
      val (uniqueKeys, skewInfo) = if (statistic == null)  {
        (null, null)
      } else {
        (statistic.getUniqueKeys, statistic.getSkewInfo)
      }
      val newTable = table.asInstanceOf[FlinkTable]
          .copy(FlinkStatistic.of(tableStats.orNull, uniqueKeys, skewInfo))
      replaceRegisteredTable(tableName, newTable)
    } else {
      // table in external catalog
      val rootCatalog = getRegisteredExternalCatalog(tablePath.head)
      val leafCatalog = tablePath.slice(1, tablePath.length - 1).foldLeft(rootCatalog) {
        case (parentCatalog, name) => parentCatalog.getSubCatalog(name)
      }
      leafCatalog match {
        case c: CrudExternalCatalog =>
          c.alterTableStats(tableName, tableStats, ignoreIfNotExists = false)
        case _ => throw new TableException(
          s"alterTableStats operation is not supported for ${leafCatalog.getClass}.")
      }
    }
  }

  /**
    * Alter skew info to a table, optimizer will pick the skewed values to join separately.
    *
    * @param tableName table name to alter.
    * @param skewInfo statistics of skewedColNames and skewedColValues.
    */
  def alterSkewInfo(
      tableName: String,
      skewInfo: util.Map[String, util.List[AnyRef]]): Unit = {
    require(tableName != null && tableName.nonEmpty, "tableName must not be null or empty.")
    alterSkewInfo(Array(tableName), skewInfo)
  }

  private def alterSkewInfo(
      tablePath: Array[String],
      skewInfo: util.Map[String, util.List[AnyRef]]): Unit = {
    val table = getTable(tablePath: _*)
    if (table == null) {
      throw new TableException(s"Table '${tablePath.mkString(".")}' was not found.")
    }

    val tableName = tablePath.last
    if (tablePath.length == 1) {
      // table in calcite root schema
      val statistic = table match {
        // call statistic instead of getStatistics of TableSourceTable
        // to fetch the original statistics.
        case t: TableSourceTable => t.statistic
        case t: FlinkTable => t.getStatistic
        case _ => throw new TableException(
          s"alter SkewInfo operation is not supported for ${table.getClass}.")
      }

      val (uniqueKeys, tableStats) = if (statistic == null)  {
        (null, null)
      } else {
        (statistic.getUniqueKeys, statistic.getTableStats)
      }
      val newTable =  table.asInstanceOf[FlinkTable]
          .copy(FlinkStatistic.of(tableStats, uniqueKeys, skewInfo))
      replaceRegisteredTable(tableName, newTable)
    } else {
      throw new TableException("alterSkewInfo operation is not supported for external catalog.")
    }
  }

  def alterUniqueKeys(
      tablePath: Array[String],
      uniqueKeys: util.Set[util.Set[String]]): Unit = {
    val table = getTable(tablePath: _*)
    if (table == null) {
      throw new TableException(s"Table '${tablePath.mkString(".")}' was not found.")
    }

    val tableName = tablePath.last
    if (tablePath.length == 1) {
      // table in calcite root schema
      val statistic = table match {
        // call statistic instead of getStatistics of TableSourceTable
        // to fetch the original statistics.
        case t: TableSourceTable => t.statistic
        case t: FlinkTable => t.getStatistic
        case _ => throw new TableException(
          s"alter UniqueKeys operation is not supported for ${table.getClass}.")
      }

      val (skewInfo, tableStats) = if (statistic == null)  {
        (null, null)
      } else {
        (statistic.getSkewInfo, statistic.getTableStats)
      }
      val newTable = table.asInstanceOf[FlinkTable]
          .copy(FlinkStatistic.of(tableStats, uniqueKeys, skewInfo))
      replaceRegisteredTable(tableName, newTable)
    } else {
      throw new TableException("alter unique keys operation is not supported for external catalog.")
    }
  }

  /**
    * Registers an external [[TableSink]] with given field names and types in this
    * [[TableEnvironment]]'s catalog.
    * Registered sink tables can be referenced in SQL DML statements.
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
      tableSink: TableSink[_]): Unit

  /**
    * Registers an external [[TableSink]] with already configured field names and field types in
    * this [[TableEnvironment]]'s catalog.
    * Registered sink tables can be referenced in SQL DML statements.
    *
    * @param name The name under which the [[TableSink]] is registered.
    * @param configuredSink The configured [[TableSink]] to register.
    */
  def registerTableSink(name: String, configuredSink: TableSink[_]): Unit

  private[flink] def getStateTableNameForWrite(name: String): String = {
    s"__W_$name"
  }

  /**
    * Replaces a registered Table with another Table under the same name.
    * We use this method to replace a [[org.apache.flink.table.plan.schema.DataStreamTable]]
    * with a [[org.apache.calcite.schema.TranslatableTable]].
    *
    * @param name Name of the table to replace.
    * @param table The table that replaces the previous table.
    */
  protected def replaceRegisteredTable(name: String, table: AbstractTable): Unit = {

    if (isRegistered(name)) {
      rootSchema.add(name, table)
    } else {
      throw new TableException(s"Table \'$name\' is not registered.")
    }
  }

  private[flink] def collect[T](
      table: Table,
      sink: CollectTableSink[T],
      jobName: Option[String]): Seq[T] = {
    throw new TableException(s"collect is not supported.")
  }

  /**
    * Scans a registered table and returns the resulting [[Table]].
    *
    * A table to scan must be registered in the TableEnvironment. It can be either directly
    * registered as DataStream, DataSet, or Table or as member of an [[ExternalCatalog]].
    *
    * Examples:
    *
    * - Scanning a directly registered table
    * {{{
    *   val tab: Table = tableEnv.scan("tableName")
    * }}}
    *
    * - Scanning a table from a registered catalog
    * {{{
    *   val tab: Table = tableEnv.scan("catalogName", "dbName", "tableName")
    * }}}
    *
    * @param tablePath The path of the table to scan.
    * @throws TableException if no table is found using the given table path.
    * @return The resulting [[Table]].
    */
  @throws[TableException]
  @varargs
  def scan(tablePath: String*): Table = {
    scanInternal(tablePath.toArray) match {
      case Some(table) => table
      case None => throw new TableException(s"Table '${tablePath.mkString(".")}' was not found.")
    }
  }

  private[flink] def scanInternal(tablePath: Array[String]): Option[Table] = {
    val table = getTable(tablePath: _*)
    if (table != null) {
      Some(new Table(this, CatalogNode(tablePath, table.getRowType(typeFactory))))
    } else {
      None
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
  def connect(connectorDescriptor: ConnectorDescriptor): TableDescriptor

  private def getSchema(schemaPath: Array[String]): SchemaPlus = {
    var schema = rootSchema
    for (schemaName <- schemaPath) {
      schema = schema.getSubSchema(schemaName)
      if (schema == null) {
        return schema
      }
    }
    schema
  }

  /**
    * Gets the names of all tables registered in this environment.
    *
    * @return A list of the names of all registered tables.
    */
  def listTables(): Array[String] = {
    // TODO list from external meta
    rootSchema.getTableNames.asScala.toArray
  }

  /**
    * Gets the names of all functions registered in this environment.
    */
  def listUserDefinedFunctions(): Array[String] = {
    chainedFunctionCatalog.getSqlOperatorTable.getOperatorList.map(e => e.getName).toArray
  }

  /**
    * Returns the AST of the specified Table API and SQL queries and the execution plan to compute
    * the result of the given [[Table]].
    *
    * @param table The table for which the AST and execution plan will be returned.
    */
  def explain(table: Table): String

  /**
    * Evaluates a SQL query or DML insert on registered tables and retrieves the result as a
    * [[Table]].
    *
    * All tables referenced by the query must be registered in the TableEnvironment.
    * A [[Table]] is automatically registered when its [[toString]] method is called, for example
    * when it is embedded into a String.
    * Hence, SQL queries can directly reference a [[Table]] as follows:
    *
    * {{{
    *   val table: Table = ...
    *   // the table is not registered to the table environment
    *   tEnv.sql(s"SELECT * FROM $table")
    * }}}
    *
    * @deprecated Use sqlQuery() instead.
    * @param query The SQL string to evaluate.
    * @return The result of the query as Table or null of the DML insert operation.
    */
  @Deprecated
  @deprecated("Please use sqlQuery() instead.")
  def sql(query: String): Table = {
    sqlQuery(query)
  }

  /**
    * Evaluates a SQL query on registered tables and retrieves the result as a [[Table]].
    *
    * All tables referenced by the query must be registered in the TableEnvironment.
    * A [[Table]] is automatically registered when its [[toString]] method is called, for example
    * when it is embedded into a String.
    * Hence, SQL queries can directly reference a [[Table]] as follows:
    *
    * {{{
    *   val table: Table = ...
    *   // the table is not registered to the table environment
    *   tEnv.sqlQuery(s"SELECT * FROM $table")
    * }}}
    *
    * @param query The SQL query to evaluate.
    * @return The result of the query as Table
    */
  def sqlQuery(query: String): Table = {
    // parse the sql query
    val parsed = flinkPlanner.parse(query)
    if (null != parsed && parsed.getKind.belongsTo(SqlKind.QUERY)) {
      // validate the sql query
      val validated = flinkPlanner.validate(parsed)
      // transform to a relational tree
      val relational = flinkPlanner.rel(validated)
      new Table(this, LogicalRelNode(relational.project()))
    } else {
      throw new TableException(
        "Unsupported SQL query! sqlQuery() only accepts SQL queries of type " +
          "SELECT, UNION, INTERSECT, EXCEPT, VALUES, and ORDER_BY.")
    }
  }

  /**
    * Returns specific FlinkCostFactory of TableEnvironment's subclass.
    */
  protected def getFlinkCostFactory: FlinkCostFactory

  /**
    * Evaluates a SQL statement such as INSERT, UPDATE or DELETE; or a DDL statement;
    * NOTE: Currently only SQL INSERT statements are supported.
    *
    * All tables referenced by the query must be registered in the TableEnvironment.
    * A [[Table]] is automatically registered when its [[toString]] method is called, for example
    * when it is embedded into a String.
    * Hence, SQL queries can directly reference a [[Table]] as follows:
    *
    * {{{
    *   // register the table sink into which the result is inserted.
    *   tEnv.registerTableSink("sinkTable", fieldNames, fieldsTypes, tableSink)
    *   val sourceTable: Table = ...
    *   // sourceTable is not registered to the table environment
    *   tEnv.sqlUpdate(s"INSERT INTO sinkTable SELECT * FROM $sourceTable")
    * }}}
    *
    * @param stmt The SQL statement to evaluate.
    */
  def sqlUpdate(stmt: String): Unit = {
    sqlUpdate(stmt, this.queryConfig)
  }

  /**
    * Evaluates a SQL statement such as INSERT, UPDATE or DELETE; or a DDL statement;
    * NOTE: Currently only SQL INSERT statements are supported.
    *
    * All tables referenced by the query must be registered in the TableEnvironment.
    * A [[Table]] is automatically registered when its [[toString]] method is called, for example
    * when it is embedded into a String.
    * Hence, SQL queries can directly reference a [[Table]] as follows:
    *
    * {{{
    *   // register the table sink into which the result is inserted.
    *   tEnv.registerTableSink("sinkTable", fieldNames, fieldsTypes, tableSink)
    *   val sourceTable: Table = ...
    *   // sourceTable is not registered to the table environment
    *   tEnv.sqlUpdate(s"INSERT INTO sinkTable SELECT * FROM $sourceTable")
    * }}}
    *
    * @param stmt The SQL statement to evaluate.
    * @param config The [[QueryConfig]] to use.
    */
  def sqlUpdate(stmt: String, config: QueryConfig): Unit = {
    // parse the sql query
    val parsed = flinkPlanner.parse(stmt)
    parsed match {
      case insert: SqlInsert =>
        if (insert.getTargetTable.isInstanceOf[SqlIdentifier] &&
            insert.getTargetTable.asInstanceOf[SqlIdentifier].toString.equals("console") &&
            getTable("console") == null) {
          val source = flinkPlanner.validate(insert.getSource)
          val queryResult = new Table(this, LogicalRelNode(flinkPlanner.rel(source).rel))
          val schema = queryResult.getSchema
          val printTableSink = new PrintTableSink(getConfig.getTimeZone).configure(
            schema.getColumnNames, schema.getTypes.asInstanceOf[Array[DataType]])
          writeToSink(queryResult, printTableSink, queryConfig, "console")
          return
        }
        // validate the insert sql
        val validated = flinkPlanner.validate(insert)
        // transform to a relational tree
        val relational:LogicalTableModify = flinkPlanner.rel(validated).rel
            .asInstanceOf[LogicalTableModify]

        // get query result as Table
        val queryResult = new Table(this, LogicalRelNode(relational.getInput(0)))

        // get name of sink table
        val targetTable = relational.getTable

        // set emit configs
        val emit = insert.getEmit
        if (emit != null && config.isInstanceOf[StreamQueryConfig]) {
          val streamConfig = config.asInstanceOf[StreamQueryConfig]
          if (emit.getBeforeDelayValue >= 0) {
            streamConfig.withEarlyFireInterval(Time.milliseconds(emit.getBeforeDelayValue))
          }
          if (emit.getAfterDelayValue >= 0) {
            streamConfig.withLateFireInterval(Time.milliseconds(emit.getAfterDelayValue))
          }
        }

        // insert query result into sink table
        insertInto(
            queryResult,
            targetTable.unwrap(classOf[schema.Table]),
            StringUtils.join(targetTable.getQualifiedName, ","),
            config)
      case _ =>
        throw new TableException(
          "Unsupported SQL query! sqlUpdate() only accepts SQL statements of type INSERT.")
    }
  }

  /**
    * Writes a [[Table]] to a [[TableSink]].
    *
    * @param table The [[Table]] to write.
    * @param sink The [[TableSink]] to write the [[Table]] to.
    * @param conf The [[QueryConfig]] to use.
    * @tparam T The data type that the [[TableSink]] expects.
    */
  private[flink] def writeToSink[T](
      table: Table,
      sink: TableSink[T],
      conf: QueryConfig,
      sinkName: String = null): Unit

  /**
    * Triggers the program execution.
    */
  def execute(): JobExecutionResult

  /**
    * Triggers the program execution with jobName.
    */
  def execute(jobName: String): JobExecutionResult

  /**
    * Writes the [[Table]] to a [[TableSink]] that was registered under the specified name.
    *
    * @param table The table to write to the TableSink.
    * @param sinkTableName The name of the registered TableSink.
    * @param conf The query configuration to use.
    */
  private[flink] def insertInto(table: Table, sinkTableName: String, conf: QueryConfig): Unit = {

    // check that sink table exists
    if (null == sinkTableName || sinkTableName.isEmpty) {
      throw TableException(TableErrors.INST.sqlInvalidSinkTblName())
    }
    if (!isRegistered(sinkTableName)) {
      throw TableException(TableErrors.INST.sqlTableNotRegistered(sinkTableName))
    }
    val targetTable = getTable(sinkTableName)

    insertInto(table, targetTable, sinkTableName, conf)
  }

  private def insertInto(
      sourceTable: Table,
      targetTable: schema.Table,
      targetTableName: String,
      conf: QueryConfig) = {
    val tableSink = targetTable match {
      case s: CatalogTable => s.tableSink
      case s: TableSinkTable[_] => s.tableSink
      case _ =>
        throw TableException(TableErrors.INST.sqlNotTableSinkError(targetTableName))

    }

    // validate schema of source table and table sink
    val srcFieldTypes = sourceTable.getSchema.getTypes
    val sinkFieldTypes = tableSink.getFieldTypes.map(DataTypes.internal)

    val srcFieldNames = sourceTable.getSchema.getColumnNames
    val sinkFieldNames = tableSink.getFieldNames

    val srcNameTypes = srcFieldNames.zip(srcFieldTypes)
    val sinkNameTypes = sinkFieldNames.zip(sinkFieldTypes)

    def typeMatch(t1: InternalType, t2: InternalType): Boolean = {
      t1 == t2 ||
          (t1.isInstanceOf[DateType] && t2.isInstanceOf[DateType]) ||
          (t1.isInstanceOf[TimestampType] && t2.isInstanceOf[TimestampType])
    }

    if (srcFieldTypes.length != sinkFieldTypes.length) {
      // format table and table sink schema strings
      val srcSchema = srcNameTypes
          .map { case (n, t) => s"$n: ${TypeUtils.getExternalClassForType(t)}" }
          .mkString("[", ", ", "]")

      val sinkSchema = sinkNameTypes
          .map { case (n, t) => s"$n: ${TypeUtils.getExternalClassForType(t)}" }
          .mkString("[", ", ", "]")

      throw ValidationException(
        TableErrors.INST.sqlInsertIntoMismatchedFieldLen(
          targetTableName, srcSchema, sinkSchema))
    } else if (srcFieldTypes.zip(sinkFieldTypes)
        .exists {
          case (_: GenericType[_], _: GenericType[_]) => false
          case (srcF, snkF) => !typeMatch(srcF, snkF)
        }
    ) {
      val diffNameTypes = srcNameTypes.zip(sinkNameTypes)
          .filter {
            case ((_, srcType), (_, sinkType)) => !typeMatch(srcType, sinkType)
          }
      val srcDiffMsg = diffNameTypes
          .map(_._1)
          .map { case (n, t) => s"$n: ${TypeUtils.getExternalClassForType(t)}" }
          .mkString("[", ", ", "]")
      val sinkDiffMsg = diffNameTypes
          .map(_._2)
          .map { case (n, t) => s"$n: ${TypeUtils.getExternalClassForType(t)}" }
          .mkString("[", ", ", "]")

      throw ValidationException(
        TableErrors.INST.sqlInsertIntoMismatchedFieldTypes(
          targetTableName, srcDiffMsg, sinkDiffMsg))
    }

    // emit the table to the configured table sink
    writeToSink(sourceTable, tableSink, conf, targetTableName)
  }

  /**
    * Registers a Calcite [[AbstractTable]] in the TableEnvironment's catalog.
    *
    * @param name The name under which the table will be registered.
    * @param table The table to register in the catalog
    * @throws TableException if another table is registered under the provided name.
    */
  @throws[TableException]
  protected def registerTableInternal(name: String, table: AbstractTable): Unit = {

    if (isRegistered(name)) {
      throw new TableException(s"Table \'$name\' already exists. " +
        s"Please, choose a different name.")
    } else {
      rootSchema.add(name, table)
    }
  }

  /**
    * Checks if the chosen table name is valid.
    *
    * @param name The table name to check.
    */
  protected def checkValidTableName(name: String): Unit = {}

  /**
    * Checks if a table is registered under the given name.
    *
    * @param name The table name to check.
    * @return true, if a table is registered under the name, false otherwise.
    */
  protected[flink] def isRegistered(name: String): Boolean = {
    val memContains: Boolean = rootSchema.getTableNames.contains(name)
    if (!memContains) {
      val schemaPaths = Array(DEFAULT_SCHEMA)
      val schema = getSchema(schemaPaths)
      if (schema != null) {
        return schema.getTable(name) != null
      }
    }
    memContains
  }

  private[flink] def getTable(tablePath: String*): org.apache.calcite.schema.Table = {
    require(tablePath != null && tablePath.nonEmpty, "tablePath must not be null or empty.")
    if (tablePath.length == 1) {
      // First, try to get the table from the memory
      var table = rootSchema.getTable(tablePath.head)

      // Second, try to get the table from the external catalog
      if (null == table) {
        val schemaPaths = Array(DEFAULT_SCHEMA)
        val schema = getSchema(schemaPaths)
        if (schema != null) {
          table = schema.getTable(tablePath.head)
        }
      }
      table
    } else {
      val schemaPaths = tablePath.slice(0, tablePath.length - 1)
      val schema = getSchema(schemaPaths.toArray)
      if (schema != null) {
        val tableName = tablePath(tablePath.length - 1)
        schema.getTable(tableName)
      } else {
        null
      }
    }
  }

  def getExternalCatalog(catalogPaths: Array[String]): ExternalCatalog = {
    val externalCatalog = if (null == catalogPaths || catalogPaths.length == 0) {
      getRegisteredExternalCatalog(DEFAULT_SCHEMA)
    } else {
      val rootCatalog = getRegisteredExternalCatalog(catalogPaths.head)
      val leafCatalog = catalogPaths.slice(1, catalogPaths.length).foldLeft(rootCatalog) {
        case (parentCatalog, name) => parentCatalog.getSubCatalog(name)
      }
      leafCatalog
    }
    externalCatalog
  }

  def registerExternalTable(
      catalogPaths: Array[String],
      tableName: String,
      externalTable: ExternalCatalogTable,
      ignoreIfExists: Boolean) = {

    val externalCatalog = getExternalCatalog(catalogPaths)
    require(
      externalCatalog.isInstanceOf[CrudExternalCatalog],
      "Catalog is not allowed to create table")

    externalCatalog.asInstanceOf[CrudExternalCatalog].createTable(
      tableName, externalTable, ignoreIfExists)
  }

  def registerExternalFunction(
      catalogPaths: Array[String],
      functionName: String,
      className: String,
      ignoreIfExists: Boolean) = {

    val externalCatalog = getExternalCatalog(catalogPaths)
    require(
      externalCatalog.isInstanceOf[CrudExternalCatalog],
      "Catalog is not allowed to create table")

    externalCatalog.asInstanceOf[CrudExternalCatalog].createFunction(
      functionName, className, ignoreIfExists)
  }

  protected def getRowType(name: String): RelDataType = {
    rootSchema.getTable(name).getRowType(typeFactory)
  }

  /** Returns a unique temporary attribute name. */
  private[flink] def createUniqueAttributeName(): String = {
    "TMP_" + attrNameCntr.getAndIncrement()
  }

  /** Returns a unique table name according to the internal naming pattern. */
  private[flink] def createUniqueTableName(): String = {
    var res = tableNamePrefix + tableNameCntr.getAndIncrement()
    while (getTable(res) != null) {
      res = tableNamePrefix + tableNameCntr.getAndIncrement()
    }
    res
  }

  /** Returns the [[FlinkRelBuilder]] of this TableEnvironment. */
  private[flink] def getRelBuilder: FlinkRelBuilder = {
    relBuilder
  }

  /** Returns the Calcite [[org.apache.calcite.plan.RelOptPlanner]] of this TableEnvironment. */
  private[flink] def getPlanner: RelOptPlanner = {
    planner
  }

  /** Returns the [[FlinkTypeFactory]] of this TableEnvironment. */
  private[flink] def getTypeFactory: FlinkTypeFactory = {
    typeFactory
  }

  /** Returns the chained [[FunctionCatalog]]. */
  private[flink] def getFunctionCatalog: FunctionCatalog = {
    chainedFunctionCatalog
  }

  /** Returns the Calcite [[FrameworkConfig]] of this TableEnvironment. */
  private[flink] def getFrameworkConfig: FrameworkConfig = {
    frameworkConfig
  }

  protected def checkRowConverterValid[OUT](
      inputTypeInfo: BaseRowTypeInfo[_],
      relType: RelDataType,
      requestedTypeInfo: TypeInformation[OUT]) : Unit = {

    // validate that at least the field types of physical and logical type match
    // we do that here to make sure that plan translation was correct
    val types = relType.getFieldList map {f => FlinkTypeFactory.toTypeInfo(f.getType)}
    if (inputTypeInfo.getFieldTypes.toList != types) {
      throw TableException(
        s"The field types of physical and logical row types do not match. " +
          s"Physical type is [$relType], Logical type is [$inputTypeInfo]. " +
          s"This is a bug and should not happen. Please file an issue.")
    }

    val fieldTypes = inputTypeInfo.getFieldTypes
    val fieldNames = inputTypeInfo.getFieldNames

    // check for valid type info
    if (!requestedTypeInfo.isInstanceOf[GenericTypeInfo[_]] &&
      requestedTypeInfo.getArity != fieldTypes.length) {
      throw new TableException(
        s"Arity [${fieldTypes.length}] of result [$fieldTypes] does not match " +
          s"the number[${requestedTypeInfo.getArity}] of requested type [$requestedTypeInfo].")
    }

    // check requested types

    def validateFieldType(fieldType: TypeInformation[_]): Unit = fieldType match {
      case _: TimeIndicatorTypeInfo =>
        throw new TableException("The time indicator type is an internal type only.")
      case _ => // ok
    }

    requestedTypeInfo match {
      // POJO type requested
      case pt: PojoTypeInfo[_] =>
        fieldNames.zip(fieldTypes) foreach {
          case (fName, fType) =>
            val pojoIdx = pt.getFieldIndex(fName)
            if (pojoIdx < 0) {
              throw new TableException(s"POJO does not define field name: $fName")
            }
            val requestedTypeInfo = pt.getTypeAt(pojoIdx)
            validateFieldType(requestedTypeInfo)
            if (fType != requestedTypeInfo) {
              throw new TableException(s"Result field '$fName' does not match requested type. " +
                s"Requested: $requestedTypeInfo; Actual: $fType")
            }
        }

      // Tuple/Case class/Row type requested
      case tt: TupleTypeInfoBase[_] =>
        fieldTypes.zipWithIndex foreach {
          case (fieldTypeInfo: GenericTypeInfo[_], i) =>
            val requestedTypeInfo = tt.getTypeAt(i)
            if (!requestedTypeInfo.isInstanceOf[GenericTypeInfo[Object]]) {
              throw new TableException(
                s"Result field '${fieldNames(i)}' does not match requested type. " +
                  s"Requested: $requestedTypeInfo; Actual: $fieldTypeInfo")
            }
          case (fieldTypeInfo, i) =>
            val requestedTypeInfo = tt.getTypeAt(i)
            validateFieldType(requestedTypeInfo)
            if (fieldTypeInfo != requestedTypeInfo) {
              val fieldNames = tt.getFieldNames
              throw new TableException(s"Result field '${fieldNames(i)}' does not match requested" +
                s" type. Requested: $requestedTypeInfo; Actual: $fieldTypeInfo")
            }
        }

      //The result type is BaseRow which is the same to input type, so here don't convert.
      case _: BaseRowTypeInfo[_] =>

      // Atomic type requested
      case at: AtomicType[_] =>
        if (fieldTypes.size != 1) {
          throw new TableException(s"Requested result type is an atomic type but " +
            s"result[$fieldTypes] has more or less than a single field.")
        }
        val requestedTypeInfo = fieldTypes.head
        validateFieldType(requestedTypeInfo)
        if (requestedTypeInfo != at) {
          throw new TableException(s"Result field does not match requested type. " +
            s"Requested: $at; Actual: $requestedTypeInfo")
        }

      case _ =>
        throw new TableException(s"Unsupported result type: $requestedTypeInfo")
    }
  }

  protected def generateRowConverterOperator[IN, OUT](
      ctx: CodeGeneratorContext,
      inputTypeInfo: BaseRowTypeInfo[_],
      relType: RelDataType,
      operatorName: String,
      rowtimeField: Option[Int],
      withChangeFlag: Boolean,
      dataType: DataType)
    : (Option[OneInputSubstituteStreamOperator[IN, OUT]], TypeInformation[OUT])  = {

    val resultType = DataTypes.toTypeInfo(dataType).asInstanceOf[TypeInformation[OUT]]

    //row needs no conversion
    if (resultType.isInstanceOf[BaseRowTypeInfo[_]]
      || (resultType.isInstanceOf[GenericTypeInfo[_]]
      && resultType.getTypeClass == classOf[BaseRow])) {
      return (None, resultType)
    }

    val requestedTypeInfo = if (withChangeFlag) {
      resultType match {
        // Scala tuple
        case t: CaseClassTypeInfo[_]
          if t.getTypeClass == classOf[(_, _)] && t.getTypeAt(0) == Types.BOOLEAN =>
          t.getTypeAt[Any](1)
        // Java tuple
        case t: TupleTypeInfo[_]
          if t.getTypeClass == classOf[JTuple2[_, _]] && t.getTypeAt(0) == Types.BOOLEAN =>
          t.getTypeAt[Any](1)
        case _ => throw new TableException(
          "Don't support " + resultType + " conversion for the retract sink")
      }
    } else {
      resultType
    }

    /**
      * The tpe may been inferred by invoking [[TypeExtractor.createTypeInfo]] based the class of
      * the resulting type. For example, converts the given [[Table]] into an append [[DataStream]].
      * If the class is Row, then the return type only is [[GenericTypeInfo[Row]]. So it should
      * convert to the [[RowTypeInfo]] in order to better serialize performance.
      *
      */
    val convertOutputType = requestedTypeInfo match {
      case gt: GenericTypeInfo[Row] if gt.getTypeClass == classOf[Row] =>
        new RowTypeInfo(
          inputTypeInfo.asInstanceOf[BaseRowTypeInfo[_]].getFieldTypes,
          inputTypeInfo.asInstanceOf[BaseRowTypeInfo[_]].getFieldNames)
      case _ => requestedTypeInfo
    }

    checkRowConverterValid(inputTypeInfo, relType, convertOutputType)

    //update out put type info
    val outputTypeInfo = if (withChangeFlag) {
      resultType match {
        // Scala tuple
        case t: CaseClassTypeInfo[_]
          if t.getTypeClass == classOf[(_, _)] && t.getTypeAt(0) == Types.BOOLEAN =>
          createTuple2TypeInformation(t.getTypeAt(0), convertOutputType)
        // Java tuple
        case t: TupleTypeInfo[_]
          if t.getTypeClass == classOf[JTuple2[_, _]] && t.getTypeAt(0) == Types.BOOLEAN =>
          new TupleTypeInfo(t.getTypeAt(0), convertOutputType)
      }
    } else {
      convertOutputType
    }

    val inputTerm = CodeGeneratorContext.DEFAULT_INPUT1_TERM
    var afterIndexModify = inputTerm
    val fieldIndexProcessCode =
      if (getCompositeTypes(convertOutputType) sameElements inputTypeInfo.getFieldTypes) {
        ""
      } else {
        // field index change (pojo)
        val mapping = convertOutputType match {
          case ct: CompositeType[_] => ct.getFieldNames.map(inputTypeInfo.getFieldIndex)
          case _ => Array(0)
        }

        val resultGenerator = new ExprCodeGenerator(ctx, false, config.getNullCheck).bindInput(
              DataTypes.internal(inputTypeInfo),
              inputTerm,
              inputFieldMapping = Option(mapping))
        val outputBaseRowType = new BaseRowTypeInfo(
          classOf[GenericRow], getCompositeTypes(convertOutputType): _*)
        val conversion = resultGenerator.generateConverterResultExpression(
          DataTypes.internal(outputBaseRowType).asInstanceOf[BaseRowType])
        afterIndexModify = CodeGenUtils.newName("afterIndexModify")
        s"""
           |${conversion.code}
           |${classOf[BaseRow].getCanonicalName} $afterIndexModify = ${conversion.resultTerm};
           |""".stripMargin
      }

    val retractProcessCode = if (!withChangeFlag) {
      generatorCollect(genToExternal(ctx, DataTypes.of(outputTypeInfo), afterIndexModify))
    } else {
      val flagResultTerm =
        s"${classOf[BaseRowUtil].getCanonicalName}.isAccumulateMsg($afterIndexModify)"
      val resultTerm = CodeGenUtils.newName("result")
      val genericRowField = classOf[GenericRow].getCanonicalName
      s"""
          |$genericRowField $resultTerm = new $genericRowField(2);
          |$resultTerm.update(0, $flagResultTerm);
          |$resultTerm.update(1, $afterIndexModify);
          |${generatorCollect(genToExternal(ctx, DataTypes.of(outputTypeInfo), resultTerm))}
          """.stripMargin
    }

    val endInputCode = ""
    val generated = OperatorCodeGenerator.generateOneInputStreamOperator[BaseRow, OUT](
      ctx,
      operatorName,
      s"""
         |$fieldIndexProcessCode
         |$retractProcessCode
         |""".stripMargin,
      endInputCode,
      DataTypes.internal(inputTypeInfo),
      config)
    val substituteStreamOperator = new OneInputSubstituteStreamOperator[IN, OUT](
      generated.name,
      generated.code,
      references = ctx.references)
    (Some(substituteStreamOperator), outputTypeInfo.asInstanceOf[TypeInformation[OUT]])
  }

  def createPartitionTransformation(
      sink: TableSink[_],
      input: StreamTransformation[BaseRow]): StreamTransformation[BaseRow] = {
    sink match {
      case par: DefinedDistribution =>
        val pk = par.getPartitionField()
        if (pk != null) {
          val pkIndex = sink.getFieldNames.indexOf(pk)
          if (pkIndex < 0) {
            throw new TableException("partitionBy field must be in the schema")
          } else {
            PartitionUtils.keyPartition(
              input, input.getOutputType.asInstanceOf[BaseRowTypeInfo[_]], Array(pkIndex))
          }
        } else {
          input
        }
      case _ => input
    }
  }

   /**
    * Reference input fields by name:
    * All fields in the schema definition are referenced by name
    * (and possibly renamed using an alias (as). In this mode, fields can be reordered and
    * projected out. Moreover, we can define proctime and rowtime attributes at arbitrary
    * positions using arbitrary names (except those that exist in the result schema). This mode
    * can be used for any input type, including POJOs.
    *
    * Reference input fields by position:
    * In this mode, fields are simply renamed. Event-time attributes can
    * replace the field on their position in the input data (if it is of correct type) or be
    * appended at the end. Proctime attributes must be appended at the end. This mode can only be
    * used if the input type has a defined field order (tuple, case class, Row) and no of fields
    * references a field of the input type.
    */
  protected def isReferenceByPosition(ct: BaseRowType, fields: Array[Expression]): Boolean = {

    val inputNames = ct.getFieldNames

    // Use the by-position mode if no of the fields exists in the input.
    // This prevents confusing cases like ('f2, 'f0, 'myName) for a Tuple3 where fields are renamed
    // by position but the user might assume reordering instead of renaming.
    fields.forall {
      case UnresolvedFieldReference(name) => !inputNames.contains(name)
      case Alias(_, _, _) => false
      case _ => true
    }
  }

  /**
    * Returns field names and field positions for a given [[TypeInformation]].
    *
    * @param inputType The DataType extract the field names and positions from.
    * @return A tuple of two arrays holding the field names and corresponding field positions.
    */
  protected[flink] def getFieldInfo(inputType: DataType):
  (Array[String], Array[Int]) = {

    (TableEnvironment.getFieldNames(inputType), TableEnvironment.getFieldIndices(inputType))
  }

  /**
    * Returns field names and field positions for a given [[TypeInformation]] and [[Array]] of
    * [[Expression]]. It does not handle time attributes but considers them in indices.
    *
    * @param inputType The [[DataType]] against which the [[Expression]]s are evaluated.
    * @param exprs     The expressions that define the field names.
    * @return A tuple of two arrays holding the field names and corresponding field positions.
    */
  protected[flink] def getFieldInfo[A](
      inputType: DataType,
      exprs: Array[Expression])
    : (Array[String], Array[Int]) = {

    TableEnvironment.validateType(inputType)

    def referenceByName(name: String, ct: BaseRowType): Option[Int] = {
      val inputIdx = ct.getFieldIndex(name)
      if (inputIdx < 0) {
        throw new TableException(s"$name is not a field of type $ct. " +
                s"Expected: ${ct.getFieldNames.mkString(", ")}. " +
            s"Make sure there is no field in physical data type referred " +
            s"if you want to refer field by position.")
      } else {
        Some(inputIdx)
      }
    }

    val indexedNames: Array[(Int, String)] = DataTypes.internal(inputType) match {

      case t: BaseRowType =>

        val isRefByPos = isReferenceByPosition(t, exprs)
        exprs.zipWithIndex flatMap {
          case (UnresolvedFieldReference(name: String), idx) =>
            if (isRefByPos) {
              Some((idx, name))
            } else {
              referenceByName(name, t).map((_, name))
            }
          case (Alias(UnresolvedFieldReference(origName), name: String, _), _) =>
            if (isRefByPos) {
              throw new TableException(
                s"Alias '$name' is not allowed if other fields are referenced by position.")
            } else {
              referenceByName(origName, t).map((_, name))
            }
          case (_: TimeAttribute, _) =>
            None
          case _ => throw new TableException(
            "Field reference expression or alias on field expression expected.")
        }

      case _: InternalType => // atomic or other custom type information
        var referenced = false
        exprs flatMap {
          case _: TimeAttribute =>
            None
          case UnresolvedFieldReference(_) if referenced =>
            // only accept the first field for an atomic type
            throw new TableException("Only the first field can reference an atomic type.")
          case UnresolvedFieldReference(name: String) =>
            referenced = true
            // first field reference is mapped to atomic type
            Some((0, name))
          case _ => throw new TableException(
            "Field reference expression expected.")
        }
    }

    val (fieldIndexes, fieldNames) = indexedNames.unzip

    if (fieldNames.contains("*")) {
      throw new TableException("Field name can not be '*'.")
    }

    (fieldNames, fieldIndexes)
  }

}

/**
  * Object to instantiate a [[TableEnvironment]] depending on the batch or stream execution
  * environment.
  */
object TableEnvironment {

  /**
    * The key mapping query plan in GlobalJobParameters.
    */
  val QUERY_PLAN_KEY = "__query__.__plan__"

  /**
    * Returns a [[BatchTableEnvironment]] for a Java [[JavaStreamExecEnv]].
    *
    * @param executionEnvironment The Java batch ExecutionEnvironment.
    */
  def getBatchTableEnvironment(
      executionEnvironment: JavaStreamExecEnv): JavaBatchTableEnvironment = {
    new JavaBatchTableEnvironment(executionEnvironment, new TableConfig())
  }

  /**
    * Returns a [[BatchTableEnvironment]] for a Java [[JavaStreamExecEnv]] and a given
    * [[TableConfig]].
    *
    * @param executionEnvironment The Java batch ExecutionEnvironment.
    * @param tableConfig          The TableConfig for the new TableEnvironment.
    */
  def getBatchTableEnvironment(
      executionEnvironment: JavaStreamExecEnv,
      tableConfig: TableConfig): JavaBatchTableEnvironment = {
    new JavaBatchTableEnvironment(executionEnvironment, tableConfig)
  }

  /**
    * Returns a [[ScalaBatchTableEnvironment]] for a Scala stream [[ScalaStreamExecEnv]].
    *
    * @param executionEnvironment The Scala StreamExecutionEnvironment.
    */
  def getBatchTableEnvironment(
      executionEnvironment: ScalaStreamExecEnv): ScalaBatchTableEnvironment = {
    new ScalaBatchTableEnvironment(executionEnvironment, new TableConfig())
  }

  /**
    * Returns a [[ScalaBatchTableEnvironment]] for a Scala stream [[ScalaStreamExecEnv]].
    *
    * @param executionEnvironment The Scala StreamExecutionEnvironment.
    * @param tableConfig The TableConfig for the new TableEnvironment.
    */
  def getBatchTableEnvironment(
      executionEnvironment: ScalaStreamExecEnv,
      tableConfig: TableConfig): ScalaBatchTableEnvironment = {

    new ScalaBatchTableEnvironment(executionEnvironment, tableConfig)
  }

  /**
    * Returns a [[JavaStreamTableEnv]] for a Java [[JavaStreamExecEnv]].
    *
    * @param executionEnvironment The Java StreamExecutionEnvironment.
    */
  def getTableEnvironment(executionEnvironment: JavaStreamExecEnv): JavaStreamTableEnv = {
    new JavaStreamTableEnv(executionEnvironment, new TableConfig())
  }

  /**
    * Returns a [[JavaStreamTableEnv]] for a Java [[JavaStreamExecEnv]] and a given [[TableConfig]].
    *
    * @param executionEnvironment The Java StreamExecutionEnvironment.
    * @param tableConfig The TableConfig for the new TableEnvironment.
    */
  def getTableEnvironment(
    executionEnvironment: JavaStreamExecEnv,
    tableConfig: TableConfig): JavaStreamTableEnv = {

    new JavaStreamTableEnv(executionEnvironment, tableConfig)
  }

  /**
    * Returns a [[ScalaStreamTableEnv]] for a Scala stream [[ScalaStreamExecEnv]].
    *
    * @param executionEnvironment The Scala StreamExecutionEnvironment.
    */
  def getTableEnvironment(executionEnvironment: ScalaStreamExecEnv): ScalaStreamTableEnv = {
    new ScalaStreamTableEnv(executionEnvironment, new TableConfig())
  }

  /**
    * Returns a [[ScalaStreamTableEnv]] for a Scala stream [[ScalaStreamExecEnv]].
    *
    * @param executionEnvironment The Scala StreamExecutionEnvironment.
    * @param tableConfig The TableConfig for the new TableEnvironment.
    */
  def getTableEnvironment(
    executionEnvironment: ScalaStreamExecEnv,
    tableConfig: TableConfig): ScalaStreamTableEnv = {

    new ScalaStreamTableEnv(executionEnvironment, tableConfig)
  }

  /**
    * Validate if class represented by the typeInfo is static and globally accessible
    * @param t type to check
    * @throws TableException if type does not meet these criteria
    */
  def validateType(t: DataType): Unit = {
    val clazz = TypeUtils.getExternalClassForType(t)
    if ((clazz.isMemberClass && !Modifier.isStatic(clazz.getModifiers)) ||
      !Modifier.isPublic(clazz.getModifiers) ||
      clazz.getCanonicalName == null) {
      throw TableException(s"Class '$clazz' described in type information '$t' must be " +
        s"static and globally accessible.")
    }
  }

  /**
    * Return rowType of tableSink. [[UpsertStreamTableSink]] and [[RetractStreamTableSink]] should
    * return recordType, others return outputType.
    * @param tableSink
    * @tparam A
    * @return
    */
  def getRowTypeForTableSink[A](tableSink: TableSink[A]): DataType = {
    tableSink match {
      case u: UpsertStreamTableSink[A] => u.getRecordType
      case r: RetractStreamTableSink[A] => r.getRecordType
      case _ => tableSink.getOutputType
    }
  }

   /**
    * Returns field names for a given [[TypeInformation]].
    *
    * @param inputType The DataType extract the field names.
    * @return An array holding the field names
    */
  def getFieldNames(inputType: DataType): Array[String] = {
    validateType(inputType)

    val fieldNames: Array[String] = DataTypes.internal(inputType) match {
      case t: BaseRowType => t.getFieldNames
      case _: InternalType => Array("f0")
    }

    if (fieldNames.contains("*")) {
      throw new TableException("Field name can not be '*'.")
    }

    fieldNames
  }

  /**
    * Returns field indexes for a given [[TypeInformation]].
    *
    * @param inputType The DataType extract the field positions from.
    * @return An array holding the field positions
    */
  def getFieldIndices(inputType: DataType): Array[Int] = {
    getFieldNames(inputType).indices.toArray
  }

  /**
    * Returns field types for a given [[TypeInformation]].
    *
    * @param inputType The DataType to extract field types from.
    * @return An array holding the field types.
    */
  def getFieldTypes(inputType: DataType): Array[InternalType] = {
    validateType(inputType)

    DataTypes.internal(inputType) match {
      case ct: BaseRowType => 0.until(ct.getArity).map(i => ct.getTypeAt(i)).toArray
      case t: InternalType => Array(t)
    }
  }

}
