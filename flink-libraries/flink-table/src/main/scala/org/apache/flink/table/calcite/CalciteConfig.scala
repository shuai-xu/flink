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

package org.apache.flink.table.calcite

import java.util.Properties

import org.apache.calcite.config.{CalciteConnectionConfig, CalciteConnectionConfigImpl, CalciteConnectionProperty}
import org.apache.calcite.sql.SqlOperatorTable
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.sql.util.ChainedSqlOperatorTable
import org.apache.calcite.sql2rel.SqlToRelConverter
import org.apache.flink.table.plan.optimize._
import org.apache.flink.util.Preconditions

/**
  * Builder for creating a Calcite configuration.
  */
class CalciteConfigBuilder {

  /**
    * Defines the optimize programs for batch exec table plan.
    */
  private var batchExecPrograms = FlinkBatchExecPrograms.buildPrograms()

  /**
    * Defines the optimize programs for stream table plan.
    */
  private var streamPrograms = FlinkStreamPrograms.buildPrograms()

  /**
    * Defines the SQL operator tables.
    */
  private var replaceOperatorTable: Boolean = false
  private var operatorTables: List[SqlOperatorTable] = Nil

  /**
    * Gets batch exec table optimize programs.
    *
    * @return batch exec table optimize programs instance.
    */
  def getBatchExecPrograms: FlinkChainedPrograms[BatchOptimizeContext] = batchExecPrograms

  /**
    * Replaces the built-in batch exec table optimize programs.
    */
  def replaceBatchExecPrograms(
      programs: FlinkChainedPrograms[BatchOptimizeContext])
  : CalciteConfigBuilder = {
    batchExecPrograms = Preconditions.checkNotNull(programs)
    this
  }

  /**
    * Gets stream table optimize programs.
    *
    * @return stream table optimize programs instance.
    */
  def getStreamPrograms: FlinkChainedPrograms[StreamOptimizeContext] = streamPrograms

  /**
    * Replaces the built-in stream table optimize programs.
    */
  def replaceStreamPrograms(
    programs: FlinkChainedPrograms[StreamOptimizeContext])
  : CalciteConfigBuilder = {
    streamPrograms = Preconditions.checkNotNull(programs)
    this
  }

  /**
    * Defines a SQL parser configuration.
    */
  private var replaceSqlParserConfig: Option[SqlParser.Config] = None

  /**
    * Defines a configuration for SqlToRelConverter.
    */
  private var replaceSqlToRelConverterConfig: Option[SqlToRelConverter.Config] = None

  /**
    * Replaces the built-in SQL operator table with the given table.
    */
  def replaceSqlOperatorTable(replaceSqlOperatorTable: SqlOperatorTable): CalciteConfigBuilder = {
    Preconditions.checkNotNull(replaceSqlOperatorTable)
    operatorTables = List(replaceSqlOperatorTable)
    replaceOperatorTable = true
    this
  }

  /**
    * Appends the given table to the built-in SQL operator table.
    */
  def addSqlOperatorTable(addedSqlOperatorTable: SqlOperatorTable): CalciteConfigBuilder = {
    Preconditions.checkNotNull(addedSqlOperatorTable)
    this.operatorTables = addedSqlOperatorTable :: this.operatorTables
    this
  }

  /**
    * Replaces the built-in SQL parser configuration with the given configuration.
    */
  def replaceSqlParserConfig(sqlParserConfig: SqlParser.Config): CalciteConfigBuilder = {
    Preconditions.checkNotNull(sqlParserConfig)
    replaceSqlParserConfig = Some(sqlParserConfig)
    this
  }

  def replaceSqlToRelConverterConfig(config: SqlToRelConverter.Config): CalciteConfigBuilder = {
    Preconditions.checkNotNull(config)
    replaceSqlToRelConverterConfig = Some(config)
    this
  }

  private class CalciteConfigImpl(
      val getBatchExecPrograms: FlinkChainedPrograms[BatchOptimizeContext],
      val getStreamPrograms: FlinkChainedPrograms[StreamOptimizeContext],
      val getSqlOperatorTable: Option[SqlOperatorTable],
      val replacesSqlOperatorTable: Boolean,
      val getSqlParserConfig: Option[SqlParser.Config],
      val getSqlToRelConverterConfig: Option[SqlToRelConverter.Config])
    extends CalciteConfig

  /**
    * Builds a new [[CalciteConfig]].
    */
  def build(): CalciteConfig = new CalciteConfigImpl(
    getBatchExecPrograms,
    getStreamPrograms,
    operatorTables match {
      case Nil => None
      case h :: Nil => Some(h)
      case _ =>
        // chain operator tables
        Some(operatorTables.reduce((x, y) => ChainedSqlOperatorTable.of(x, y)))
    },
    this.replaceOperatorTable,
    replaceSqlParserConfig,
    replaceSqlToRelConverterConfig)
}

/**
  * Calcite configuration for defining a custom Calcite configuration for Table and SQL API.
  */
trait CalciteConfig {

  /**
    * Returns batch exec table optimize programs.
    */
  def getBatchExecPrograms: FlinkChainedPrograms[BatchOptimizeContext]

  /**
    * Returns stream table optimize programs.
    */
  def getStreamPrograms: FlinkChainedPrograms[StreamOptimizeContext]

  /**
    * Returns whether this configuration replaces the built-in SQL operator table.
    */
  def replacesSqlOperatorTable: Boolean

  /**
    * Returns a custom SQL operator table.
    */
  def getSqlOperatorTable: Option[SqlOperatorTable]

  /**
    * Returns a custom SQL parser configuration.
    */
  def getSqlParserConfig: Option[SqlParser.Config]

  /**
    * Returns a custom configuration for SqlToRelConverter.
    */
  def getSqlToRelConverterConfig: Option[SqlToRelConverter.Config]
}

object CalciteConfig {

  val DEFAULT: CalciteConfig = createBuilder().build()

  /**
    * Creates a new builder for constructing a [[CalciteConfig]].
    */
  def createBuilder(): CalciteConfigBuilder = {
    new CalciteConfigBuilder
  }

  def connectionConfig(parserConfig : SqlParser.Config): CalciteConnectionConfig = {
    val prop = new Properties()
    prop.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName,
      String.valueOf(parserConfig.caseSensitive))
    new CalciteConnectionConfigImpl(prop)
  }
}
