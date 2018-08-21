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

import java.util
import java.util.{Collections, Properties}

import com.google.common.collect.ImmutableSet
import org.apache.calcite.config.{CalciteConnectionConfigImpl, CalciteConnectionProperty, Lex}
import org.apache.calcite.jdbc.CalciteSchema
import org.apache.calcite.rel.`type`.RelDataTypeFactory
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.tools.{FrameworkConfig, Frameworks}
import org.apache.flink.table.api.{TableConfig, TableSchema}
import org.apache.flink.table.calcite.{FlinkCalciteCatalogReader, FlinkTypeSystem}
import org.apache.flink.table.codegen.ExpressionReducer
import org.apache.flink.table.plan.schema.TableSourceTable
import org.apache.flink.table.plan.stats.{ColumnStats, FlinkStatistic, TableStats}
import org.apache.flink.table.sources.TableSource
import org.apache.flink.table.types.{DataType, DataTypes, InternalType}
import org.apache.flink.table.validate.FunctionCatalog

import scala.collection.JavaConversions._

object MetadataTestUtil {

  def createFrameworkConfig(defaultSchema: SchemaPlus): FrameworkConfig = {
    val sqlParserConfig = SqlParser
      .configBuilder()
      .setLex(Lex.JAVA)
      .build()
    val sqlOperatorTable = FunctionCatalog.withBuiltIns.getSqlOperatorTable
    val config = new TableConfig
    Frameworks
      .newConfigBuilder
      .defaultSchema(defaultSchema)
      .parserConfig(sqlParserConfig)
      .costFactory(new DataSetCostFactory)
      .typeSystem(new FlinkTypeSystem)
      .operatorTable(sqlOperatorTable)
      .executor(new ExpressionReducer(config))
      .build
  }

  def createCatalogReader(
      rootSchema: SchemaPlus,
      typeFactory: RelDataTypeFactory): FlinkCalciteCatalogReader = {
    val prop = new Properties()
    prop.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName, "false")
    val calciteConnConfig = new CalciteConnectionConfigImpl(prop)
    new FlinkCalciteCatalogReader(
      CalciteSchema.from(rootSchema),
      Collections.emptyList(),
      typeFactory,
      calciteConnConfig)
  }

  def createRootSchemaWithCommonTable(): SchemaPlus = {
    val rootSchema = CalciteSchema.createRootSchema(true, false).plus()
    val types = Array(DataTypes.INT, DataTypes.DOUBLE).asInstanceOf[Array[DataType]]
    val tableSchema = new TableSchema(
      Array("id", "score"),
      types.asInstanceOf[Array[InternalType]])
    val colStatsOfT1 = Map[java.lang.String, ColumnStats](
      "id" -> ColumnStats(3L, 1L, 8D, 8, 5, -5),
      "score" -> ColumnStats(5L, 0L, 32D, 32, 6.1D, 0D)
    )
    val ts1 = new TableSource {
      override def getReturnType: DataType =
        DataTypes.createRowType(types, tableSchema.getColumnNames)

      override def getTableStats: TableStats = TableStats(100L, colStatsOfT1)
    }
    rootSchema.add("t1", new TableSourceTable(ts1))

    val colStatsOfBigT1 = Map[java.lang.String, ColumnStats](
      "id" -> ColumnStats(512 * 512 * 128L, 1L, 8D, 8, 5, -5),
      "score" -> ColumnStats(30000L, 0L, 32D, 32, 6.1D, 0D)
    )
    val tsBigT1 = new TableSource {
      override def getReturnType: DataType =
        DataTypes.createRowType(types, tableSchema.getColumnNames)

      override def getTableStats: TableStats = TableStats(512 * 512 * 512L, colStatsOfBigT1)
    }
    rootSchema.add("bigT1", new TableSourceTable(tsBigT1))

    val colStatsOfT2 = Map[java.lang.String, ColumnStats](
      "id" -> ColumnStats(5L, 0L, 8D, 8, 10, 0),
      "score" -> ColumnStats(7L, 0L, 32D, 32, 5.1D, 0D)
    )
    val ts2 = new TableSource {
      override def getReturnType: DataType =
        DataTypes.createRowType(types, tableSchema.getColumnNames)

      override def getTableStats: TableStats = TableStats(50L, colStatsOfT2)
    }
    val uniqueKeysOfT2: util.Set[util.Set[String]] = ImmutableSet.of()
    rootSchema.add("t2",
      new TableSourceTable(ts2, FlinkStatistic.of(uniqueKeysOfT2)))

    val ts3 = new TableSource {
      override def getReturnType: DataType =
        DataTypes.createRowType(types, tableSchema.getColumnNames)

      override def getTableStats: TableStats = TableStats(100L)
    }
    val uniqueKeysOfT3 = ImmutableSet.of(ImmutableSet.of("id"))
    rootSchema.add("t3", new TableSourceTable(ts3, FlinkStatistic.of(uniqueKeysOfT3)))

    val ts4 = new TableSource {
      override def getReturnType: DataType =
        DataTypes.createRowType(types, tableSchema.getColumnNames)

      override def getTableStats: TableStats = TableStats(100L)
    }
    rootSchema.add("t4", new TableSourceTable(ts4))

    val colStatsOfT5 = Map[java.lang.String, ColumnStats](
      "id" -> ColumnStats(100L, 0L, 8D, 8, 100, 1),
      "score" -> ColumnStats(80L, 0L, 32D, 32, 100D, 0D)
    )
    val ts5 = new TableSource {
      override def getReturnType: DataType =
        DataTypes.createRowType(types, tableSchema.getColumnNames)

      override def getTableStats: TableStats = TableStats(100L, colStatsOfT5)
    }
    rootSchema.add("t5", new TableSourceTable(ts5))

    val colStatsOfT6 = Map[java.lang.String, ColumnStats](
      "id" -> ColumnStats(80L, 0L, 8D, 8, 180, 101),
      "score" -> ColumnStats(50L, 0L, 32D, 32, 100D, 0D)
    )
    val ts6 = new TableSource {
      override def getReturnType: DataType =
        DataTypes.createRowType(types, tableSchema.getColumnNames)

      override def getTableStats: TableStats = TableStats(80L, colStatsOfT6)
    }
    rootSchema.add("t6", new TableSourceTable(ts6))

    val ts7 = new TableSource {
      override def getReturnType: DataType = {
        val types = Array[DataType](DataTypes.INT, DataTypes.INT, DataTypes.INT)
        val names = Array("a", "b", "c")
        DataTypes.createRowType(types, names)
      }

      override def getTableStats: TableStats = TableStats(100L)
    }
    val uniqueKeysOfT7 = ImmutableSet.of(ImmutableSet.of("a", "b"), ImmutableSet.of("a"))
    rootSchema.add("t7", new TableSourceTable(ts7, FlinkStatistic.of(uniqueKeysOfT7)))

    val colStatsOfT8 = Map[java.lang.String, ColumnStats](
      "id" -> ColumnStats(80L, 0L, 8D, 8, 180, 101),
      "score" -> ColumnStats(50L, 0L, 32D, 32, 100D, 0D),
      "english_name" -> ColumnStats(0L, 80L, 32D, 32, null, null)
    )
    val ts8 = new TableSource {
      override def getReturnType: DataType =
        DataTypes.createRowType(
          Array[DataType](DataTypes.INT, DataTypes.DOUBLE, DataTypes.STRING),
          Array("id", "score", "english_name"))

      override def getTableStats: TableStats = TableStats(80L, colStatsOfT8)
    }
    rootSchema.add("t8", new TableSourceTable(ts8))

    val tableSchemaOfTStudent = new TableSchema(
      Array("id", "score", "age", "height"),
      Array(
        DataTypes.INT,
        DataTypes.DOUBLE,
        DataTypes.INT,
        DataTypes.DOUBLE))
    val colStatsOfT3 = Map[java.lang.String, ColumnStats](
      "id" -> ColumnStats(50L, 0L, 8D, 8, 10, 0),
      "score" -> ColumnStats(7L, 0L, 32D, 32, 5.1D, 0D),
      "age" -> ColumnStats(25L, 0L, 4D, 4, 46, 0),
      "height" -> ColumnStats(46L, 0L, 32D, 32, 172.1D, 161.0D))
    val tStudent = new TableSource {
      override def getReturnType: DataType =
        DataTypes.createRowType(
          tableSchemaOfTStudent.getTypes.asInstanceOf[Array[DataType]],
          tableSchemaOfTStudent.getColumnNames)

      override def getTableStats: TableStats = TableStats(50L, colStatsOfT3)
    }
    val uniqueKeysOfTStudent = ImmutableSet.of(ImmutableSet.of("id"))
    rootSchema.add("student",
      new TableSourceTable(tStudent, FlinkStatistic.of(uniqueKeysOfTStudent)))

    val timeTableSchema = new TableSchema(
      Array("a", "b", "c", "proctime", "rowtime"),
      Array(
        DataTypes.INT,
        DataTypes.STRING,
        DataTypes.LONG,
        DataTypes.PROCTIME_INDICATOR,
        DataTypes.ROWTIME_INDICATOR))
    val timeSource = new TableSource {
      override def getReturnType: DataType =
        DataTypes.createRowType(
          timeTableSchema.getTypes.asInstanceOf[Array[DataType]],
          timeTableSchema.getColumnNames)

      override def getTableStats: TableStats = TableStats(50L,
        Map[java.lang.String, ColumnStats](
          "a" -> ColumnStats(30L, 0L, 4D, 4, 45, 5),
          "b" -> ColumnStats(5L, 0L, 32D, 32, null, null),
          "c" -> ColumnStats(48L, 0L, 8D, 8, 50, 0)))
    }
    rootSchema.add("temporalTable", new TableSourceTable(timeSource))

    val timeTableSchema1 = new TableSchema(
      Array("a", "b", "c", "proctime", "rowtime"),
      Array(
        DataTypes.LONG,
        DataTypes.STRING,
        DataTypes.INT,
        DataTypes.PROCTIME_INDICATOR,
        DataTypes.ROWTIME_INDICATOR))
    val uniqueKeysOfTimeSource = ImmutableSet.of(ImmutableSet.of("a"))
    val timeSourceWithUniqueKeys = new TableSource {
      override def getReturnType: DataType =
        DataTypes.createRowType(
          timeTableSchema1.getTypes.asInstanceOf[Array[DataType]],
          timeTableSchema1.getColumnNames)

      override def getTableStats: TableStats = TableStats(50L,
        Map[java.lang.String, ColumnStats](
          "a" -> ColumnStats(50L, 0L, 8D, 8, 55, 5),
          "b" -> ColumnStats(5L, 0L, 32D, 32, null, null),
          "c" -> ColumnStats(48L, 0L, 4D, 4, 50, 0)))
    }
    rootSchema.add("temporalTable1",
      new TableSourceTable(timeSourceWithUniqueKeys, FlinkStatistic.of(uniqueKeysOfTimeSource)))

    val bigTimeSource = new TableSource {
      override def getReturnType: DataType =
        DataTypes.createRowType(
          timeTableSchema.getTypes.asInstanceOf[Array[DataType]],
          timeTableSchema.getColumnNames)

      override def getTableStats: TableStats = TableStats(512 * 512 * 512L,
        Map[java.lang.String, ColumnStats](
          "a" -> ColumnStats(512 * 512 * 64L, 0L, 8D, 4, 45, 5),
          "b" -> ColumnStats(2L, 0L, 12D, 32, null, null),
          "c" -> ColumnStats(48L, 0L, 8D, 8, 50, 0)))
    }
    rootSchema.add("bigTemporalTable", new TableSourceTable(bigTimeSource))
    rootSchema
  }
}
