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

package org.apache.flink.table.sqlgen

import org.apache.flink.api.java.operators.join.JoinType
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.types.DataTypes
import org.apache.flink.table.api.{TableEnvironment, TableException}
import org.apache.flink.table.expressions.{Alias, UnresolvedFieldReference}
import org.apache.flink.table.plan.logical._
import org.apache.flink.table.plan.schema.{TableSourceSinkTable, TemporalDimensionTableSourceTable}
import org.apache.flink.table.sources.TableSource
import org.apache.flink.table.typeutils.TypeUtils
import org.apache.flink.table.util.ComplexDimTVF

import scala.collection.mutable

class SqlGenVisitor(tableEnv: TableEnvironment)
  extends LogicalNodeVisitor[(String, String, Boolean)] {

  val sqlMap = new java.util.IdentityHashMap[LogicalNode, (String, String, Boolean)]()

  val nestedTableMap = mutable.Map[String, TableSource]()

  val exprVisitor = new ExprGenVisitor(tableEnv)

  private def getChildSql(node: LogicalNode): (String, String, Boolean) = {
    var data = sqlMap.get(node)
    if (data == null) {
      data = this.visit(node)
      sqlMap.put(node, data)
    }
    data
  }

  def visit(logicalNode: LogicalNode): (String, String, Boolean) = {
    logicalNode match {
      case visitable: LogicalNodeVisitable => visitable.accept(this)
      case _ => throw new TableException(s"${logicalNode} is not Visitable")
    }
  }


  def visit(project: Project) = {
    val childSql = getChildSql(project.child)

    val viewName = SqlGenUtil.getNextView()
    val sql =
      s"""
         |CREATE VIEW ${viewName} AS
         |SELECT
         |  ${project.projectList.map(_.accept(exprVisitor)).mkString(", ")}
         |FROM ${childSql._1}
         |;
       """.stripMargin

    sqlMap.put(project, (viewName, sql, true))
    (viewName, sql, true)
  }

  override def visit(alias: AliasNode) = {
    val childSql = getChildSql(alias.child)

    val viewName = SqlGenUtil.getNextView()
    val names = alias.aliasList.map(_.asInstanceOf[UnresolvedFieldReference].name)
    val input = alias.child.output
    val projectList = names.zip(input).map { case (name, attr) =>
      Alias(attr, name)} ++ input.drop(names.length)
    val sql =
      s"""
         |CREATE VIEW ${viewName} AS
         |SELECT
         |  ${projectList.map(_.accept(exprVisitor)).mkString(",  ")}
         |FROM ${childSql._1}
         |;
       """.stripMargin

    sqlMap.put(alias, (viewName, sql, true))
    (viewName, sql, true)
  }

  override def visit(filter: Filter) = {
    val childSql = getChildSql(filter.child)

    val viewName = SqlGenUtil.getNextView()
    val input = filter.child.output

    val sql =
      s"""
         |CREATE VIEW ${viewName} AS
         |SELECT
         |  ${input.map(_.accept(exprVisitor)).mkString(", ")}
         |FROM ${childSql._1}
         |WHERE ${filter.condition.accept(exprVisitor)}
         |;
       """.stripMargin

    sqlMap.put(filter, (viewName, sql, true))
    (viewName, sql, true)
  }

  override def visit(aggregate: Aggregate) = {
    val childSql = getChildSql(aggregate.child)

    val viewName = SqlGenUtil.getNextView()
    val sql =
      s"""
         |CREATE VIEW ${viewName} AS
         |SELECT
         |  ${(aggregate.groupingExpressions ++ aggregate.aggregateExpressions).map(_.accept
      (exprVisitor)).mkString(", ")}
         |FROM ${childSql._1}
         |GROUP BY ${aggregate.groupingExpressions.map(_.accept(exprVisitor)).mkString(", ")}
         |;
       """.stripMargin

    sqlMap.put(aggregate, (viewName, sql, true))
    (viewName, sql, true)
  }

  override def visit(union: Union) = {
    val childSqlLeft = getChildSql(union.left)
    val childSqlRight = getChildSql(union.right)
    val viewName = SqlGenUtil.getNextView()
    val sql =
      s"""
         |CREATE VIEW ${viewName} AS
         |SELECT
         |  ${union.left.output.map(_.accept(exprVisitor)).mkString(", ")}
         |FROM ${childSqlLeft._1}
         |${if(union.all) "UNION ALL" else "UNION"}
         |SELECT
         |  ${union.right.output.map(_.accept(exprVisitor)).mkString(", ")}
         |FROM ${childSqlRight._1}
         |;
       """.stripMargin

    sqlMap.put(union, (viewName, sql, true))
    (viewName, sql, true)
  }

  override def visit(join: Join) = {
    val childSqlLeft = getChildSql(join.left)
    val childSqlRight = getChildSql(join.right)
    val viewName = SqlGenUtil.getNextView()
    val allFields = join.left.output
                    .map(f => s"${f.accept(exprVisitor)}") ++
      join.right.output.map(f => s"${f.accept(exprVisitor)}")
    val joinType = if (join.joinType == JoinType.LEFT_OUTER) "LEFT JOIN" else "JOIN"
    val joinPost = if (!childSqlRight._3) "ON TRUE" else ""
    val joinCondition = if (join.condition.isDefined && childSqlRight._3) {
      s"ON ${join.condition.get.accept(exprVisitor)}\n;"
    } else {
      ";"
    }
    val joinContent = if (childSqlRight._1.indexOf("ComplexDimTVFParameter") >= 0) {
      val pos = childSqlRight._1.indexOf("_")
      var tmpExpr = childSqlRight._1.substring(0, pos)
      val leftFieldName = SqlGenUtil.getSideFieldName(join, left = true)
      tmpExpr = tmpExpr.replace("ComplexDimTVFParameter", s"`$leftFieldName`")

      val rightFieldName = SqlGenUtil.getSideFieldName(join, left = false)
      val tableSource = nestedTableMap(childSqlRight._1)

      val dimFunc = new ComplexDimTVF(tableSource, rightFieldName)
      val (funcId, functionContent, comment) = SqlGenUtil.getFunctionIdAndContent(dimFunc)

      exprVisitor.functionMap += funcId -> (functionContent, comment)

      tmpExpr = tmpExpr.replace("ComplexDimTVFFunction", funcId)

      // TODO correct args for getResultType
      val outputNames = TypeUtils.createTypeInfoFromDataType(
        dimFunc.getResultType(Array[AnyRef](), Array[Class[_]]()))
          .asInstanceOf[RowTypeInfo]
          .getFieldNames.map(f => s"`$f`").mkString(", ")

      tmpExpr = tmpExpr.replace("ComplexDimTVFReturn", outputNames)

      tmpExpr
    } else {
      childSqlRight._1
    }

    val sql =
      s"""
         |CREATE VIEW ${viewName} AS
         |SELECT
         |  ${allFields.mkString(", ")}
         |FROM ${childSqlLeft._1}
         |${joinType} ${joinContent} ${joinPost}
         |${joinCondition}
       """.stripMargin

    sqlMap.put(join, (viewName, sql, true))
    (viewName, sql, true)
  }

  override def visit(catalog: CatalogNode) = {
    val source = tableEnv.getTable(catalog.tablePath.head)
    val tableSource = source match {
      case Some(table) => table match {
        case tableSourceSinkTable: TableSourceSinkTable[_] =>
          tableSourceSinkTable.tableSourceTable.get.tableSource
        case temporalDimensionTableSourceTable: TemporalDimensionTableSourceTable[_] =>
          temporalDimensionTableSourceTable.tableSource
      }
      case _ => throw new TableException("can't find table source")
    }

    var tableName = SqlGenUtil.getNextTable(catalog.tablePath.head)
    val nestedTable: Boolean = SqlGenUtil.isNestedTable(tableSource)
    val dimTable: Boolean = tableSource.isInstanceOf[ConnectorDimSource[_]]
    var sql = ""
    if (!nestedTable) {
      val properties: Map[String, String] = SqlGenUtil.getTableProperties(tableSource)
      tableSource match {
        case _: ConnectorSource[_] | _: ConnectorDimSource[_] =>
        case _ => throw
          new RuntimeException("Source must be instance of ConnectorSource or ConnectorDimSource")
      }
      val tableSchema = tableSource.getTableSchema.getColumns
        .map {column => (column.name, SqlGenUtil.getTypeName(column.internalType))}

      sql = if (!dimTable) {
        val proctimeAttr = SqlGenUtil.getProcTimeAttr(tableSource)
        val schema = if (proctimeAttr.isEmpty) {
          tableSchema.map
          {
            case (fieldName, fieldType) => s"`$fieldName` $fieldType"
          }.toSeq
        } else {
          tableSchema.map {
            case (fieldName, _) if fieldName == proctimeAttr.get
              => s"`$fieldName` AS PROCTIME"
            case (fieldName, fieldType)
              => s"`$fieldName` $fieldType"
          }.toSeq
        }
        s"""
           |CREATE TABLE ${tableName} (
           |  ${schema.mkString(",\n  ")}
           |) with (
           |  ${properties.map(p => s"${p._1} = '${p._2}'").mkString(",\n  ")}
           |)
           |;
         """.stripMargin
      } else {
        val primaryKeys = SqlGenUtil.getPrimaryKeys(tableSource)
        if (primaryKeys == null || primaryKeys.length == 0) {
          throw new RuntimeException("DimSource must have primaryKeys")
        }
        s"""
           |CREATE TABLE ${tableName} (
           |  ${tableSchema.map(e => s"`${e._1}` ${e._2}").mkString(",\n  ")},
           |  PRIMARY KEY (${primaryKeys.map(k => s"`$k`").mkString(", ")}),
           |  PERIOD FOR SYSTEM_TIME
           |) with (
           |  ${properties.map(p => s"${p._1} = '${p._2}'").mkString(",\n  ")}
           |)
           |;
         """.stripMargin
      }
    }

    var show = true
    if (dimTable) {
      tableName = s"$tableName FOR SYSTEM_TIME AS OF PROCTIME()"
    } else if (nestedTable) {
      tableName = SqlGenUtil.getNestedJoinExpr(tableSource, this)
      show = false
    }

    sqlMap.put(catalog, (tableName, sql, show))
    (tableName, sql, show)
  }

  override def visit(windowAggregate: WindowAggregate) = {
    val childSql = getChildSql(windowAggregate.child)
    val viewName = SqlGenUtil.getNextView()

    val outExprs: Seq[String] = if (windowAggregate.propertyExpressions.isEmpty) {
      windowAggregate.aggregateExpressions.map(_.accept(exprVisitor))
    } else {
      (windowAggregate.aggregateExpressions.map(_.accept(exprVisitor)) ++
        SqlGenUtil.resolveWindowProperties(windowAggregate, exprVisitor))
    }

    val groupBy: Seq[String] = if(windowAggregate.groupingExpressions.isEmpty) {
      Seq(windowAggregate.window.accept(exprVisitor))
    } else {
      (Seq(windowAggregate.window.accept(exprVisitor))
        ++ (windowAggregate.groupingExpressions.map(_.accept(exprVisitor))))
    }

    val sql =
      s"""
         |CREATE VIEW ${viewName} AS
         |SELECT
         |  ${outExprs.mkString(", ")}
         |FROM ${childSql._1}
         |GROUP BY ${groupBy.mkString(", ")}
         |;
       """.stripMargin
    sqlMap.put(windowAggregate, (viewName, sql, true))
    (viewName, sql, true)
  }

  override def visit(logicalTableFunctionCall: LogicalTableFunctionCall) = {
    val (funcId, content, comment) =
      SqlGenUtil.getFunctionIdAndContent(logicalTableFunctionCall.tableFunction)
    if (!exprVisitor.functionMap.contains(funcId)) {
      exprVisitor.functionMap += funcId -> (content, comment)
    }
    val sql = s"LATERAL TABLE(${funcId}" +
      s"(${logicalTableFunctionCall.parameters.map(_.accept(exprVisitor)).mkString(", ")})) " +
      s"AS T${SqlGenUtil.getNextCounter()}(${logicalTableFunctionCall.output.map(_.accept
      (exprVisitor)).mkString(", ")})"

    sqlMap.put(logicalTableFunctionCall, (sql, "", false))
    (sql, "", false)
  }

  override def visit(sinkNode: SinkNode) = {
    val properties: Map[String, String] = SqlGenUtil.getTableProperties(sinkNode.sink)
    val tableSchema = sinkNode.sink.getFieldNames
                      .zip(sinkNode.sink.getFieldTypes)
                      .map(x => x._1 -> SqlGenUtil.getTypeName(DataTypes.internal(x._2)))
    val childSql = getChildSql(sinkNode.child)
    val tableName = SqlGenUtil.getNextTable("outputTable")
    var schemaList = tableSchema.map(e => s"`${e._1}` ${e._2}").toList

    val primaryKeys = SqlGenUtil.getPrimaryKeys(sinkNode.sink)
    if (primaryKeys != null && primaryKeys.length > 0) {
      val primaryKeyExpr = s"PRIMARY KEY(${primaryKeys.map(x => s"`$x`").mkString(",")})"
      schemaList = schemaList ::: List(primaryKeyExpr)
    }

    val sql =
      s"""
         |CREATE TABLE ${tableName} (
         |  ${schemaList.mkString(",\n  ")}
         |) with (
         |  ${properties.map(p => s"${p._1} = '${p._2}'").mkString(",\n  ")}
         |)
         |;
         |
         |INSERT INTO ${tableName}
         |SELECT
         |  ${sinkNode.child.output.map(_.accept(exprVisitor)).mkString(", ")}
         |FROM ${childSql._1}
         |;
       """.stripMargin
    sqlMap.put(sinkNode, ("", sql, true))
    ("", sql, true)
  }

}
