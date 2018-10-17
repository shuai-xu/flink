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

package org.apache.flink.table.sources.csv

import java.util.{Set => JSet}

import com.google.common.collect.ImmutableSet
import org.apache.commons.lang3.StringEscapeUtils
import org.apache.flink.table.annotation.TableType
import org.apache.flink.table.catalog.{ExternalCatalogTable, TableSourceConverter}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

/**
  * The class defines a converter used to convert [[CsvTableSource]] to
  * or from [[ExternalCatalogTable]].
  */
@TableType(value = "csv")
class CsvTableSourceConverter extends TableSourceConverter[CsvTableSource] {

  private val LOG: Logger = LoggerFactory.getLogger(this.getClass)

  private val required: JSet[String] = ImmutableSet.of("path", "fieldDelim", "rowDelim")

  override def requiredProperties: JSet[String] = required

  override def fromExternalCatalogTable(
    externalCatalogTable: ExternalCatalogTable): CsvTableSource = {
    val params = externalCatalogTable.properties.asScala
    val csvTableSourceBuilder = new CsvTableSource.Builder

    params.get("path").foreach(csvTableSourceBuilder.path)
    params.get("fieldDelim").foreach(
      delim => csvTableSourceBuilder.fieldDelimiter(getJavaEscapedDelim(delim)))
    params.get("rowDelim").foreach(
      delim => csvTableSourceBuilder.lineDelimiter(getJavaEscapedDelim(delim)))
    params.get("quoteCharacter").foreach(quoteStr =>
      if (quoteStr.length != 1) {
        throw new IllegalArgumentException("the value of param must only contain one character!")
      } else {
        csvTableSourceBuilder.quoteCharacter(quoteStr.charAt(0))
      }
    )
    params.get("ignoreFirstLine").foreach(ignoreFirstLineStr =>
      if(ignoreFirstLineStr.toBoolean) {
        csvTableSourceBuilder.ignoreFirstLine()
      }
    )
    params.get("ignoreComments").foreach(csvTableSourceBuilder.commentPrefix)
    params.get("lenient").foreach(lenientStr =>
      if(lenientStr.toBoolean) {
        csvTableSourceBuilder.ignoreParseErrors
      }
    )
    params.get("emptyColumnAsNull").foreach(
      v => if(v.toBoolean) {
        csvTableSourceBuilder.enableEmptyColumnAsNull()
      })
    externalCatalogTable.schema.getColumnNames
      .zip(externalCatalogTable.schema.getTypes)
      .foreach(field => csvTableSourceBuilder.field(field._1, field._2))

    csvTableSourceBuilder.build()
  }

  def getJavaEscapedDelim(delim: String): String = {
    val unescapedDelim = StringEscapeUtils.unescapeJava(delim)
    if (delim != null && !(delim == unescapedDelim)) {
      LOG.info(s"Delimiter unescaped from {$delim} to {$unescapedDelim}.")
    }
    unescapedDelim
  }

}
