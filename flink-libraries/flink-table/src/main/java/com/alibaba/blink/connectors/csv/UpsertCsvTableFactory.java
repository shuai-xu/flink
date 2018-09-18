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

package com.alibaba.blink.connectors.csv;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.DimensionTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import com.alibaba.blink.exceptions.NotEnoughParamsException;
import com.alibaba.blink.table.api.RichTableSchema;
import com.alibaba.blink.table.api.TableFactory;
import com.alibaba.blink.table.api.TableProperties;
import com.alibaba.blink.table.examples.UpsertCsvSQLTableSink;

import java.util.TimeZone;

import scala.Option;
import scala.Some;

/**
 * Upsert csv table factory.
 */
public class UpsertCsvTableFactory implements TableFactory {

	@Override
	public TableSource createTableSource(String tableName, RichTableSchema schema, TableProperties properties) {
		throw new UnsupportedOperationException("UPSERTCSV 作为读插件暂不支持.");
	}

	@Override
	public DimensionTableSource createDimensionTableSource(String tableName, RichTableSchema schema, TableProperties properties) {
		throw new UnsupportedOperationException("UPSERTCSV 作为维表插件暂不支持.");
	}

	@Override
	public TableSink createTableSink(String tableName, RichTableSchema schema, TableProperties properties) {
		String path = properties.getString(CsvOptions.PATH);
		if (StringUtils.isNullOrWhitespaceOnly(path)) {
			throw new NotEnoughParamsException(CsvOptions.PARAMS_HELP_MSG);
		}
		String fieldDelim = CsvTableFactory.getJavaEscapedDelim(properties.getString(CsvOptions.OPTIONAL_FIELD_DELIM));
		String lineDelim = CsvTableFactory.getJavaEscapedDelim(properties.getString(CsvOptions.OPTIONAL_LINE_DELIM));
		String quoteCharacter = CsvTableFactory.getJavaEscapedDelim(properties.getString(CsvOptions.OPTIONAL_QUOTE_CHARACTER));
		if (quoteCharacter != null) {
			Preconditions.checkArgument(
					quoteCharacter.length() == 1,
					"quote character should be a single character, " + quoteCharacter + " found.");
		}
		int parallelism = properties.getInteger(CsvOptions.PARALLELISM, -1);
		Option numFiles = parallelism == -1 ? Option.apply(null) : new Some(parallelism);
		boolean writeModeFlag = properties.getBoolean(CsvOptions.OPTIONAL_OVER_RIDE_MODE);
		FileSystem.WriteMode writeMode = writeModeFlag ?
				FileSystem.WriteMode.OVERWRITE :
				FileSystem.WriteMode.NO_OVERWRITE;

		String timezone = properties.getString(CsvOptions.TIME_ZONE_KEY);
		TimeZone tz = (timezone == null) ? TimeZone.getTimeZone("UTC") : TimeZone.getTimeZone(timezone);

		return new UpsertCsvSQLTableSink(
			path,
			schema.getColumnTypes(),
			new Some(schema.getColumnNames()),
			new Some(fieldDelim),
			new Some(lineDelim),
			new Some(quoteCharacter),
			numFiles,
			new Some(writeMode),
			Option.apply(null),
			new Some(tz));

	}
}
