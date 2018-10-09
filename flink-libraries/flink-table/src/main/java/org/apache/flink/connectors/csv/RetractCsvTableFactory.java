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

package org.apache.flink.connectors.csv;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.exceptions.NotEnoughParamsException;
import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.api.TableFactory;
import org.apache.flink.table.api.TableProperties;
import org.apache.flink.table.examples.RetractCsvSQLTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.DimensionTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.util.StringUtils;

import java.util.TimeZone;

import scala.Option;
import scala.Some;

/**
 * Retract csv table factory.
 */
public class RetractCsvTableFactory implements TableFactory {

	private ClassLoader classLoader;

	@Override
	public TableSource createTableSource(
		String s,
		RichTableSchema richTableSchema,
		TableProperties tableProperties) {
		throw new UnsupportedOperationException("RETRACTCSV 作为读插件暂不支持.");
	}

	@Override
	public DimensionTableSource<?> createDimensionTableSource(
		String s, RichTableSchema richTableSchema, TableProperties tableProperties) {
		throw new UnsupportedOperationException("RETRACT 作为维表插件暂不支持.");
	}

	@Override
	public TableSink<?> createTableSink(String s, RichTableSchema richTableSchema,
										TableProperties tableProperties) {
		String path = tableProperties.getString(CsvOptions.PATH);
		if (StringUtils.isNullOrWhitespaceOnly(path)) {
			throw new NotEnoughParamsException(CsvOptions.PARAMS_HELP_MSG);
		}
		String fieldDelim = tableProperties.getString(CsvOptions.OPTIONAL_FIELD_DELIM);
		String recordDelim = tableProperties.getString(CsvOptions.OPTIONAL_LINE_DELIM);
		String quoteCharacter = tableProperties.getString(CsvOptions.OPTIONAL_QUOTE_CHARACTER);

		int parallelism = tableProperties.getInteger(CsvOptions.PARALLELISM, -1);
		Option numFiles = parallelism == -1 ? Option.apply(null) : new Some(parallelism);

		boolean writeModeFlag = tableProperties.getBoolean(CsvOptions.OPTIONAL_OVER_RIDE_MODE);
		FileSystem.WriteMode writeMode = writeModeFlag ?
				FileSystem.WriteMode.OVERWRITE :
				FileSystem.WriteMode.NO_OVERWRITE;

		String timezone = tableProperties.getString(CsvOptions.TIME_ZONE_KEY);
		TimeZone tz = (timezone == null) ? TimeZone.getTimeZone("UTC") : TimeZone.getTimeZone(timezone);

		return new RetractCsvSQLTableSink(
				path,
				richTableSchema.getColumnTypes(),
				new Some(richTableSchema.getColumnNames()),
				new Some(fieldDelim),
				new Some(recordDelim),
				Option.apply(quoteCharacter),
				numFiles,
				new Some(writeMode),
				Option.apply(null),
				new Some(tz));

	}

	@Override
	public void setClassLoader(ClassLoader classLoader) {
		this.classLoader = classLoader;
	}
}
