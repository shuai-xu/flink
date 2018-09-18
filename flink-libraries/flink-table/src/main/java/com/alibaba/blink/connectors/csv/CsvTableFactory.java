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
import org.apache.flink.table.errorcode.TableErrors;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.DimensionTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.sources.csv.CsvTableSource;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import com.alibaba.blink.exceptions.NotEnoughParamsException;
import com.alibaba.blink.table.api.RichTableSchema;
import com.alibaba.blink.table.api.TableFactory;
import com.alibaba.blink.table.api.TableProperties;
import com.alibaba.blink.table.examples.CsvSQLTableSink;
import org.apache.commons.lang3.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TimeZone;

import scala.Option;
import scala.Some;

/**
 * csv table factory.
 */
public class CsvTableFactory implements TableFactory {

	private static final Logger LOG = LoggerFactory.getLogger(CsvTableFactory.class);

	private ClassLoader classLoader;

	@Override
	public TableSource createTableSource(
		String tableName,
		RichTableSchema schema,
		TableProperties properties) {

		String path = properties.getString(CsvOptions.PATH);
		if (StringUtils.isNullOrWhitespaceOnly(path)) {
			throw new NotEnoughParamsException(CsvOptions.PARAMS_HELP_MSG);
		}

		String fieldDelim = getJavaEscapedDelim(properties.getString(CsvOptions.OPTIONAL_FIELD_DELIM));
		String lineDelim = getJavaEscapedDelim(properties.getString(CsvOptions.OPTIONAL_LINE_DELIM));
		String charset = properties.getString(CsvOptions.OPTIONAL_CHARSET);
		boolean emptyColumnAsNull = properties.getBoolean(CsvOptions.EMPTY_COLUMN_AS_NULL);
		String timezone = properties.getString(CsvOptions.TIME_ZONE_KEY);
		TimeZone tz = (timezone == null) ? TimeZone.getTimeZone("UTC") : TimeZone.getTimeZone(timezone);

		CsvTableSource.Builder builder = CsvTableSource.builder()
													.path(path)
													.fieldDelimiter(fieldDelim)
													.lineDelimiter(lineDelim)
													.charset(charset)
													.fields(schema.getColumnNames(), schema.getColumnTypes(), schema.getNullables())
													.timezone(tz)
													.setNestedFileEnumerate(properties.getBoolean(CsvOptions.OPTIONAL_ENUMERATE_NESTED_FILES));

		if (emptyColumnAsNull) {
			builder.enableEmptyColumnAsNull();
		}

		String quoteCharacter = getJavaEscapedDelim(properties.getString(CsvOptions.OPTIONAL_QUOTE_CHARACTER));
		if (quoteCharacter != null) {
			Preconditions.checkArgument(
					quoteCharacter.length() == 1,
					"quote character should be a single character, " + quoteCharacter + " found.");
			builder.quoteCharacter(quoteCharacter.charAt(0));
		}

		boolean firstLineAsHeader = properties.getBoolean(CsvOptions.OPTIONAL_FIRST_LINE_AS_HEADER);
		if (firstLineAsHeader) {
			builder.ignoreFirstLine();
		}
		return builder.build();
	}

	@Override
	public DimensionTableSource createDimensionTableSource(
			String tableName,
			RichTableSchema schema,
			TableProperties properties) {
		String path = properties.getString(CsvOptions.PATH);
		if (StringUtils.isNullOrWhitespaceOnly(path)) {
			throw new NotEnoughParamsException(CsvOptions.PARAMS_HELP_MSG);
		}
		if (schema.deduceAllIndexes().isEmpty()) {
			throw new RuntimeException(TableErrors.INST.sqlDimTableRequiresIndex());
		}

		String fieldDelim = getJavaEscapedDelim(properties.getString(CsvOptions.OPTIONAL_FIELD_DELIM));
		String charset = properties.getString(CsvOptions.OPTIONAL_CHARSET);
		boolean emptyColumnAsNull = properties.getBoolean(CsvOptions.EMPTY_COLUMN_AS_NULL);

		CsvDimTable csvDimTable = new CsvDimTable(schema, properties, path, schema.getColumnNames(), schema
				.getColumnTypes(), emptyColumnAsNull);

		String lineDelim = getJavaEscapedDelim(properties.getString(CsvOptions.OPTIONAL_LINE_DELIM));
		csvDimTable.setFieldDelim(fieldDelim).setCharsetName(charset).setRowDelim(lineDelim)
			.setNestedFileEnumerate(properties.getBoolean(CsvOptions.OPTIONAL_ENUMERATE_NESTED_FILES));

		String quoteCharacter = getJavaEscapedDelim(properties.getString(CsvOptions.OPTIONAL_QUOTE_CHARACTER));
		if (quoteCharacter != null) {
			Preconditions.checkArgument(
					quoteCharacter.length() == 1,
					"quote character should be a single character, " + quoteCharacter + " found.");
			csvDimTable.setQuoteCharacter(quoteCharacter.charAt(0));
		}

		boolean ignoreFirstLine = properties.getBoolean(CsvOptions.OPTIONAL_FIRST_LINE_AS_HEADER);
		csvDimTable.setIgnoreFirstLine(ignoreFirstLine);

		String timezone = properties.getString(CsvOptions.TIME_ZONE_KEY);
		TimeZone tz = (timezone == null) ? TimeZone.getTimeZone("UTC") : TimeZone.getTimeZone(timezone);
		csvDimTable.setTimezone(tz);

		return csvDimTable;
	}

	@Override
	public TableSink createTableSink(String tableName, RichTableSchema schema, TableProperties properties) {
		String path = properties.getString(CsvOptions.PATH);
		if (StringUtils.isNullOrWhitespaceOnly(path)) {
			throw new NotEnoughParamsException(CsvOptions.PARAMS_HELP_MSG);
		}
		boolean writeModeFlag = properties.getBoolean(CsvOptions.OPTIONAL_OVER_RIDE_MODE);
		FileSystem.WriteMode writeMode = writeModeFlag ?
				FileSystem.WriteMode.OVERWRITE :
				FileSystem.WriteMode.NO_OVERWRITE;
		String fieldDelim = getJavaEscapedDelim(properties.getString(CsvOptions.OPTIONAL_FIELD_DELIM));
		String lineDelim = getJavaEscapedDelim(properties.getString(CsvOptions.OPTIONAL_LINE_DELIM));
		String quoteCharacter = getJavaEscapedDelim(properties.getString(CsvOptions.OPTIONAL_QUOTE_CHARACTER));
		if (quoteCharacter != null) {
			Preconditions.checkArgument(
					quoteCharacter.length() == 1,
					"quote character should be a single character, " + quoteCharacter + " found.");
		}
		int parallelism = properties.getInteger(CsvOptions.PARALLELISM, -1);
		Option numFiles = parallelism == -1 ? Option.apply(null) : new Some(parallelism);

		String timezone = properties.getString(CsvOptions.TIME_ZONE_KEY);
		TimeZone tz = (timezone == null) ? TimeZone.getTimeZone("UTC") : TimeZone.getTimeZone(timezone);

		return new CsvSQLTableSink(
				path,
				schema.getColumnTypes(),
				new Some(schema.getColumnNames()),
				new Some(fieldDelim),
				new Some(lineDelim),
				new Some(quoteCharacter),
				numFiles,
				new Some(writeMode),
				new Some(tz));
	}

	public static String getJavaEscapedDelim(String fieldDelim) {
		String unescapedFieldDelim = StringEscapeUtils.unescapeJava(fieldDelim);
		if (fieldDelim != null && !fieldDelim.equals(unescapedFieldDelim)) {
			LOG.info("Field delimiter unescaped from {} to {}.", fieldDelim, unescapedFieldDelim);
		}
		return unescapedFieldDelim;
	}

	@Override
	public void setClassLoader(ClassLoader classLoader) {
		this.classLoader = classLoader;
	}
}
