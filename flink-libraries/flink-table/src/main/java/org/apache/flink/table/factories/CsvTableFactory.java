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

package org.apache.flink.table.factories;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.sinks.BatchTableSink;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sinks.csv.CsvTableSink;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.csv.CsvTableSource;
import org.apache.flink.table.util.TableProperties;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import org.apache.commons.lang3.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import scala.Option;
import scala.Some;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;


/**
 * A CSV table factory from Alibaba's Blink.
 */
public class CsvTableFactory implements
		StreamTableSourceFactory<BaseRow>,
		BatchTableSourceFactory<BaseRow>,
		StreamTableSinkFactory<BaseRow>,
		BatchTableSinkFactory<BaseRow> {
	private static final Logger LOG = LoggerFactory.getLogger(CsvTableFactory.class);

	@Override
	public BatchTableSink<BaseRow> createBatchTableSink(Map<String, String> properties) {
		return createCsvTableSink(properties);
	}

	@Override
	public BatchTableSource<BaseRow> createBatchTableSource(Map<String, String> properties) {
		return createCsvTableSource(properties);
	}

	@Override
	public StreamTableSink<BaseRow> createStreamTableSink(Map<String, String> properties) {
		return createCsvTableSink(properties);
	}

	@Override
	public StreamTableSource<BaseRow> createStreamTableSource(Map<String, String> properties) {
		return createCsvTableSource(properties);
	}

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CONNECTOR_TYPE, "CSV");
		context.put(CONNECTOR_PROPERTY_VERSION, "1");
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		return CsvOptions.SUPPORTED_KEYS;
	}

	private CsvTableSource createCsvTableSource(Map<String, String> props) {
		TableProperties properties = new TableProperties();
		properties.putProperties(props);
		RichTableSchema schema = properties.readSchemaFromProperties(null);

		final String path = properties.getString(CsvOptions.PATH);
		if (StringUtils.isNullOrWhitespaceOnly(path)) {
			throw new IllegalArgumentException(CsvOptions.PARAMS_HELP_MSG);
		}

		final String fieldDelim = getJavaEscapedDelim(properties.getString(CsvOptions.OPTIONAL_FIELD_DELIM));
		final String lineDelim = getJavaEscapedDelim(properties.getString(CsvOptions.OPTIONAL_LINE_DELIM));
		final String charset = properties.getString(CsvOptions.OPTIONAL_CHARSET);
		final boolean emptyColumnAsNull = properties.getBoolean(CsvOptions.EMPTY_COLUMN_AS_NULL);
		final String timeZone = properties.getString(CsvOptions.TIME_ZONE);
		final TimeZone tz = (timeZone == null) ? TimeZone.getTimeZone("UTC") : TimeZone.getTimeZone(timeZone);
		final boolean enumerateNestedFiles = properties.getBoolean(CsvOptions.OPTIONAL_ENUMERATE_NESTED_FILES);

		CsvTableSource.Builder builder = CsvTableSource.builder()
			.path(path)
			.fieldDelimiter(fieldDelim)
			.lineDelimiter(lineDelim)
			.charset(charset)
			.fields(schema.getColumnNames(), schema.getColumnTypes(), schema.getNullables())
			.timezone(tz)
			.setNestedFileEnumerate(enumerateNestedFiles);

		if (emptyColumnAsNull) {
			builder.enableEmptyColumnAsNull();
		}

		final String quoteCharacter = getJavaEscapedDelim(properties.getString(CsvOptions.OPTIONAL_QUOTE_CHARACTER));
		if (quoteCharacter != null) {
			Preconditions.checkArgument(
				quoteCharacter.length() == 1,
				"quote character should be a single character, " + quoteCharacter + " found.");
			builder.quoteCharacter(quoteCharacter.charAt(0));
		}

		final boolean firstLineAsHeader = properties.getBoolean(CsvOptions.OPTIONAL_FIRST_LINE_AS_HEADER);
		if (firstLineAsHeader) {
			builder.ignoreFirstLine();
		}

		final String commentsPrefix = properties.getString(CsvOptions.OPTIONAL_COMMENTS_PREFIX);
		if (commentsPrefix != null) {
			builder.commentPrefix(commentsPrefix);
		}
		return builder.build();
	}

	private CsvTableSink createCsvTableSink(Map<String, String> props) {
		TableProperties properties = new TableProperties();
		properties.putProperties(props);
		RichTableSchema schema = properties.readSchemaFromProperties(null);

		final String path = properties.getString(CsvOptions.PATH);
		if (StringUtils.isNullOrWhitespaceOnly(path)) {
			throw new IllegalArgumentException(CsvOptions.PARAMS_HELP_MSG);
		}

		final boolean writeModeFlag = properties.getBoolean(CsvOptions.OPTIONAL_OVER_RIDE_MODE);
		final FileSystem.WriteMode writeMode =
			writeModeFlag ? FileSystem.WriteMode.OVERWRITE : FileSystem.WriteMode.NO_OVERWRITE;
		final String fieldDelim = getJavaEscapedDelim(properties.getString(CsvOptions.OPTIONAL_FIELD_DELIM));
		final String lineDelim = getJavaEscapedDelim(properties.getString(CsvOptions.OPTIONAL_LINE_DELIM));
		final String quoteCharacter = getJavaEscapedDelim(properties.getString(CsvOptions.OPTIONAL_QUOTE_CHARACTER));
		if (quoteCharacter != null) {
			Preconditions.checkArgument(
				quoteCharacter.length() == 1,
				"quote character should be a single character, " + quoteCharacter + " found.");
		}
		final int parallelism = properties.getInteger(CsvOptions.PARALLELISM, -1);
		Option numFiles = parallelism == -1 ? Option.apply(null) : new Some(parallelism);
		final String timeZone = properties.getString(CsvOptions.TIME_ZONE);
		final TimeZone tz = (timeZone == null) ? TimeZone.getTimeZone("UTC") : TimeZone.getTimeZone(timeZone);

		return (CsvTableSink) (
			new CsvTableSink(
				path,
				Option.apply(fieldDelim),
				Option.apply(lineDelim),
				Option.apply(quoteCharacter),
				numFiles,
				Option.apply(writeMode),
				Option.empty(),
				Option.apply(tz)
			).configure(schema.getColumnNames(), schema.getColumnTypes())
		);
	}

	public static String getJavaEscapedDelim(String fieldDelim) {
		String unescapedFieldDelim = StringEscapeUtils.unescapeJava(fieldDelim);
		if (fieldDelim != null && !fieldDelim.equals(unescapedFieldDelim)) {
			LOG.info("Field delimiter unescaped from {} to {}.", fieldDelim, unescapedFieldDelim);
		}
		return unescapedFieldDelim;
	}
}
