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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sinks.csv.UpsertCsvTableSink;
import org.apache.flink.table.util.TableProperties;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import scala.Option;
import scala.Some;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.factories.CsvTableFactory.getJavaEscapedDelim;

/**
 * Upsert csv table factory.
 */
public class UpsertCsvTableFactory implements StreamTableSinkFactory<Tuple2<Boolean, Row>> {
	@Override
	public StreamTableSink<Tuple2<Boolean, Row>> createStreamTableSink(Map<String, String> props) {
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

		return (StreamTableSink<Tuple2<Boolean, Row>>) (
			new UpsertCsvTableSink(
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

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CONNECTOR_TYPE, "UPSERTCSV");
		context.put(CONNECTOR_PROPERTY_VERSION, "1");
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		return CsvOptions.SUPPORTED_KEYS;
	}
}
