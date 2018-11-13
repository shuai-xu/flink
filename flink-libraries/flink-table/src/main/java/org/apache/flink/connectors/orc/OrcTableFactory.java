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

package org.apache.flink.connectors.orc;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.exceptions.NotEnoughParamsException;
import org.apache.flink.exceptions.UnsupportedTableException;
import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.api.TableFactory;
import org.apache.flink.table.api.TableProperties;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.orc.OrcTableSink;
import org.apache.flink.table.sources.DimensionTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.sources.orc.OrcVectorizedColumnRowTableSource;
import org.apache.flink.util.StringUtils;

import org.apache.orc.CompressionKind;

import java.util.Arrays;

import scala.Option;
import scala.Some;

/**
 * Orc TableFactory.
 */
public class OrcTableFactory implements TableFactory {
	private static final String DEFAULT_WRITE_MODE = "None";

	@Override
	public TableSource createTableSource(String tableName, RichTableSchema schema, TableProperties properties) {
		String filePath = properties.getString(OrcOptions.FILE_PATH);
		if (StringUtils.isNullOrWhitespaceOnly(filePath)) {
			throw new NotEnoughParamsException(OrcOptions.PARAMS_HELP_MSG);
		}

		boolean enumerateNestedFiles = properties.getBoolean(OrcOptions.ENUMERATE_NESTED_FILES);

		InternalType[] dataTypes =
			Arrays.stream(schema.getColumnTypes()).map(x -> DataTypes.internal(x)).toArray(InternalType[]::new);

		OrcVectorizedColumnRowTableSource t =  new OrcVectorizedColumnRowTableSource(
			new Path(filePath),
			dataTypes,
			schema.getColumnNames(),
			enumerateNestedFiles);
		t.setSchemaFields(schema.getColumnNames());
		return t;
	}

	@Override
	public DimensionTableSource<?> createDimensionTableSource(String tableName, RichTableSchema schema, TableProperties properties) {
		throw new UnsupportedTableException("Orc Dimension Table is not supported now.");
	}

	@Override
	public TableSink<?> createTableSink(String tableName, RichTableSchema schema, TableProperties properties) {
		String filePath = properties.getString(OrcOptions.FILE_PATH);
		if (StringUtils.isNullOrWhitespaceOnly(filePath)) {
			throw new NotEnoughParamsException(OrcOptions.PARAMS_HELP_MSG);
		}

		Option<FileSystem.WriteMode> writeModeOption = null;
		String writeMode = properties.getString(OrcOptions.WRITE_MODE);
		if (!DEFAULT_WRITE_MODE.equals(writeMode)) {
			writeModeOption = new Some(FileSystem.WriteMode.valueOf(
				properties.getString(OrcOptions.WRITE_MODE)));
		}

		CompressionKind compressionKind = CompressionKind.valueOf(
			properties.getString(OrcOptions.COMPRESSION_CODEC_NAME));

		return new OrcTableSink(filePath, writeModeOption, compressionKind);
	}
}
