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

package org.apache.flink.connectors.parquet;

import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.exceptions.NotEnoughParamsException;
import org.apache.flink.exceptions.UnsupportedTableException;
import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.api.TableFactory;
import org.apache.flink.table.api.TableProperties;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.parquet.ParquetTableSink;
import org.apache.flink.table.sources.DimensionTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.sources.parquet.ParquetVectorizedColumnRowTableSource;
import org.apache.flink.util.StringUtils;

import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import scala.Option;
import scala.Some;

/**
 * Parquet table factory.
 */
public class ParquetTableFactory implements TableFactory {
	private static final String DEFAULT_WRITE_MODE = "None";

	@Override
	public TableSource createTableSource(String s, RichTableSchema richTableSchema, TableProperties tableProperties) {
		String filePath = tableProperties.getString(ParquetOptions.FILE_PATH);
		if (StringUtils.isNullOrWhitespaceOnly(filePath)) {
			throw new NotEnoughParamsException(ParquetOptions.PARAMS_HELP_MSG);
		}

		boolean enumerateNestedFiles = tableProperties.getBoolean(ParquetOptions.ENUMERATE_NESTED_FILES);
		return new ParquetVectorizedColumnRowTableSource(
				new Path(filePath),
				richTableSchema.getColumnTypes(),
				richTableSchema.getColumnNames(),
				enumerateNestedFiles);
	}

	@Override
	public DimensionTableSource<?> createDimensionTableSource(String s, RichTableSchema richTableSchema, TableProperties tableProperties) {
		throw new UnsupportedTableException("Parquet 作为维表暂不支持.");
	}

	@Override
	public TableSink<?> createTableSink(String s, RichTableSchema richTableSchema, TableProperties tableProperties) {
		String filePath = tableProperties.getString(ParquetOptions.FILE_PATH);
		if (StringUtils.isNullOrWhitespaceOnly(filePath)) {
			throw new NotEnoughParamsException(ParquetOptions.PARAMS_HELP_MSG);
		}

		Option<WriteMode> writeModeOption = null;
		String writeMode = tableProperties.getString(ParquetOptions.WRITE_MODE);
		if (!DEFAULT_WRITE_MODE.equals(writeMode)) {
			writeModeOption = new Some(WriteMode.valueOf(
					tableProperties.getString(ParquetOptions.WRITE_MODE)));
		}

		CompressionCodecName compressionCodecName = CompressionCodecName.valueOf(tableProperties
				.getString(ParquetOptions.COMPRESSION_CODEC_NAME));

		return new ParquetTableSink(filePath, writeModeOption, compressionCodecName);
	}
}
