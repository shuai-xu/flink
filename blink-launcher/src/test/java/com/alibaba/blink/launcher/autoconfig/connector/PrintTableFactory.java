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

package com.alibaba.blink.launcher.autoconfig.connector;

import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.api.TableFactory;
import org.apache.flink.table.api.TableProperties;
import org.apache.flink.table.sinks.PrintTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.DimensionTableSource;
import org.apache.flink.table.sources.TableSource;

import java.util.TimeZone;

/**
 * A simplified class, mostly copied from blink-connector, for IT test only.
 */
public class PrintTableFactory implements TableFactory {

	@Override
	public TableSource createTableSource(String s, RichTableSchema richTableSchema, TableProperties tableProperties) {
		throw new UnsupportedOperationException("Print 作为源表暂不支持.");
	}

	@Override
	public DimensionTableSource createDimensionTableSource(String s, RichTableSchema richTableSchema, TableProperties tableProperties) {
		throw new UnsupportedOperationException("Print 作为维表暂不支持.");
	}

	@Override
	public TableSink createTableSink(String s, RichTableSchema schema, TableProperties tableProperties) {
		TimeZone tz = TimeZone.getDefault();
		PrintTableSink printSink = new PrintTableSink(tz);
		return printSink;
	}
}
