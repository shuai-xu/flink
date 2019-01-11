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

package org.apache.flink.table.api;

import org.apache.flink.table.util.TableProperties;
import org.apache.flink.util.Preconditions;

/**
 * Table schema and table properties. TableInfo is used to register table source and sink.
 *
 * <p>Example:
 *
 * <pre>
 * {@code
 *   TableInfo.create(tEnv)
 *       .withSchema(
 *         new TableSchema.Builder()
 *         .column("a", DataTypes.INT)
 *         .column("b", DataTypes.STRING).build())
 *       .withProperties(
 *         new TableProperties()
 *         .property("connector.type", "csv")
 *         .property("path", inputFilePath)
 *         .property("fieldDelim", " "))
 *       .registerTableSource("MyTable");
 *
 *   tEnv.scan("MyTable").select("a, b");
 * }
 * </pre>
 */
public class TableInfo {

	// table schema include column names and types
	private TableSchema schema;
	// table properties, such as connector type and connector properties
	private TableProperties properties;
	// table Environment
	private TableEnvironment tEnv;

	private TableInfo(TableEnvironment tEnv) {
		this.tEnv = tEnv;
	}

	public static TableInfo create(TableEnvironment tEnv) {
		return new TableInfo(tEnv);
	}

	/**
	 * Specifies the table schema.
	 */
	public TableInfo withSchema(TableSchema schema) {
		if (schema == null) {
			throw new TableException("TableSchema should not be null!");
		}
		this.schema = schema;
		return this;
	}

	/**
	 * Specifies the properties that related to the table.
	 */
	public TableInfo withProperties(TableProperties tableProperties) {
		if (tableProperties == null) {
			throw new TableException("TableProperties should not be null!");
		}
		this.properties = tableProperties;
		return this;
	}

	public TableSchema getSchema() {
		return schema;
	}

	public TableProperties getProperties() {
		return properties;
	}

	/**
	 * Searches for the specified table source, configures it accordingly, and registers it as
	 * a table under the given name.
	 */
	public void registerTableSource(String tableName) {
		Preconditions.checkNotNull(schema, "Table schema is needed when you register a Table");
		Preconditions.checkNotNull(properties, "Table properties are needed when you register a Table");
		tEnv.registerTableSource(tableName, this);
	}

	/**
	 * Searches for the specified table sink, configures it accordingly, and registers it as
	 * a table under the given name.
	 */
	public void registerTableSink(String tableName) {
		Preconditions.checkNotNull(schema, "Table schema is needed when you register a Table");
		Preconditions.checkNotNull(properties, "Table properties are needed when you register a Table");
		tEnv.registerTableSink(tableName, this);
	}
}
