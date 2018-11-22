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

package org.apache.flink.table.catalog;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.plan.stats.TableStats;

import java.util.Map;

/**
 * Represents a table object.
 */
public class CatalogTable extends CommonTable {
	private final String tableType;
	private final TableSchema tableSchema;
	private final TableStats tableStats;

	// Many catalog allows for temporary objects such as tables which are live only with the current session.
	// They are usually stored in memory only. Thus, “temporary” attribute here is used to mark such temp tables.
	private boolean temporary = false;

	public CatalogTable(String tableType, TableSchema tableSchema, TableStats tableStats, Map<String, String> tableProperties) {
		this(tableType, tableSchema, tableStats, tableProperties, false);
	}

	public CatalogTable(
		String tableType,
		TableSchema tableSchema,
		TableStats tableStats,
		Map<String, String> tableProperties,
		boolean temporary) {

		super(tableProperties);
		this.tableType = tableType;
		this.tableSchema = tableSchema;
		this.tableStats = tableStats;
		this.temporary = temporary;
	}

	public String getTableType() {
		return tableType;
	}

	public TableSchema getTableSchema() {
		return tableSchema;
	}

	public TableStats getTableStats() {
		return tableStats;
	}

	public boolean isTemporary() {
		return temporary;
	}

	public void setTemporary(boolean temporary) {
		this.temporary = temporary;
	}
}
