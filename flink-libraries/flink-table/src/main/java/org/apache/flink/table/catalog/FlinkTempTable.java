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

import org.apache.calcite.schema.impl.AbstractTable;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents AbstractTable/RelTable in Flink catalog.
 */
public class FlinkTempTable extends CatalogTable {

	private final AbstractTable abstractTable;

	public FlinkTempTable(AbstractTable table) {
		this(table, new HashMap<>());
	}

	public FlinkTempTable(AbstractTable table, Map<String, String> properties) {

		super(null,
			null,
			null,
			properties);

		this.abstractTable = table;
	}

	public AbstractTable getAbstractTable() {
		return abstractTable;
	}

	@Override
	public FlinkTempTable deepCopy() {
		return new FlinkTempTable(abstractTable, new HashMap<>(super.getProperties()));
	}
}
