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

import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.plan.stats.TableStats;

import org.apache.calcite.rex.RexNode;

import java.util.LinkedHashSet;
import java.util.Map;

/**
 * Represents a view in catalog.
 */
public class CatalogView extends CatalogTable {
	// Original text of the view definition.
	private final String originalQuery;

	// Expanded text of the original view definition
	// This is needed because the context such as current DB is
	// lost after the session, in which view is defined, is gone.
	// Expanded query text takes care of the this, as an example.
	private final String expandedQuery;

	public CatalogView(
		String tableType,
		TableSchema tableSchema,
		Map<String, String> properties,
		boolean isStreaming,
		String originalQuery,
		String expandedQuery) {

		super(tableType, tableSchema, new TableStats(), properties, isStreaming);

		this.originalQuery = originalQuery;
		this.expandedQuery = expandedQuery;
	}

	public CatalogView(
		String tableType,
		TableSchema tableSchema,
		Map<String, String> properties,
		RichTableSchema richTableSchema,
		TableStats tableStats,
		String comment,
		LinkedHashSet<String> partitionColumnNames,
		boolean isPartitioned,
		Map<String, RexNode> computedColumns,
		String rowTimeField,
		long watermarkOffset,
		long createTime,
		long lastAccessTime,
		boolean isStreaming,
		String originalQuery,
		String expandedQuery) {

		super(tableType, tableSchema, properties, richTableSchema, tableStats, comment, partitionColumnNames, isPartitioned, computedColumns, rowTimeField, watermarkOffset, createTime, lastAccessTime, isStreaming);

		this.originalQuery = originalQuery;
		this.expandedQuery = expandedQuery;
	}

	public String getOriginalQuery() {
		return originalQuery;
	}

	public String getExpandedQuery() {
		return expandedQuery;
	}

	public static CatalogView createCatalogView(CatalogTable table, String originalQuery, String expandedQuery) {
		return new CatalogView(
			table.getTableType(),
			table.getTableSchema(),
			table.getProperties(),
			table.getRichTableSchema(),
			table.getTableStats(),
			table.getComment(),
			table.getPartitionColumnNames(),
			table.isPartitioned(),
			table.getComputedColumns(),
			table.getRowTimeField(),
			table.getWatermarkOffset(),
			table.getCreateTime(),
			table.getLastAccessTime(),
			table.isStreaming(),
			originalQuery,
			expandedQuery
		);
	}

	@Override
	public String toString() {
		return "CatalogView{" +
			"originalQuery='" + originalQuery + '\'' +
			", expandedQuery='" + expandedQuery + '\'' +
			", tableType='" + getTableType() + '\'' +
			", tableSchema=" + getTableSchema() +
			", tableStats=" + getTableStats() +
			", properties=" + getProperties() +
			", richTableSchema=" + getRichTableSchema() +
			", comment='" + getComment() + '\'' +
			", partitionColumnNames=" + getPartitionColumnNames() +
			", isPartitioned=" + isPartitioned() +
			", computedColumns=" + getComputedColumns() +
			", rowTimeField='" + getRowTimeField() + '\'' +
			", watermarkOffset=" + getWatermarkOffset() +
			", createTime=" + getCreateTime() +
			", lastAccessTime=" + getLastAccessTime() +
			", isStreaming=" + isStreaming() +
			'}';
	}
}
