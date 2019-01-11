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

package org.apache.flink.connectors.hbase;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A simple table descriptor for HBase Table.
 */
public class HTableSchema implements Serializable {

	private static final long serialVersionUID = 3210931821170546473L;

	// the name of the HBase Table
	final String tableName;

	// the rowKey of the HBase Table
	final RowKey rowKey;

	// the column families of the HBase Table
	final List<HColumn> columnFamilies;

	private HTableSchema(String tableName, RowKey rowKey, List<HColumn> columnFamilies) {
		this.tableName = tableName;
		this.rowKey = rowKey;
		this.columnFamilies = columnFamilies;
	}

	public String getTableName() {
		return tableName;
	}

	public RowKey getRowKey() {
		return rowKey;
	}

	public List<HColumn> getColumnFamilies() {
		return columnFamilies;
	}

	/**
	 * Returns a flat qualifiers list in defining order.
	 * Qualifier info is a Tuple3 representation:
	 * (byte[] columnFamily, byte[] qualifier, TypeInformation typeInfo)
	 */
	public List<Tuple3<byte[], byte[], TypeInformation<?>>> getFlatQualifiers() {
		List<Tuple3<byte[], byte[], TypeInformation<?>>> qualifierList = new ArrayList<>();

		for (HColumn column : columnFamilies) {
			byte[] columnBytes = Bytes.toBytes(column.name);
			for (HQualifier qualifier : column.qualifiers) {
				qualifierList.add(new Tuple3<>(columnBytes, Bytes.toBytes(qualifier.name), qualifier.type));
			}
		}
		return qualifierList;
	}

	/**
	 * Represents the RowKey in a HBase Table.
	 */
	public static class RowKey {
		public String name;
		public TypeInformation type;

		public RowKey(String name, TypeInformation type) {
			this.name = name;
			this.type = type;
		}

		@Override
		public String toString() {
			return "RowKey{" + "name='" + name + '\'' + ", type=" + type + '}';
		}
	}

	/**
	 * Represents a Column in a HBase Table's Column Family(s).
	 */
	public static class HColumn {
		public String name;
		public List<HQualifier> qualifiers;

		private HColumn(String name, List<HQualifier> qualifiers) {
			this.name = name;
			this.qualifiers = qualifiers;
		}

		@Override
		public String toString() {
			return "HColumn{" + "name='" + name + '\'' + ", qualifiers=" + qualifiers + '}';
		}
	}

	/**
	 * Represents a Qualifier in a Column Family of HBase Table.
	 */
	public static class HQualifier {
		public String name;
		public TypeInformation type;

		public HQualifier(String name, TypeInformation type) {
			this.name = name;
			this.type = type;
		}

		@Override
		public String toString() {
			return "HQualifier{name='" + name + '\'' + ", type=" + type + '}';
		}
	}

	/**
	 * Helps to build a Column Family of HBase Table.
	 */
	public static class HColumnBuilder {
		String name;
		List<HQualifier> qualifiers;

		// for uniqueness check, case sensitive
		Set<String> uniqueQualifiers = new HashSet<>();

		public HColumnBuilder(String name) {
			this.name = name;
			this.qualifiers = new ArrayList<>();
		}

		public HColumnBuilder addQualifier(HQualifier qualifier) {
			Preconditions.checkNotNull(qualifier, "qualifier should not be null!");
			Preconditions.checkNotNull(qualifier.type, "qualifier should not be null!");
			Preconditions.checkArgument(!uniqueQualifiers.contains(qualifier.name),
				"qualifier[" + qualifier.name + "] in column[" + name + "] already exists!");

			this.qualifiers.add(qualifier);
			this.uniqueQualifiers.add(qualifier.name);
			return this;
		}

		public HColumn build() {
			return new HColumn(name, qualifiers);
		}
	}

	/**
	 * Helps to build a HBase Table.
	 */
	public static class Builder {
		String tableName;
		RowKey rowKey;
		List<HColumn> columnFamilies;

		// for uniqueness check, case sensitive
		Set<String> uniqueCF = new HashSet<>();

		public Builder(String tableName, RowKey rowKey) {
			this.tableName = tableName;
			this.rowKey = rowKey;
			this.columnFamilies = new ArrayList<>();
		}

		public Builder addHColumn(HColumn column) {
			Preconditions.checkArgument(null != column && !uniqueCF.contains(column.name), "column[" + column.name + "] already exists!");
			this.uniqueCF.add(column.name);
			this.columnFamilies.add(column);
			return this;
		}

		public HTableSchema build() {
			Preconditions.checkArgument(!StringUtils.isNullOrWhitespaceOnly(tableName), "table name should not be empty!");
			Preconditions.checkNotNull(rowKey, "rowkey should not be null!");
			Preconditions.checkArgument(null != columnFamilies && columnFamilies.size() > 0, "column family(s) should not be empty!");

			return new HTableSchema(tableName, rowKey, columnFamilies);
		}
	}

}
