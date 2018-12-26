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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.api.types.TimestampType;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
  * A table schema that represents a table's structure with field names and types.
  */
@PublicEvolving
public class TableSchema {

	private final Column[] columns;

	private final String[] primaryKeys;

	private final String[][] uniqueKeys;

	private final ComputedColumn[] computedColumns;

	private final Watermark[] watermarks;

	private final Map<String, Integer> columnNameToIndex;

	public TableSchema(Column[] columns) {
		this(columns, new String[]{}, new String[][]{}, new ComputedColumn[]{}, new Watermark[]{});
	}

	public TableSchema(
			Column[] columns, String[] primaryKeys, String[][] uniqueKeys,
			ComputedColumn[] computedColumns, Watermark[] watermarks) {
		this.columns = columns;
		this.primaryKeys = primaryKeys;
		this.uniqueKeys = uniqueKeys;
		this.computedColumns = computedColumns;
		this.watermarks = watermarks;

		// validate and create name to index
		columnNameToIndex = new HashMap<>();
		final Set<String> duplicateNames = new HashSet<>();
		final Set<String> uniqueNames = new HashSet<>();
		for (int i = 0; i < this.columns.length; i++) {
			Preconditions.checkNotNull(columns[i]);
			final String fieldName = this.columns[i].name();
			columnNameToIndex.put(fieldName, i);

			if (uniqueNames.contains(fieldName)) {
				duplicateNames.add(fieldName);
			} else {
				uniqueNames.add(fieldName);
			}
		}

		if (!duplicateNames.isEmpty()) {
			throw new TableException(
				"Table column names must be unique.\n" +
				"The duplicate columns are: " + duplicateNames.toString() + "\n" +
				"All column names: " +
					Arrays.toString(Arrays.stream(this.columns).map(Column::name).toArray(String[]::new))
			);
		}

		// validate primary keys
		for (int i = 0; i < primaryKeys.length; i++) {
			if (!columnNameToIndex.containsKey(primaryKeys[i])) {
				throw new TableException(
					"Primary key field: " + primaryKeys[i] + " not found in table schema."
				);
			}
		}

		// validate unique keys
		for (int i = 0; i < uniqueKeys.length; i++) {
			String[] uniqueKey = uniqueKeys[i];

			if (null == uniqueKey || 0 == uniqueKey.length) {
				throw new TableException("Unique key should not be empty.");
			}

			for (int j = 0; j < uniqueKey.length; j++) {
				if (!columnNameToIndex.containsKey(uniqueKey[j])) {
					throw new TableException(
						"Unique key field: " + uniqueKey[j] + " not found in table schema."
					);
				}
			}
		}
	}

	public TableSchema(String[] names, InternalType[] types, boolean[] nulls) {
		this(validate(names, types, nulls));
	}

	public TableSchema(String[] names, InternalType[] types) {
		this(validate(names, types));
	}

	private static Column[] validate(String[] names, InternalType[] types, boolean[] nulls) {
		if (names.length != types.length) {
			throw new TableException(
				"Number of column indexes and column names must be equal.\n" +
					"Column names count is [" + names.length + "]\n" +
					"Column types count is [" + types.length + "]\n" +
					"Column names: " + Arrays.toString(names) + "\n" +
					"Column types: " + Arrays.toString(types)
			);
		}

		if (names.length != nulls.length) {
			throw new TableException(
				"Number of column names and nullabilities must be equal.\n" +
					"Column names count is: " + names.length + "\n" +
					"Column nullabilities count is: " + nulls.length + "\n" +
					"List of all field names: " + Arrays.toString(names) + "\n" +
					"List of all field nullabilities: " + Arrays.toString(nulls)
			);
		}

		List<Column> columns = new ArrayList<>();
		for (int i = 0; i < names.length; i++) {
			columns.add(new Column(names[i], types[i], nulls[i]));
		}
		return columns.toArray(new Column[columns.size()]);
	}

	private static Column[] validate(String[] names, InternalType[] types) {
		boolean[] nulls = new boolean[types.length];
		for (int i = 0; i < types.length; i++) {
			nulls[i] = !TimestampType.ROWTIME_INDICATOR.equals(types[i])
				&& !TimestampType.PROCTIME_INDICATOR.equals(types[i]);
		}
		return validate(names, types, nulls);
	}

	public Column[] getColumns() {
		return this.columns;
	}

	public String[] getPrimaryKeys() {
		return this.primaryKeys;
	}

	public String[][] getUniqueKeys() {
		return this.uniqueKeys;
	}

	/**
	 * Returns all field type information as an array.
	 */
	public InternalType[] getFieldTypes() {
		return Arrays.stream(columns).map(Column::internalType).toArray(InternalType[]::new);
	}

	/**
	 * Returns the specified type information for the given field index.
	 *
	 * @param fieldIndex the index of the field
	 */
	public Optional<InternalType> getFieldType(int fieldIndex) {
		if (fieldIndex < 0 || fieldIndex >= columns.length) {
			return Optional.empty();
		}
		return Optional.of(columns[fieldIndex].internalType());
	}

	/**
	 * Returns the specified type information for the given field name.
	 *
	 * @param fieldName the name of the field
	 */
	public Optional<InternalType> getFieldType(String fieldName) {
		if (columnNameToIndex.containsKey(fieldName)) {
			return Optional.of(columns[columnNameToIndex.get(fieldName)].internalType());
		}
		return Optional.empty();
	}

	/**
	 * Returns the number of fields.
	 */
	public int getFieldCount() {
		return columns.length;
	}

	/**
	 * Returns all field names as an array.
	 */
	public String[] getFieldNames() {
		return Arrays.stream(columns).map(Column::name).toArray(String[]::new);
	}

	/**
	 * Returns the specified name for the given field index.
	 *
	 * @param fieldIndex the index of the field
	 */
	public Optional<String> getFieldName(int fieldIndex) {
		if (fieldIndex < 0 || fieldIndex >= columns.length) {
			return Optional.empty();
		}
		return Optional.of(columns[fieldIndex].name());
	}

	/**
	 * Returns all field nullables as an array.
	 */
	public boolean[] getFieldNullables() {
		boolean[] nulls = new boolean[columns.length];
		for (int i = 0; i < columns.length; i++) {
			nulls[i] = columns[i].isNullable();
		}
		return nulls;
	}

	/**
	 * @deprecated Use {@link TableSchema#getFieldTypes()} instead. Can be dropped after 1.7.
	 */
	@Deprecated
	public InternalType[] getTypes() {
		return Arrays.stream(columns).map(Column::internalType).toArray(InternalType[]::new);
	}

	/**
	 * @deprecated Use {@link TableSchema#getFieldNames()} instead. Can be dropped after 1.7.
	 */
	@Deprecated
	public String[] getColumnNames() {
		return Arrays.stream(columns).map(Column::name).toArray(String[]::new);
	}

	/**
	 * @deprecated Use {@link TableSchema#getFieldNullables()} instead. Can be dropped after 1.7.
	 */
	@Deprecated
	public boolean[] getNullables() {
		return getFieldNullables();
	}

	public ComputedColumn[] getComputedColumns() {
		return computedColumns;
	}

	public Watermark[] getWatermarks() {
		return watermarks;
	}

	/**
	 * Returns the specified column for the given field index.
	 *
	 * @param fieldIndex the index of the field
	 */
	public Column getColumn(int fieldIndex) {
		Preconditions.checkArgument(fieldIndex >= 0 && fieldIndex < columns.length);
		return columns[fieldIndex];
	}

	/**
	 * Returns the map for column name to field index.
	 */
	public Map<String, Integer> columnNameToIndex() {
		return columnNameToIndex;
	}

	/**
	 * @deprecated Use {@link TableSchema#getFieldType(int)} instead. Can be dropped after 1.7.
	 */
	@Deprecated
	public InternalType getType(int fieldIndex) {
		Preconditions.checkArgument(fieldIndex >= 0 && fieldIndex < columns.length);
		return columns[fieldIndex].internalType();
	}

	/**
	 * @deprecated Use {@link TableSchema#getFieldType(String)} instead. Can be dropped after 1.7.
	 */
	@Deprecated
	public Optional<InternalType> getType(String fieldName) {
		if (columnNameToIndex.containsKey(fieldName)) {
			return Optional.of(columns[columnNameToIndex.get(fieldName)].internalType());
		}
		return Optional.empty();
	}

	/**
	 * @deprecated Use {@link TableSchema#getFieldName(int)} instead. Can be dropped after 1.7.
	 */
	@Deprecated
	public String getColumnName(int fieldIndex) {
		Preconditions.checkArgument(fieldIndex >= 0 && fieldIndex < columns.length);
		return columns[fieldIndex].name();
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append("root\n");
		for (int i = 0; i < columns.length; i++) {
			sb.append(" |-- name: ").append(columns[i].name()).append("\n");
			sb.append(" |-- type: ").append(columns[i].internalType()).append("\n");
			sb.append(" |-- isNullable: ").append(columns[i].isNullable()).append("\n");
		}

		if (primaryKeys.length > 0) {
			sb.append("primary keys\n");
			sb.append(" |-- ").append(String.join(", ", primaryKeys)).append("\n");
		}

		if (uniqueKeys.length > 0) {
			sb.append("unique keys\n");
			for (int i = 0; i < uniqueKeys.length; i++) {
				sb.append(" |-- ").append(String.join(", ", uniqueKeys[i])).append("\n");
			}
		}
		return sb.toString();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		TableSchema schema = (TableSchema) o;
		return Arrays.equals(columns, schema.columns) &&
			Arrays.equals(primaryKeys, schema.primaryKeys) &&
			Arrays.equals(uniqueKeys, schema.uniqueKeys);
	}

	@Override
	public int hashCode() {
		int result = Arrays.hashCode(columns);
		result = 31 * result + Arrays.hashCode(primaryKeys);
		result = 31 * result + Arrays.hashCode(uniqueKeys);
		return result;
	}

	public static Builder builder() {
		return new Builder();
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Builder for creating a {@link TableSchema}.
	 */
	public static class Builder {

		private List<Column> columns;

		private List<String> primaryKey;

		private List<List<String>> uniqueKeys;

		private List<ComputedColumn> computedColumns;

		private List<Watermark> watermarks;

		public Builder() {
			columns = new ArrayList<>();
			primaryKey = new ArrayList<>();
			uniqueKeys = new ArrayList<>();
			computedColumns = new ArrayList<>();
			watermarks = new ArrayList<>();
		}

		/**
		 * Add a field with name and type. The call order of this method determines the order
		 * of fields in the schema.
		 */
		@Deprecated
		public Builder field(String name, InternalType type) {
			columns.add(new Column(name, type));
			return this;
		}

		@Deprecated
		public Builder field(String name, InternalType type, boolean nullable) {
			columns.add(new Column(name, type, nullable));
			return this;
		}

		public Builder column(String name, InternalType type) {
			columns.add(new Column(name, type));
			return this;
		}

		public Builder column(String name, InternalType type, boolean nullable) {
			columns.add(new Column(name, type, nullable));
			return this;
		}

		public Builder primaryKey(String... fields) {
			Arrays.stream(fields).forEach(field -> primaryKey.add(field));
			return this;
		}

		public Builder uniqueKey(String... fields) {
			List<String> uniqueKey = new ArrayList<>();
			Arrays.stream(fields).forEach(field -> uniqueKey.add(field));
			uniqueKeys.add(uniqueKey);
			return this;
		}

		public Builder computedColumn(String name, String expression) {
			computedColumns.add(new ComputedColumn(name, expression));
			return this;
		}

		public Builder watermark(String name, String eventTime, long offset) {
			watermarks.add(new Watermark(name, eventTime, offset));
			return this;
		}

		/**
		 * Returns a {@link TableSchema} instance.
		 */
		public TableSchema build() {
			return new TableSchema(
				columns.stream().toArray(Column[]::new),
				primaryKey.stream().toArray(String[]::new),
				// List<List<String>> -> String[][]
				uniqueKeys.stream()
					.map(u -> u.toArray(new String[u.size()]))  // mapping each List to an array
					.collect(Collectors.toList())               // collecting as a List<String[]>
					.toArray(new String[uniqueKeys.size()][]),
				computedColumns.stream().toArray(ComputedColumn[]::new),
				watermarks.stream().toArray(Watermark[]::new));
		}
	}
}
