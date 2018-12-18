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

/**
  * A table schema that represents a table's structure with field names and types.
  */
@PublicEvolving
public class TableSchema2 {

	private final Column2[] columns;

	private final String[] primaryKeys;

	private final String[][] uniqueKeys;

	private final Map<String, Integer> columnNameToIndex;

	public TableSchema2(Column2[] columns) {
		this(columns, new String[]{}, new String[][]{});
	}

	public TableSchema2(Column2[] columns, String[] primaryKeys, String[][] uniqueKeys) {
		this.columns = columns;
		this.primaryKeys = primaryKeys;
		this.uniqueKeys = uniqueKeys;

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
				"Field names must be unique.\n" +
				"List of duplicate fields: " + duplicateNames.toString() + "\n" +
				"List of all fields: " +
					Arrays.toString(Arrays.stream(this.columns).map(Column2::name).toArray(String[]::new))
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

	public TableSchema2(String[] names, InternalType[] types, Boolean[] nullabilities) {
		this(validate(names, types, nullabilities));
	}

	public TableSchema2(String[] names, InternalType[] types) {
		this(validate(names, types));
	}

	private static Column2[] validate(String[] names, InternalType[] types, Boolean[] nullabilities) {
		if (names.length != types.length || names.length != nullabilities.length) {
			throw new TableException(
				"Number of column names, types and nullabilities must be equal.\n" +
					"Column name count is: " + names.length + "\n" +
					"Column type count is: " + types.length + "\n" +
					"Column nullability count is: " + nullabilities.length + "\n" +
					"List of all field names: " + Arrays.toString(names) + "\n" +
					"List of all field types: " + Arrays.toString(types) + "\n" +
					"List of all field nullabilities: " + Arrays.toString(nullabilities)
			);
		}

		List<Column2> columns = new ArrayList<>();
		for (int i = 0; i < names.length; i++) {
			columns.add(new Column2(names[i], types[i], nullabilities[i]));
		}
		return columns.toArray(new Column2[columns.size()]);
	}

	private static Column2[] validate(String[] names, InternalType[] types) {
		Boolean[] nullabilities =
			Arrays.stream(types).map(
			t -> (!TimestampType.ROWTIME_INDICATOR.equals(t)
				&& !TimestampType.PROCTIME_INDICATOR.equals(t))).toArray(Boolean[]::new);
		return validate(names, types, nullabilities);
	}

	public Column2[] getColumns() {
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
		return Arrays.stream(columns).map(Column2::type).toArray(InternalType[]::new);
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
		return Optional.of(columns[fieldIndex].type());
	}

	/**
	 * Returns the specified type information for the given field name.
	 *
	 * @param fieldName the name of the field
	 */
	public Optional<InternalType> getFieldType(String fieldName) {
		if (columnNameToIndex.containsKey(fieldName)) {
			return Optional.of(columns[columnNameToIndex.get(fieldName)].type());
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
		return Arrays.stream(columns).map(Column2::name).toArray(String[]::new);
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
	public Boolean[] getFieldNullables() {
		return Arrays.stream(columns).map(Column2::isNullable).toArray(Boolean[]::new);
	}

	/**
	 * @deprecated Use {@link TableSchema2#getFieldTypes()} instead. Can be dropped after 1.7.
	 */
	@Deprecated
	public InternalType[] getTypes() {
		return Arrays.stream(columns).map(Column2::type).toArray(InternalType[]::new);
	}

	/**
	 * @deprecated Use {@link TableSchema2#getFieldNames()} instead. Can be dropped after 1.7.
	 */
	@Deprecated
	public String[] getColumnNames() {
		return Arrays.stream(columns).map(Column2::name).toArray(String[]::new);
	}

	/**
	 * @deprecated Use {@link TableSchema2#getFieldNullables()} instead. Can be dropped after 1.7.
	 */
	@Deprecated
	public Boolean[] getNullables() {
		return Arrays.stream(columns).map(Column2::isNullable).toArray(Boolean[]::new);
	}

	/**
	 * Returns the specified column for the given field index.
	 *
	 * @param fieldIndex the index of the field
	 */
	public Column2 getColumn(int fieldIndex) {
		Preconditions.checkArgument(fieldIndex >= 0 && fieldIndex < columns.length);
		return columns[fieldIndex];
	}

	/**
	 * @deprecated Use {@link TableSchema2#getFieldType(int)} instead. Can be dropped after 1.7.
	 */
	@Deprecated
	public InternalType getType(int fieldIndex) {
		Preconditions.checkArgument(fieldIndex >= 0 && fieldIndex < columns.length);
		return columns[fieldIndex].type();
	}

	/**
	 * @deprecated Use {@link TableSchema2#getFieldType(String)} instead. Can be dropped after 1.7.
	 */
	@Deprecated
	public Optional<InternalType> getType(String fieldName) {
		if (columnNameToIndex.containsKey(fieldName)) {
			return Optional.of(columns[columnNameToIndex.get(fieldName)].type());
		}
		return Optional.empty();
	}

	/**
	 * @deprecated Use {@link TableSchema2#getFieldName(int)} instead. Can be dropped after 1.7.
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
			sb.append(" |-- type: ").append(columns[i].type()).append("\n");
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
		TableSchema2 schema = (TableSchema2) o;
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
	 * Builder for creating a {@link TableSchema2}.
	 */
	public static class Builder {

		private List<String> fieldNames;

		private List<InternalType> fieldTypes;

		public Builder() {
			fieldNames = new ArrayList<>();
			fieldTypes = new ArrayList<>();
		}

		/**
		 * Add a field with name and type. The call order of this method determines the order
		 * of fields in the schema.
		 */
		public Builder field(String name, InternalType type) {
			Preconditions.checkNotNull(name);
			Preconditions.checkNotNull(type);
			fieldNames.add(name);
			fieldTypes.add(type);
			return this;
		}

		/**
		 * Returns a {@link TableSchema2} instance.
		 */
		public TableSchema2 build() {
			return new TableSchema2(
				fieldNames.toArray(new String[0]),
				fieldTypes.toArray(new InternalType[0]));
		}
	}
}
