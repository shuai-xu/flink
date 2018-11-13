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

package org.apache.flink.connectors.csv;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.io.CsvInputFormat;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.api.TableProperties;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.types.BaseRowType;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.sources.AsyncConfig;
import org.apache.flink.table.sources.DimensionTableSource;
import org.apache.flink.table.sources.IndexKey;
import org.apache.flink.table.sources.csv.BaseRowCsvInputFormat;
import org.apache.flink.table.typeutils.AbstractRowSerializer;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;
import org.apache.flink.table.typeutils.TypeUtils;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import scala.Option;

/**
 * CSV dim table.
 */
public class CsvDimTable implements DimensionTableSource<BaseRow> {
	private TableProperties properties;
	private RichTableSchema schema;
	private String path;
	private String[] fieldNames;
	private InternalType[] fieldTypes;
	private String fieldDelim = CsvInputFormat.DEFAULT_FIELD_DELIMITER;
	private String rowDelim = CsvInputFormat.DEFAULT_LINE_DELIMITER;
	private String charsetName = "UTF-8";
	private Character quoteCharacter = null;
	private Boolean ignoreFirstLine = false;
	private String ignoreComments = null;
	private Boolean lenient = false;

	private BaseRowType rowType;
	private boolean emptyColumnAsNull;

	private TimeZone timezone = null;

	private boolean nestedFileEnumerate = false;

	public CsvDimTable(
			RichTableSchema schema,
			TableProperties properties,
			String path,
			String[] fieldNames,
			InternalType[] fieldTypes,
			boolean emptyColumnAsNull) {
		this.schema = schema;
		this.properties = properties;
		this.path = path;
		this.fieldNames = fieldNames;
		this.fieldTypes = fieldTypes;
		rowType = schema.getResultType(GenericRow.class);
		this.emptyColumnAsNull = emptyColumnAsNull;
	}

	@Override
	public String explainSource() {
		return "csv-dim";
	}

	@Override
	public Collection<IndexKey> getIndexes() {
		return schema.toIndexKeys();
	}

	@Override
	public FlatMapFunction<BaseRow, BaseRow> getLookupFunction(IndexKey keys) {
		CsvRowFetcher csvRowFectcher = new CsvRowFetcher(
				properties, path, fieldNames, fieldTypes, keys, rowType, emptyColumnAsNull, timezone, nestedFileEnumerate);
		csvRowFectcher.setCharsetName(charsetName)
			.setFieldDelim(fieldDelim)
			.setIgnoreComments(ignoreComments)
			.setLenient(lenient)
			.setQuoteCharacter(quoteCharacter)
			.setIgnoreFirstLine(ignoreFirstLine)
			.setRowDelim(rowDelim);
		return csvRowFectcher;
	}

	@Override
	public AsyncFunction<BaseRow, BaseRow> getAsyncLookupFunction(IndexKey keys) {
		throw new UnsupportedOperationException("CSV do not support async join currently");
	}

	@Override
	public boolean isTemporal() {
		return true;
	}

	@Override
	public boolean isAsync() {
		return false;
	}

	@Override
	public AsyncConfig getAsyncConfig() {
		return new AsyncConfig();
	}

	@Override
	public DataType getReturnType() {
		return DataTypes.internal(rowType);
	}

	@Override
	public TableStats getTableStats() {
		return null;
	}

	public CsvDimTable setFieldDelim(String fieldDelim) {
		this.fieldDelim = fieldDelim;
		return this;
	}

	public CsvDimTable setRowDelim(String rowDelim) {
		this.rowDelim = rowDelim;
		return this;
	}

	public CsvDimTable setCharsetName(String charsetName) {
		this.charsetName = charsetName;
		return this;
	}

	public CsvDimTable setQuoteCharacter(Character quoteCharacter) {
		this.quoteCharacter = quoteCharacter;
		return this;
	}

	public CsvDimTable setIgnoreFirstLine(Boolean ignoreFirstLine) {
		this.ignoreFirstLine = ignoreFirstLine;
		return this;
	}

	public CsvDimTable setIgnoreComments(String ignoreComments) {
		this.ignoreComments = ignoreComments;
		return this;
	}

	public CsvDimTable setLenient(Boolean lenient) {
		this.lenient = lenient;
		return this;
	}

	public CsvDimTable setTimezone(TimeZone timezone) {
		this.timezone = timezone;
		return this;
	}

	public CsvDimTable setNestedFileEnumerate(Boolean isEnabled) {
		this.nestedFileEnumerate = isEnabled;
		return this;
	}

	@Override
	public TableSchema getTableSchema() {
		return TableSchema.fromDataType(getReturnType(), Option.empty());
	}
}

class CsvRowFetcher extends DimJoinFetcher implements FlatMapFunction<BaseRow, BaseRow>, ResultTypeQueryable<BaseRow> {
	private TableProperties properties;
	private String path;
	private String[] fieldNames;
	private InternalType[] fieldTypes;
	private String fieldDelim = CsvInputFormat.DEFAULT_FIELD_DELIMITER;
	private String rowDelim = CsvInputFormat.DEFAULT_LINE_DELIMITER;
	private String charsetName = "UTF-8";
	private Character quoteCharacter = null;
	private Boolean ignoreFirstLine = false;
	private String ignoreComments = null;
	private Boolean lenient = false;
	private BaseRowType rowType;
	private AbstractRowSerializer<BaseRow> rowSerializer;

	private TimeZone timezone = null;

	private List<Integer> sourceKeys = new ArrayList<>();
	private List<Integer> targetKeys = new ArrayList<>();
	private List<InternalType> keyTypes = new ArrayList<>();
	private int[] selectedFields;
	private int fieldLength;
	private boolean emptyColumnAsNull;
	private boolean nestedFileEnumerate;

	private boolean uniqueIndex;
	private Map<Object, List<BaseRow>> one2manyDataMap = new HashMap<>();
	private Map<Object, BaseRow> one2oneDataMap = new HashMap<>();

	public CsvRowFetcher(
			TableProperties properties,
			String path,
			String[] fieldNames,
			InternalType[] fieldTypes,
			IndexKey checkedIndex,
			BaseRowType rowType,
			boolean emptyColumnAsNull,
			TimeZone timezone,
			boolean nestedFileEnumerate) {
		this.properties = properties;
		this.path = path;
		this.fieldNames = fieldNames;
		this.fieldTypes = fieldTypes;
		this.rowType = rowType;
		this.rowSerializer = (AbstractRowSerializer<BaseRow>) TypeUtils.createSerializer(rowType);
		this.fieldLength = rowType.getArity();
		this.uniqueIndex = checkedIndex.isUnique();
		List<Integer> indexCols = checkedIndex.getDefinedColumns();
		for (int i = 0; i < indexCols.size(); i++) {
			sourceKeys.add(i);
			int targetIdx = indexCols.get(i);
			assert targetIdx != -1;
			targetKeys.add(targetIdx);
			keyTypes.add(fieldTypes[targetIdx]);
		}
		selectedFields = new int[fieldTypes.length];
		for (int i = 0; i < selectedFields.length; i++) {
			selectedFields[i] = i;
		}
		this.emptyColumnAsNull = emptyColumnAsNull;

		TimeZone tz = (timezone == null) ? TimeZone.getTimeZone("UTC") : timezone;
		this.timezone = tz;

		this.nestedFileEnumerate = nestedFileEnumerate;
	}

	static int findPkIndex(String pk, String[] fieldNames) {
		for (int j = 0; j < fieldNames.length; j++) {
			if (fieldNames[j].equals(pk)) {
				return j;
			}
		}
		return -1;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		BaseRowCsvInputFormat inputFormat = new BaseRowCsvInputFormat(new Path(path), fieldTypes, rowDelim, fieldDelim,
				selectedFields, emptyColumnAsNull);

		inputFormat.setTimezone(timezone);
		inputFormat.setSkipFirstLineAsHeader(ignoreFirstLine);
		inputFormat.setLenient(lenient);
		if (charsetName != null) {
			inputFormat.setCharset(charsetName);
		}
		if (quoteCharacter != null) {
			inputFormat.enableQuotedStringParsing(quoteCharacter);
		}
		if (ignoreComments != null) {
			inputFormat.setCommentPrefix(ignoreComments);
		}

		inputFormat.setNestedFileEnumeration(nestedFileEnumerate);

		FileInputSplit[] inputSplits = inputFormat.createInputSplits(1);
		for (FileInputSplit split : inputSplits) {
			inputFormat.open(split);
			GenericRow row = new GenericRow(rowType.getArity());
			while (true) {
				BaseRow r = inputFormat.nextRecord(row);
				if (r == null) {
					break;
				} else {
					Object key = getTargetKey(r);
					if (uniqueIndex) {
						// TODO exception when duplicate data on uk ?
						one2oneDataMap.put(key, rowSerializer.copy(r));
					} else {
						if (one2manyDataMap.containsKey(key)) {
							one2manyDataMap.get(key).add(rowSerializer.copy(r));
						} else {
							List<BaseRow> rows = new ArrayList<>();
							rows.add(rowSerializer.copy(r));
							one2manyDataMap.put(key, rows);
						}
					}
				}
			}
			inputFormat.close();
		}
	}

	@Override
	public void close() throws Exception {
		super.close();
	}

	@Override
	public void flatMap(BaseRow row, Collector<BaseRow> collector) throws Exception {
		Object srcKey = getSourceKey(row);
		if (uniqueIndex) {
			if (one2oneDataMap.containsKey(srcKey)) {
				collector.collect(one2oneDataMap.get(srcKey));
			}
		} else {
			if (one2manyDataMap.containsKey(srcKey)) {
				for (BaseRow row1 : one2manyDataMap.get(srcKey)) {
					collector.collect(row1);
				}
			}
		}
	}

	@Override
	public BaseRowTypeInfo<BaseRow> getProducedType() {
		return (BaseRowTypeInfo) DataTypes.toTypeInfo(rowType);
	}

	protected Object getSourceKey(BaseRow source) {
		return getKey(source, sourceKeys);
	}

	protected Object getTargetKey(BaseRow target) {
		return getKey(target, targetKeys);
	}

	private Object getKey(BaseRow input, List<Integer> keys) {
		if (keys.size() == 1) {
			int keyIdx = keys.get(0);
			if (!input.isNullAt(keyIdx)) {
				return input.get(keyIdx, keyTypes.get(0));
			}
			return null;
		} else {
			GenericRow key = new GenericRow(keys.size());
			for (int i = 0; i < keys.size(); i++) {
				int keyIdx = keys.get(i);
				Object field = null;
				if (!input.isNullAt(keyIdx)) {
					field = input.get(keyIdx, keyTypes.get(i));
				}
				if (field == null) {
					return null;
				}
				key.update(i, field);
			}
			return key;
		}
	}

	public CsvRowFetcher setFieldDelim(String fieldDelim) {
		this.fieldDelim = fieldDelim;
		return this;
	}

	public CsvRowFetcher setRowDelim(String rowDelim) {
		this.rowDelim = rowDelim;
		return this;
	}

	public CsvRowFetcher setCharsetName(String charsetName) {
		this.charsetName = charsetName;
		return this;
	}

	public CsvRowFetcher setQuoteCharacter(Character quoteCharacter) {
		this.quoteCharacter = quoteCharacter;
		return this;
	}

	public CsvRowFetcher setIgnoreFirstLine(Boolean ignoreFirstLine) {
		this.ignoreFirstLine = ignoreFirstLine;
		return this;
	}

	public CsvRowFetcher setIgnoreComments(String ignoreComments) {
		this.ignoreComments = ignoreComments;
		return this;
	}

	public CsvRowFetcher setLenient(Boolean lenient) {
		this.lenient = lenient;
		return this;
	}

}
