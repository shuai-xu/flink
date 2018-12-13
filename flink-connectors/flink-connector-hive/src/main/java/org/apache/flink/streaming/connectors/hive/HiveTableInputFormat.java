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

package org.apache.flink.streaming.connectors.hive;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.hadoop.mapred.HadoopInputFormatBase;
import org.apache.flink.api.java.hadoop.mapred.wrapper.HadoopInputSplit;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.catalog.hive.HiveCatalogConfig;
import org.apache.flink.table.catalog.hive.TypeConverterUtil;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hive.hcatalog.data.HCatRecordSerDe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR;

/**
 * The HiveTableInputFormat are inspired by the HCatInputFormat..
 */
public class HiveTableInputFormat extends HadoopInputFormatBase<Writable, Writable, GenericRow>
		implements ResultTypeQueryable {
	private static Logger logger = LoggerFactory.getLogger(HiveTableInputFormat.class);
	private final RowTypeInfo rowTypeInfo;
	// Necessary info to init deserializer
	private final Properties properties;
	private final String serDeInfoClass;
	private transient Deserializer deserializer;
	private transient List<? extends StructField> fieldRefs;
	private transient StructObjectInspector oi;

	public HiveTableInputFormat(
			InputFormat mapredInputFormat,
			JobConf jobConf,
			RowTypeInfo rowTypeInfo,
			Properties properties,
			String serDeInfoClass) {
		super(mapredInputFormat, Writable.class, Writable.class, jobConf);
		this.rowTypeInfo = rowTypeInfo;
		this.properties = properties;
		this.serDeInfoClass = serDeInfoClass;
	}

	@Override
	public void open(HadoopInputSplit split) throws IOException {
		try {
			deserializer = (Deserializer) Class.forName(serDeInfoClass).newInstance();
			Configuration conf = new Configuration();
			SerDeUtils.initializeSerDe(deserializer, conf, properties, null);
			// Get the row structure
			oi = (StructObjectInspector) deserializer.getObjectInspector();
			fieldRefs = oi.getAllStructFieldRefs();
		} catch (Exception e) {
			logger.error("Error happens when deserialize from storage file.");
			throw new RuntimeException(e);
		}
		super.open(split);
	}

	@Override
	public GenericRow nextRecord(GenericRow reuse) throws IOException {
		if (!this.fetched) {
			fetchNext();
		}
		if (!this.hasNext) {
			return null;
		}
		try {
			Object o = deserializer.deserialize(value);
			for (int i = 0; i < fieldRefs.size(); i++) {
				StructField fref = fieldRefs.get(i);
				reuse.update(i, HCatRecordSerDe.serializeField(oi.getStructFieldData(o, fref), fref
						.getFieldObjectInspector()));
			}
		} catch (Exception e){
			logger.error("Error happens when converting hive data type to flink data type.");
			throw new RuntimeException(e);
		}
		this.fetched = false;
		return reuse;
	}

	@Override
	public TypeInformation getProducedType() {
		return new BaseRowTypeInfo(GenericRow.class, rowTypeInfo.getFieldTypes(), rowTypeInfo.getFieldNames());
	}

	/**
	 * Use this  class to build HiveTableInputFormat.
	 */
	public static class Builder {
		private final RowTypeInfo rowTypeInfo;
		private final JobConf jobConf;

		public Builder(RowTypeInfo rowTypeInfo, JobConf jobConf) {
			this.rowTypeInfo = rowTypeInfo;
			this.jobConf = jobConf;
		}

		public HiveTableInputFormat build() {
			try {
				StorageDescriptor storageDescriptor = createStorageDescriptor(jobConf, rowTypeInfo);
				jobConf.setStrings(INPUT_DIR, storageDescriptor.getLocation());
				InputFormat inputFormat = (InputFormat) Class.forName(storageDescriptor.getInputFormat()).newInstance();

				SerDeInfo serDeInfo = storageDescriptor.getSerdeInfo();
				Properties properties = new Properties();
				properties.setProperty(serdeConstants.SERIALIZATION_FORMAT,
									serDeInfo.getParameters().get(serdeConstants.SERIALIZATION_FORMAT));
				properties.setProperty(serdeConstants.LIST_COLUMNS,
									StringUtils.join(rowTypeInfo.getFieldNames(), ","));
				String[] colTypes = new String[rowTypeInfo.getArity()];
				List<FieldSchema> cols = storageDescriptor.getCols();
				int t = 0;
				for (FieldSchema col: cols){
					colTypes[t++] = col.getType();
				}
				properties.setProperty(serdeConstants.LIST_COLUMN_TYPES, StringUtils.join(colTypes, ":"));
				properties.setProperty(serdeConstants.SERIALIZATION_NULL_FORMAT, "NULL");

				return new HiveTableInputFormat(inputFormat, jobConf, rowTypeInfo, properties,
												serDeInfo.getSerializationLib());
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	private static StorageDescriptor createStorageDescriptor(JobConf jobConf, RowTypeInfo rowTypeInfo) {
		StorageDescriptor storageDescriptor = new StorageDescriptor();
		storageDescriptor.setLocation(jobConf.get(HiveCatalogConfig.HIVE_TABLE_LOCATION));
		storageDescriptor.setInputFormat(jobConf.get(HiveCatalogConfig.HIVE_TABLE_INPUT_FORMAT));
		storageDescriptor.setOutputFormat(jobConf.get(HiveCatalogConfig.HIVE_TABLE_OUTPUT_FORMAT));
		storageDescriptor.setCompressed(Boolean.parseBoolean(jobConf.get(HiveCatalogConfig.HIVE_TABLE_COMPRESSED)));
		storageDescriptor.setNumBuckets(Integer.parseInt(jobConf.get(HiveCatalogConfig.HIVE_TABLE_NUM_BUCKETS)));

		SerDeInfo serDeInfo = new SerDeInfo();
		serDeInfo.setSerializationLib(jobConf.get(HiveCatalogConfig.HIVE_TABLE_SERDE_LIBRARY));
		Map<String, String> parameters = new HashMap<>();
		parameters.put(serdeConstants.SERIALIZATION_FORMAT, jobConf.get(HiveCatalogConfig.HIVE_TABLE_STORAGE_SERIALIZATION_FORMAT));
		serDeInfo.setParameters(parameters);
		List<FieldSchema> fieldSchemas = new ArrayList<>();
		for (int i = 0; i < rowTypeInfo.getArity(); i++) {
			String hiveType = TypeConverterUtil.flinkTypeToHiveType.get((rowTypeInfo.getFieldTypes()[i]));
			if (null == hiveType) {
				logger.error("Now we don't support flink type of " + rowTypeInfo.getFieldTypes()[i] + " converting from " +
							"hive");
				throw new RuntimeException("Now we don't support flink type of "
										+ rowTypeInfo.getFieldTypes()[i] + " converting from hive");
			}
			fieldSchemas.add(
					new FieldSchema(rowTypeInfo.getFieldNames()[i], hiveType, ""));
		}
		storageDescriptor.setCols(fieldSchemas);
		storageDescriptor.setSerdeInfo(serDeInfo);
		return storageDescriptor;
	}
}
