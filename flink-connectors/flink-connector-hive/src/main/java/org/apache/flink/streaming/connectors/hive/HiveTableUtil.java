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

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.catalog.hive.HiveCatalogUtil;
import org.apache.flink.table.types.TypeConverters;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.DEFAULT_LIST_COLUMN_TYPES_SEPARATOR;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_COMPRESSED;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_INPUT_FORMAT;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_LOCATION;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_NUM_BUCKETS;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_OUTPUT_FORMAT;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_SERDE_LIBRARY;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_STORAGE_SERIALIZATION_FORMAT;

/**
 * Utils for meta objects and data types conversion between Flink and Hive.
 */
public class HiveTableUtil {
	/**
	 * Convert partition value string to actual type in flink.
	 * @param partitionValue
	 * @param clazz
	 * @return
	 */
	public static Object getActualObjectFromString(String partitionValue, Class clazz) {
		Object partitionObject = null;
		if (String.class.equals(clazz)) {
			partitionObject = partitionValue;
		} else if (Short.class.equals(clazz)) {
			partitionObject = Short.parseShort(partitionValue);
		} else if (Integer.class.equals(clazz)) {
			partitionObject = Integer.parseInt(partitionValue);
		} else if (Long.class.equals(clazz)) {
			partitionObject = Long.parseLong(partitionValue);
		} else if (Float.class.equals(clazz)) {
			partitionObject = Float.parseFloat(partitionValue);
		} else if (Double.class.equals(clazz)) {
			partitionObject = Double.parseDouble(partitionValue);
		} else if (Boolean.class.equals(clazz)) {
			partitionObject = Boolean.parseBoolean(partitionValue);
		} else if (Timestamp.class.equals(clazz)) {
			partitionObject = Timestamp.parse(partitionValue);
		} else if (Date.class.equals(clazz)) {
			partitionObject = Date.parse(partitionValue);
		} else if (Time.class.equals(clazz)) {
			partitionObject = Time.parse(partitionValue);
		} else if (BigDecimal.class.equals(clazz)) {
			partitionObject = new BigDecimal(partitionValue);
		} else if (BigInteger.class.equals(clazz)) {
			partitionObject = new BigInteger(partitionValue);
		}
		return partitionObject;
	}

	public static ObjectInspector getObjectInspector(Class clazz) throws IOException {
		TypeInfo typeInfo = null;
		if (String.class.equals(clazz)) {
			typeInfo = TypeInfoFactory.stringTypeInfo;
		} else if (Short.class.equals(clazz)) {
			typeInfo = TypeInfoFactory.shortTypeInfo;
		} else if (Integer.class.equals(clazz)) {
			typeInfo = TypeInfoFactory.intTypeInfo;
		} else if (Long.class.equals(clazz)) {
			typeInfo = TypeInfoFactory.longTypeInfo;
		} else if (Float.class.equals(clazz)) {
			typeInfo = TypeInfoFactory.floatTypeInfo;
		} else if (Double.class.equals(clazz)) {
			typeInfo = TypeInfoFactory.doubleTypeInfo;
		} else if (Boolean.class.equals(clazz)) {
			typeInfo = TypeInfoFactory.booleanTypeInfo;
		} else if (Timestamp.class.equals(clazz)) {
			typeInfo = TypeInfoFactory.timestampTypeInfo;
		} else if (Date.class.equals(clazz)) {
			typeInfo = TypeInfoFactory.dateTypeInfo;
		} else if (BigDecimal.class.equals(clazz)) {
			typeInfo = TypeInfoFactory.decimalTypeInfo;
		} else if (BigInteger.class.equals(clazz)) {
			typeInfo = TypeInfoFactory.longTypeInfo;
		} else {
			throw new IOException("Not supported convert class " + String.valueOf(clazz));
		}
		return getObjectInspector(typeInfo);
	}

	private static ObjectInspector getObjectInspector(TypeInfo type) throws IOException {

		switch (type.getCategory()) {

			case PRIMITIVE:
				PrimitiveTypeInfo primitiveType = (PrimitiveTypeInfo) type;
				return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(primitiveType);

			case MAP:
				MapTypeInfo mapType = (MapTypeInfo) type;
				MapObjectInspector mapInspector = ObjectInspectorFactory.getStandardMapObjectInspector(
						getObjectInspector(mapType.getMapKeyTypeInfo()), getObjectInspector(mapType.getMapValueTypeInfo()));
				return mapInspector;

			case LIST:
				ListTypeInfo listType = (ListTypeInfo) type;
				ListObjectInspector listInspector = ObjectInspectorFactory.getStandardListObjectInspector(
						getObjectInspector(listType.getListElementTypeInfo()));
				return listInspector;

			case STRUCT:
				StructTypeInfo structType = (StructTypeInfo) type;
				List<TypeInfo> fieldTypes = structType.getAllStructFieldTypeInfos();

				List<ObjectInspector> fieldInspectors = new ArrayList<ObjectInspector>();
				for (TypeInfo fieldType : fieldTypes) {
					fieldInspectors.add(getObjectInspector(fieldType));
				}

				StructObjectInspector structInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
						structType.getAllStructFieldNames(), fieldInspectors);
				return structInspector;

			default:
				throw new IOException("Unknown field schema type");
		}
	}

	/**
	 * Construct storageDescriptor from jobConf and rowTypeInfo.
	 * @param jobConf
	 * @param rowTypeInfo
	 * @return
	 */
	public static StorageDescriptor createStorageDescriptor(JobConf jobConf, RowTypeInfo rowTypeInfo) {
		StorageDescriptor storageDescriptor = new StorageDescriptor();
		storageDescriptor.setLocation(jobConf.get(HIVE_TABLE_LOCATION));
		storageDescriptor.setInputFormat(jobConf.get(HIVE_TABLE_INPUT_FORMAT));
		storageDescriptor.setOutputFormat(jobConf.get(HIVE_TABLE_OUTPUT_FORMAT));
		storageDescriptor.setCompressed(Boolean.parseBoolean(jobConf.get(HIVE_TABLE_COMPRESSED)));
		storageDescriptor.setNumBuckets(Integer.parseInt(jobConf.get(HIVE_TABLE_NUM_BUCKETS)));

		SerDeInfo serDeInfo = new SerDeInfo();
		serDeInfo.setSerializationLib(jobConf.get(HIVE_TABLE_SERDE_LIBRARY));
		Map<String, String> parameters = new HashMap<>();
		parameters.put(serdeConstants.SERIALIZATION_FORMAT, jobConf.get(HIVE_TABLE_STORAGE_SERIALIZATION_FORMAT));
		serDeInfo.setParameters(parameters);
		List<FieldSchema> fieldSchemas = new ArrayList<>();
		for (int i = 0; i < rowTypeInfo.getArity(); i++) {
			String hiveType = HiveCatalogUtil.convert(TypeConverters.createInternalTypeFromTypeInfo(rowTypeInfo.getFieldTypes()[i]));
			fieldSchemas.add(new FieldSchema(rowTypeInfo.getFieldNames()[i], hiveType, null));
		}
		storageDescriptor.setCols(fieldSchemas);
		storageDescriptor.setSerdeInfo(serDeInfo);
		return storageDescriptor;
	}

	// --------------------------------------------------------------------------------------------
	//  Helper methods
	// --------------------------------------------------------------------------------------------

	public static Properties createPropertiesFromStorageDescriptor(StorageDescriptor storageDescriptor) {
		SerDeInfo serDeInfo = storageDescriptor.getSerdeInfo();
		Map<String, String> parameters = serDeInfo.getParameters();
		Properties properties = new Properties();
		properties.setProperty(serdeConstants.SERIALIZATION_FORMAT,
							parameters.get(serdeConstants.SERIALIZATION_FORMAT));
		List<String> colTypes = new ArrayList<>();
		List<String> colNames = new ArrayList<>();
		List<FieldSchema> cols = storageDescriptor.getCols();
		for (FieldSchema col: cols){
			colTypes.add(col.getType());
			colNames.add(col.getName());
		}
		properties.setProperty(serdeConstants.LIST_COLUMNS, StringUtils.join(colNames, ","));
		properties.setProperty(serdeConstants.COLUMN_NAME_DELIMITER, ",");
		properties.setProperty(serdeConstants.LIST_COLUMN_TYPES, StringUtils.join(colTypes, DEFAULT_LIST_COLUMN_TYPES_SEPARATOR));
		properties.setProperty(serdeConstants.SERIALIZATION_NULL_FORMAT, "NULL");
		properties.putAll(parameters);
		return properties;
	}

	public static FileInputFormat.FileBaseStatistics getFileStats(
			FileInputFormat.FileBaseStatistics cachedStats, org.apache.hadoop.fs.Path[] hadoopFilePaths,
			ArrayList<FileStatus> files) throws IOException {

		long latestModTime = 0L;

		// get the file info and check whether the cached statistics are still valid.
		for (org.apache.hadoop.fs.Path hadoopPath : hadoopFilePaths) {

			final Path filePath = new Path(hadoopPath.toUri());
			final FileSystem fs = FileSystem.get(filePath.toUri());

			final FileStatus file = fs.getFileStatus(filePath);
			latestModTime = Math.max(latestModTime, file.getModificationTime());

			// enumerate all files and check their modification time stamp.
			if (file.isDir()) {
				FileStatus[] fss = fs.listStatus(filePath);
				files.ensureCapacity(files.size() + fss.length);

				for (FileStatus s : fss) {
					if (!s.isDir()) {
						files.add(s);
						latestModTime = Math.max(s.getModificationTime(), latestModTime);
					}
				}
			} else {
				files.add(file);
			}
		}

		// check whether the cached statistics are still valid, if we have any
		if (cachedStats != null && latestModTime <= cachedStats.getLastModificationTime()) {
			return cachedStats;
		}
		// calculate the whole length
		long len = 0;
		for (FileStatus s : files) {
			len += s.getLen();
		}
		// sanity check
		if (len <= 0) {
			len = BaseStatistics.SIZE_UNKNOWN;
		}
		return new FileInputFormat.FileBaseStatistics(latestModTime, len, BaseStatistics.AVG_RECORD_BYTES_UNKNOWN);
	}

}
