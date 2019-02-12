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

import org.apache.flink.api.common.io.LocatableInputSplitAssigner;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.hadoop.common.HadoopInputFormatCommonBase;
import org.apache.flink.api.java.hadoop.mapred.wrapper.HadoopDummyReporter;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.sources.Partition;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configurable;
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
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.DEFAULT_LIST_COLUMN_TYPES_SEPARATOR;
import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR;

/**
 * The HiveTableInputFormat are inspired by the HCatInputFormat and HadoopInputFormatBase.
 */
public class HiveTableInputFormat extends HadoopInputFormatCommonBase<BaseRow, HiveTableInputSplit>
		implements ResultTypeQueryable {
	private static final long serialVersionUID = 6351448428766433164L;
	private static Logger logger = LoggerFactory.getLogger(HiveTableInputFormat.class);

	// Mutexes to avoid concurrent operations on Hadoop InputFormats.
	// Hadoop parallelizes tasks across JVMs which is why they might rely on this JVM isolation.
	// In contrast, Flink parallelizes using Threads, so multiple Hadoop InputFormat instances
	// might be used in the same JVM.
	private static final Object OPEN_MUTEX = new Object();
	private static final Object CONFIGURE_MUTEX = new Object();
	private static final Object CLOSE_MUTEX = new Object();

	private JobConf jobConf;

	protected transient Writable key;
	protected transient Writable value;

	private transient RecordReader<Writable, Writable> recordReader;
	protected transient boolean fetched = false;
	protected transient boolean hasNext;

	private Boolean isPartitioned;
	private RowTypeInfo rowTypeInfo;

	// Necessary info to init deserializer
	private String[] partitionColNames;
	private List<Partition> partitions;
	private transient Deserializer deserializer;
	private transient List<? extends StructField> fieldRefs;
	private transient StructObjectInspector oi;
	private transient InputFormat mapredInputFormat;
	private transient HiveTablePartition hiveTablePartition;
	private transient GenericRow reuse;

	public HiveTableInputFormat(
			JobConf jobConf,
			Boolean isPartitioned,
			String[] partitionColNames,
			List<Partition> partitions,
			RowTypeInfo rowTypeInfo) {
		super(jobConf.getCredentials());
		this.rowTypeInfo = rowTypeInfo;
		this.jobConf = jobConf;
		this.isPartitioned = isPartitioned;
		this.partitionColNames = partitionColNames;
		this.partitions = partitions;
	}

	@Override
	public void open(HiveTableInputSplit split) throws IOException {
		this.hiveTablePartition = split.getHiveTablePartition();
		StorageDescriptor sd = hiveTablePartition.getStorageDescriptor();
		jobConf.set(INPUT_DIR, sd.getLocation());
		try {
			this.mapredInputFormat = (org.apache.hadoop.mapred.InputFormat)
				Class.forName(sd.getInputFormat(), true, Thread.currentThread().getContextClassLoader()).newInstance();
		} catch (Exception e) {
			throw new RuntimeException("Unable to instantiate the hadoop input format", e);
		}
		ReflectionUtils.setConf(mapredInputFormat, jobConf);
		// enforce sequential configuration() calls
		synchronized (CONFIGURE_MUTEX) {
			// configure MR InputFormat if necessary
			if (this.mapredInputFormat instanceof Configurable) {
				((Configurable) this.mapredInputFormat).setConf(this.jobConf);
			} else if (this.mapredInputFormat instanceof JobConfigurable) {
				((JobConfigurable) this.mapredInputFormat).configure(this.jobConf);
			}
		}
		// enforce sequential open() calls
		synchronized (OPEN_MUTEX) {

			this.recordReader = this.mapredInputFormat.getRecordReader(split.getHadoopInputSplit(),
				jobConf, new HadoopDummyReporter());
			if (this.recordReader instanceof Configurable) {
				((Configurable) this.recordReader).setConf(jobConf);
			}
			key = this.recordReader.createKey();
			value = this.recordReader.createValue();
			this.fetched = false;
		}
		try {
			deserializer = (Deserializer) Class.forName(sd.getSerdeInfo().getSerializationLib()).newInstance();
			Configuration conf = new Configuration();
			Properties properties = createPropertiesFromStorageDescriptor(sd);
			SerDeUtils.initializeSerDe(deserializer, conf, properties, null);
			// Get the row structure
			oi = (StructObjectInspector) deserializer.getObjectInspector();
			fieldRefs = oi.getAllStructFieldRefs();
		} catch (Exception e) {
			logger.error("Error happens when deserialize from storage file.");
			throw new RuntimeException(e);
		}
		reuse = new GenericRow(rowTypeInfo.getArity());
	}

	@Override
	public HiveTableInputSplit[] createInputSplits(int minNumSplits)
			throws IOException {
		List<HiveTableInputSplit> hiSplit = new ArrayList<>();
		int splitNum = 0;
		for (Partition partition : partitions) {
			HiveTablePartition tablePartition = (HiveTablePartition) partition;
			StorageDescriptor sd = tablePartition.getStorageDescriptor();
			InputFormat format;
			try {
				format = (org.apache.hadoop.mapred.InputFormat)
					Class.forName(sd.getInputFormat(), true, Thread.currentThread().getContextClassLoader()).newInstance();
			} catch (Exception e) {
				throw new RuntimeException("Unable to instantiate the hadoop input format", e);
			}
			ReflectionUtils.setConf(format, jobConf);
			jobConf.set(INPUT_DIR, sd.getLocation());
			//TODO: we should consider how to calculate the splits according to minNumSplits in the future.
			org.apache.hadoop.mapred.InputSplit[] splitArray = format.getSplits(jobConf, minNumSplits);
			for (int i = 0; i < splitArray.length; i++) {
				hiSplit.add(new HiveTableInputSplit(splitNum++, splitArray[i], jobConf, tablePartition));
			}
		}

		return hiSplit.toArray(new HiveTableInputSplit[hiSplit.size()]);
	}

	@Override
	public void configure(org.apache.flink.configuration.Configuration parameters) {

	}

	// Todo: refactor code to get statistics from hms or partition.
	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStats) throws IOException {
		// only gather base statistics for FileInputFormats
		if (!(mapredInputFormat instanceof FileInputFormat)) {
			return null;
		}

		final org.apache.flink.api.common.io.FileInputFormat.FileBaseStatistics cachedFileStats =
				(cachedStats instanceof org.apache.flink.api.common.io.FileInputFormat.FileBaseStatistics) ?
				(org.apache.flink.api.common.io.FileInputFormat.FileBaseStatistics) cachedStats : null;

		try {
			final org.apache.hadoop.fs.Path[] paths = FileInputFormat.getInputPaths(this.jobConf);

			return getFileStats(cachedFileStats, paths, new ArrayList<FileStatus>(1));
		} catch (IOException ioex) {
			if (logger.isWarnEnabled()) {
				logger.warn("Could not determine statistics due to an io error: "
							+ ioex.getMessage());
			}
		} catch (Throwable t) {
			if (logger.isErrorEnabled()) {
				logger.error("Unexpected problem while getting the file statistics: " + t.getMessage(), t);
			}
		}

		// no statistics available
		return null;
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(HiveTableInputSplit[] inputSplits) {
		return new LocatableInputSplitAssigner(inputSplits);
	}

	@Override
	public boolean reachedEnd() throws IOException {
		if (!fetched) {
			fetchNext();
		}
		return !hasNext;
	}

	@Override
	public void close() throws IOException {
		if (this.recordReader != null) {
			// enforce sequential close() calls
			synchronized (CLOSE_MUTEX) {
				this.recordReader.close();
			}
		}
	}

	protected void fetchNext() throws IOException {
		hasNext = this.recordReader.next(key, value);
		fetched = true;
	}

	@Override
	public BaseRow nextRecord(BaseRow ignore) throws IOException {
		if (!this.fetched) {
			fetchNext();
		}
		if (!this.hasNext) {
			return null;
		}
		try {
			Object o = deserializer.deserialize(value);
			int index = 0;
			for (; index < fieldRefs.size(); index++) {
				StructField fref = fieldRefs.get(index);
				reuse.update(index, HiveRecordSerDe.serializeField(
							oi.getStructFieldData(o, fref),
							fref.getFieldObjectInspector()));
			}
			if (isPartitioned) {
				for (String partition : partitionColNames){
					reuse.update(index++, hiveTablePartition.getPartitionValues().get(partition));
				}
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
		return new BaseRowTypeInfo(rowTypeInfo.getFieldTypes(), rowTypeInfo.getFieldNames());
	}

	// --------------------------------------------------------------------------------------------
	//  Custom serialization methods
	// --------------------------------------------------------------------------------------------

	private void writeObject(ObjectOutputStream out) throws IOException {
		super.write(out);
		jobConf.write(out);
		out.writeObject(isPartitioned);
		out.writeObject(rowTypeInfo);

		out.writeObject(partitionColNames);
		out.writeObject(partitions);
	}

	@SuppressWarnings("unchecked")
	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		super.read(in);
		if (jobConf == null) {
			jobConf = new JobConf();
		}
		jobConf.readFields(in);
		jobConf.getCredentials().addAll(this.credentials);
		Credentials currentUserCreds = getCredentialsFromUGI(UserGroupInformation.getCurrentUser());
		if (currentUserCreds != null) {
			jobConf.getCredentials().addAll(currentUserCreds);
		}
		isPartitioned = (Boolean) in.readObject();
		rowTypeInfo = (RowTypeInfo) in.readObject();

		partitionColNames = (String[]) in.readObject();
		partitions = (List<Partition>) in.readObject();
	}

	// --------------------------------------------------------------------------------------------
	//  Helper methods
	// --------------------------------------------------------------------------------------------

	private static Properties createPropertiesFromStorageDescriptor(StorageDescriptor storageDescriptor) {
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

	private org.apache.flink.api.common.io.FileInputFormat.FileBaseStatistics getFileStats(
			org.apache.flink.api.common.io.FileInputFormat.FileBaseStatistics cachedStats, org.apache.hadoop.fs.Path[] hadoopFilePaths,
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

		return new org.apache.flink.api.common.io.FileInputFormat.FileBaseStatistics(latestModTime, len,
																					BaseStatistics.AVG_RECORD_BYTES_UNKNOWN);
	}

	/**
	 * Use this class to build HiveTableInputFormat.
	 */
	public static class Builder {
		private RowTypeInfo rowTypeInfo;
		private JobConf jobConf;
		private Boolean isPartitioned;
		private String[] partitionColNames;
		private List<Partition> partitions;

		public Builder(RowTypeInfo rowTypeInfo, JobConf jobConf, String dbName, String tableName, Boolean
				isPartitioned, String[] partitionColNames, List<Partition> partitions) {
			this.rowTypeInfo = rowTypeInfo;
			this.jobConf = jobConf;
			this.isPartitioned = isPartitioned;
			this.partitionColNames = partitionColNames;
			this.partitions = partitions;
		}

		public HiveTableInputFormat build() {
			try {
				return new HiveTableInputFormat(jobConf, isPartitioned, partitionColNames, partitions, rowTypeInfo);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}
}
