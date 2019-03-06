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

import org.apache.flink.api.common.io.FinalizeOnMaster;
import org.apache.flink.api.java.hadoop.common.HadoopOutputFormatCommonBase;
import org.apache.flink.api.java.hadoop.mapred.wrapper.HadoopDummyProgressable;
import org.apache.flink.api.java.hadoop.mapred.wrapper.HadoopDummyReporter;
import org.apache.flink.api.java.hadoop.mapreduce.utils.HadoopUtils;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.runtime.conversion.DataStructureConverters;
import org.apache.flink.table.types.InternalType;
import org.apache.flink.table.types.TypeConverters;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.JobContextImpl;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapred.TaskAttemptContextImpl;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.apache.flink.api.java.hadoop.common.HadoopInputFormatCommonBase.getCredentialsFromUGI;
import static org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.OUTDIR;

/**
 * HiveTableOutputFormat used to write data to hive table.
 */
public class HiveTableOutputFormat extends HadoopOutputFormatCommonBase<BaseRow> implements FinalizeOnMaster {

	private static final Logger logger = LoggerFactory.getLogger(HiveTableOutputFormat.class);

	private static final long serialVersionUID = 1L;

	// Mutexes to avoid concurrent operations on Hadoop OutputFormats.
	// Hadoop parallelizes tasks across JVMs which is why they might rely on this JVM isolation.
	// In contrast, Flink parallelizes using Threads, so multiple Hadoop OutputFormat instances
	// might be used in the same JVM.
	protected static final Object OPEN_MUTEX = new Object();
	protected static final Object CONFIGURE_MUTEX = new Object();
	protected static final Object CLOSE_MUTEX = new Object();

	private JobConf jobConf;

	private Boolean isPartitioned;
	private RowTypeInfo rowTypeInfo;

	// Necessary info to init deserializer
	private String[] partitionColNames;
	private transient AbstractSerDe serializer;
	private transient List<? extends StructField> fieldRefs;
	//StructObjectInspector represents the hive row structure.
	private transient StructObjectInspector sois;
	private transient HiveTablePartition hiveTablePartition;
	protected OutputFormat mapredOutputFormat;
	protected transient RecordWriter recordWriter;
	protected transient OutputCommitter outputCommitter;
	protected transient TaskAttemptContext context;
	private transient InternalType[] fieldTypes;
	private DataStructureConverters.DataStructureConverter[] converters;

	public HiveTableOutputFormat(JobConf jobConf,
								Boolean isPartitioned,
								String[] partitionColNames,
								RowTypeInfo rowTypeInfo,
								HiveTablePartition hiveTablePartition) {
		super(jobConf.getCredentials());
		HadoopUtils.mergeHadoopConf(jobConf);
		this.isPartitioned = isPartitioned;
		this.partitionColNames = partitionColNames;
		this.rowTypeInfo = rowTypeInfo;
		this.jobConf = jobConf;
		this.hiveTablePartition = hiveTablePartition;
	}

	public JobConf getJobConf() {
		return jobConf;
	}

	// --------------------------------------------------------------------------------------------
	//  OutputFormat
	// --------------------------------------------------------------------------------------------

	@Override
	public void configure(Configuration parameters) {

		// enforce sequential configure() calls
		synchronized (CONFIGURE_MUTEX) {
			// configure MR OutputFormat if necessary
			if (this.mapredOutputFormat instanceof Configurable) {
				((Configurable) this.mapredOutputFormat).setConf(this.jobConf);
			} else if (this.mapredOutputFormat instanceof JobConfigurable) {
				((JobConfigurable) this.mapredOutputFormat).configure(this.jobConf);
			}
		}
	}

	/**
	 * create the temporary output file for hadoop RecordWriter.
	 * @param taskNumber The number of the parallel instance.
	 * @param numTasks The number of parallel tasks.
	 * @throws java.io.IOException
	 */
	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		StorageDescriptor sd = hiveTablePartition.getStorageDescriptor();
		jobConf.set(OUTDIR, sd.getLocation());
		try {
			this.mapredOutputFormat = (OutputFormat) Class.forName(sd.getOutputFormat(), true,
																Thread.currentThread().getContextClassLoader()).newInstance();
		} catch (Exception e) {
			throw new RuntimeException("Unable to instantiate the hadoop output format", e);
		}
		ReflectionUtils.setConf(mapredOutputFormat, jobConf);

		// enforce sequential open() calls
		synchronized (OPEN_MUTEX) {
			if (Integer.toString(taskNumber + 1).length() > 6) {
				throw new IOException("Task id too large.");
			}

			TaskAttemptID taskAttemptID = TaskAttemptID.forName("attempt__0000_r_"
																+ String.format("%" + (6 - Integer.toString(taskNumber + 1).length()) + "s", " ").replace(" ", "0")
																+ Integer.toString(taskNumber + 1)
																+ "_0");

			this.jobConf.set("mapred.task.id", taskAttemptID.toString());
			this.jobConf.setInt("mapred.task.partition", taskNumber + 1);
			// for hadoop 2.2
			this.jobConf.set("mapreduce.task.attempt.id", taskAttemptID.toString());
			this.jobConf.setInt("mapreduce.task.partition", taskNumber + 1);

			this.context = new TaskAttemptContextImpl(this.jobConf, taskAttemptID);

			this.outputCommitter = this.jobConf.getOutputCommitter();

			JobContext jobContext = new JobContextImpl(this.jobConf, new JobID());

			this.outputCommitter.setupJob(jobContext);

			this.recordWriter = this.mapredOutputFormat.getRecordWriter(null, this.jobConf, Integer.toString(taskNumber + 1), new HadoopDummyProgressable());
		}
		try {
			serializer = (AbstractSerDe) Class.forName(sd.getSerdeInfo().getSerializationLib()).newInstance();
			ReflectionUtils.setConf(serializer, context.getConfiguration());
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
			Properties properties = HiveTableUtil.createPropertiesFromStorageDescriptor(sd);
			SerDeUtils.initializeSerDe(serializer, conf, properties, null);
			// Get the row structure
			StructObjectInspector soi = (StructObjectInspector) serializer.getObjectInspector();
			fieldRefs = soi.getAllStructFieldRefs();
		} catch (Exception e) {
			logger.error("Error happens when deserialize from storage file.");
			throw new RuntimeException(e);
		}
		fieldTypes = new InternalType[rowTypeInfo.getArity()];
		List<ObjectInspector> objectInspectors = new ArrayList<>();
		for (int i = 0; i < fieldTypes.length; i++) {
			fieldTypes[i] = TypeConverters.createInternalTypeFromTypeInfo(rowTypeInfo.getTypeAt(i));
			objectInspectors.add(HiveTableUtil.getObjectInspector(rowTypeInfo.getTypeAt(i).getTypeClass()));
		}
		converters = new DataStructureConverters.DataStructureConverter[fieldTypes.length];
		sois = ObjectInspectorFactory.getStandardStructObjectInspector(Arrays.asList(rowTypeInfo.getFieldNames()),
																	objectInspectors);
	}

	/**
	 * commit the task by moving the output file out from the temporary directory.
	 * @throws java.io.IOException
	 */
	@Override
	public void close() throws IOException {

		// enforce sequential close() calls
		synchronized (CLOSE_MUTEX) {
			this.recordWriter.close(new HadoopDummyReporter());

			if (this.outputCommitter.needsTaskCommit(this.context)) {
				this.outputCommitter.commitTask(this.context);
			}
		}
	}

	@Override
	public void finalizeGlobal(int parallelism) throws IOException {
		try {
			StorageDescriptor sd = hiveTablePartition.getStorageDescriptor();
			jobConf.set(OUTDIR, sd.getLocation());
			JobContext jobContext = new JobContextImpl(this.jobConf, new JobID());
			OutputCommitter outputCommitter = this.jobConf.getOutputCommitter();

			// finalize HDFS output format
			outputCommitter.commitJob(jobContext);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Custom serialization methods
	// --------------------------------------------------------------------------------------------

	private void writeObject(ObjectOutputStream out) throws IOException {
		super.write(out);
		jobConf.write(out);
		out.writeObject(isPartitioned);
		out.writeObject(rowTypeInfo);
		out.writeObject(hiveTablePartition);
		out.writeObject(partitionColNames);
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
		hiveTablePartition = (HiveTablePartition) in.readObject();
		partitionColNames = (String[]) in.readObject();
	}

	@Override
	public void writeRecord(BaseRow record) throws IOException {
		try {
			this.recordWriter.write(null, serializer.serialize(serializeField(record), sois));
		} catch (IOException | SerDeException e) {
			throw new IOException("Could not write Record.", e);
		}
	}

	private Object serializeField(BaseRow record) {
		List<Object> res = new ArrayList<>(record.getArity());
		for (int i = 0; i < record.getArity(); i++) {
			if (converters[i] == null) {
				converters[i] = DataStructureConverters.getConverterForType(fieldTypes[i]);
			}
			res.add(converters[i].toExternal(record, i));
		}
		return res;
	}
}
