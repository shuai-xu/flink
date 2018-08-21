/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.graph;

import org.apache.flink.api.common.CheckpointMode;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobType;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.StreamTransformation;
import org.apache.flink.streaming.config.JobConfigurer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Properties using to generate a StreamGraph.
 * Can be built from StreamExecutionEnvironment or batch plan.
 */
public class StreamGraphProperties implements java.io.Serializable {

	private ExecutionConfig executionConfig;
	private CheckpointConfig checkpointConfig;
	private TimeCharacteristic timeCharacteristic;
	private AbstractStateBackend stateBackend;
	private boolean chainingEnabled;
	private boolean twoInputChainEnabled;
	private String jobName = StreamExecutionEnvironment.DEFAULT_JOB_NAME;
	private JobConfigurer jobConfigurer;
	private List<Tuple2<String, DistributedCache.DistributedCacheEntry>> cacheFile = new ArrayList<>();
	private ScheduleMode scheduleMode;
	private JobType jobType;
	private long bufferTimeout;
	private ResultPartitionType defaultResultPartitionType;
	private ShuffleProperties shuffleProperties;
	private Configuration configuration;

	/** This configuration is for custom parameters. */
	private final Configuration customConfiguration = new Configuration();

	public static StreamGraphProperties buildStreamProperties(StreamExecutionEnvironment env) {
		StreamGraphProperties streamGraphProperties = new StreamGraphProperties();

		streamGraphProperties.setJobType(JobType.INFINITE_STREAM);
		streamGraphProperties.setExecutionConfig(env.getConfig());
		streamGraphProperties.setCheckpointConfig(env.getCheckpointConfig());
		streamGraphProperties.setTimeCharacteristic(env.getStreamTimeCharacteristic());
		streamGraphProperties.setStateBackend(env.getStateBackend());
		streamGraphProperties.setChainingEnabled(env.isChainingEnabled());
		streamGraphProperties.setJobConfigurer(env.getJobConfigurer());
		streamGraphProperties.setCacheFiles(env.getCachedFiles());
		streamGraphProperties.setBufferTimeout(env.getBufferTimeout());
		streamGraphProperties.setDefaultResultPartitionType(ResultPartitionType.PIPELINED_BOUNDED);
		streamGraphProperties.setConfiguration(env.getConfiguration());

		// For infinite stream job, by default schedule tasks in eager mode
		streamGraphProperties.setScheduleMode(ScheduleMode.EAGER);
		Configuration flinkConf = GlobalConfiguration.loadConfiguration();

		if (flinkConf.contains(CoreOptions.ENABLE_TWO_INPUT_CHAINING)) {
			streamGraphProperties.setTwoInputChainEnabled(flinkConf.getBoolean(CoreOptions.ENABLE_TWO_INPUT_CHAINING));
		} else {
			streamGraphProperties.setTwoInputChainEnabled(false);
		}

		return streamGraphProperties;
	}

	public static StreamGraphProperties buildBatchProperties(StreamExecutionEnvironment env, ShuffleProperties shuffleProperties) {
		StreamGraphProperties streamGraphProperties = new StreamGraphProperties();

		streamGraphProperties.setJobType(JobType.FINITE_STREAM);
		env.getConfig().enableObjectReuse();
		env.getConfig().enableInputElementReuse();
		streamGraphProperties.setExecutionConfig(env.getConfig());
		CheckpointConfig checkpointConfig = new CheckpointConfig();
		checkpointConfig.setCheckpointMode(CheckpointMode.BATCH);
		streamGraphProperties.setCheckpointConfig(checkpointConfig);
		streamGraphProperties.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		streamGraphProperties.setChainingEnabled(true);
		streamGraphProperties.getExecutionConfig().setLatencyTrackingInterval(-1L);
		streamGraphProperties.setCacheFiles(env.getCachedFiles());
		streamGraphProperties.setBufferTimeout(-1L);
		streamGraphProperties.setDefaultResultPartitionType(ResultPartitionType.PIPELINED);
		streamGraphProperties.setShuffleProperties(shuffleProperties);
		streamGraphProperties.setConfiguration(env.getConfiguration());

		// For finite stream job, by default schedule tasks in lazily from sources mode
		streamGraphProperties.setScheduleMode(ScheduleMode.LAZY_FROM_SOURCES);
		Configuration flinkConf = GlobalConfiguration.loadConfiguration();

		if (flinkConf.contains(CoreOptions.ENABLE_TWO_INPUT_CHAINING)) {
			streamGraphProperties.setTwoInputChainEnabled(flinkConf.getBoolean(CoreOptions.ENABLE_TWO_INPUT_CHAINING));
		} else {
			streamGraphProperties.setTwoInputChainEnabled(true);
		}

		return streamGraphProperties;
	}

	public void setExecutionConfig(ExecutionConfig executionConfig) {
		this.executionConfig = executionConfig;
	}

	public void setCheckpointConfig(CheckpointConfig checkpointConfig) {
		this.checkpointConfig = checkpointConfig;
	}

	public void setTimeCharacteristic(TimeCharacteristic timeCharacteristic) {
		this.timeCharacteristic = timeCharacteristic;
	}

	public void setStateBackend(AbstractStateBackend stateBackend) {
		this.stateBackend = stateBackend;
	}

	public void setChainingEnabled(boolean chainingEnabled) {
		this.chainingEnabled = chainingEnabled;
	}

	public ExecutionConfig getExecutionConfig() {
		return executionConfig;
	}

	public CheckpointConfig getCheckpointConfig() {
		return checkpointConfig;
	}

	public TimeCharacteristic getTimeCharacteristic() {
		return timeCharacteristic;
	}

	public AbstractStateBackend getStateBackend() {
		return stateBackend;
	}

	public boolean isChainingEnabled() {
		return chainingEnabled;
	}

	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public void configureTransformations(List<StreamTransformation> transformations) {
		if (jobConfigurer != null) {
			jobConfigurer.configureTransformations(this, transformations);
		}
	}

	public void configureJobVertices(JobGraph jobGraph, StreamGraph streamGraph, Map<JobVertex, ArrayList<Integer>> vertexTransformations) {
		if (jobConfigurer != null) {
			jobConfigurer.configureJobVertices(jobGraph, streamGraph, vertexTransformations);
		}
	}

	public void setJobConfigurer(JobConfigurer jobConfigurer) {
		this.jobConfigurer = jobConfigurer;
	}

	public void setCacheFiles(List<Tuple2<String, DistributedCache.DistributedCacheEntry>> cacheFile) {
		this.cacheFile = cacheFile;
	}

	public List<Tuple2<String, DistributedCache.DistributedCacheEntry>> getCacheFiles() {
		return cacheFile;
	}

	public ScheduleMode getScheduleMode() {
		return scheduleMode;
	}

	public void setScheduleMode(ScheduleMode scheduleMode) {
		this.scheduleMode = scheduleMode;
	}

	public JobType getJobType() {
		return jobType;
	}

	public void setJobType(JobType jobType) {
		this.jobType = jobType;
	}

	public long getBufferTimeout() {
		return bufferTimeout;
	}

	public void setBufferTimeout(long bufferTimeout) {
		this.bufferTimeout = bufferTimeout;
	}

	public ResultPartitionType getDefaultResultPartitionType() {
		return defaultResultPartitionType;
	}

	public void setDefaultResultPartitionType(ResultPartitionType defaultResultPartitionType) {
		this.defaultResultPartitionType = defaultResultPartitionType;
	}

	public void setShuffleProperties(ShuffleProperties shuffleProperties) {
		this.shuffleProperties = shuffleProperties;
	}

	public int getShuffleMemorySize() {
		return shuffleProperties == null ? 0 : shuffleProperties.getShuffleMemorySize();
	}

	public Configuration getConfiguration() {
		return configuration;
	}

	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	public boolean isTwoInputChainEnabled() {
		return twoInputChainEnabled;
	}

	public void setTwoInputChainEnabled(boolean twoInputChainEnabled) {
		this.twoInputChainEnabled = twoInputChainEnabled;
	}

	public Configuration getCustomConfiguration() {
		return this.customConfiguration;
	}
}
