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

package com.alibaba.blink.launcher.autoconfig.connector;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction;
import org.apache.flink.table.api.InputPartitionSource;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.util.LinkedList;
import java.util.List;

/**
 * A simplified class, mostly copied from blink-connector, for IT test only.
 */
public abstract class AbstractParallelSource<T, CURSOR extends Serializable> extends InputFormatSourceFunction<T> implements ListCheckpointed<Tuple2<InputSplit, CURSOR>>, ResultTypeQueryable<T>, InputPartitionSource {
	static final Logger LOG = LoggerFactory.getLogger(AbstractParallelSource.class);
	static final long serialVersionUID = -7848357196819780804L;
	transient List<Tuple2<InputSplit, CURSOR>> initialProgress;
	boolean recoryFromState = false;
	boolean enableWatermarkEmitter = true;
	boolean disableParallelRead = false;
	boolean initInputSplitInMaster = true;

	public void disableWatermarkEmitter() {
		this.enableWatermarkEmitter = false;
	}

	public AbstractParallelSource() {
		super(null, null);
	}

	public void initOperator(Configuration config) throws IOException {
	}

	public abstract List<String> getPartitionList() throws Exception;

	public void open(Configuration config) throws IOException {
		this.initOperator(config);

		LOG.info("Init source succ.");
	}

	public void run(SourceContext<T> ctx) throws Exception {
	}

	public void close() throws IOException {
	}

	public void cancel() {
	}

	public TypeInformation<T> getProducedType() {
		ParameterizedType p = (ParameterizedType) this.getClass().getGenericSuperclass();
		return (TypeInformation<T>) TypeExtractor.createTypeInfo(p.getActualTypeArguments()[0]);
	}

	public List<Tuple2<InputSplit, CURSOR>> snapshotState(long checkpointId, long timestamp) throws Exception {
		List<Tuple2<InputSplit, CURSOR>> state = new LinkedList();
		return state;
	}

	public void restoreState(List<Tuple2<InputSplit, CURSOR>> state) throws Exception {
		LOG.info("Restoring state: {}", state);
		this.recoryFromState = true;
		if (state != null && !state.isEmpty()) {
			this.initialProgress = state;
		}
	}

	public void disableParallelRead() {
		this.disableParallelRead = true;
	}

	public void setInitInputSplitInMaster(boolean enabled) {
		this.initInputSplitInMaster = enabled;
	}

	public boolean getInitInputSplitInMaster() {
		return this.initInputSplitInMaster;
	}

	public void enableParallelRead() {
		this.disableParallelRead = false;
	}

	public boolean isParallelReadDisabled() {
		return this.disableParallelRead;
	}

	private static class PreAssignedInputSplitAssigner implements InputSplitAssigner {
		private InputSplit[] inputSplits;
		private int[] taskInputSplitSize;
		private int[] taskInputSplitStartIndex;
		private int[] taskInputSplitNextIndex;

		public PreAssignedInputSplitAssigner(InputSplit[] inputSplits, int[] taskInputSplitSize, int[] taskInputSplitStartIndex) {
			this.inputSplits = inputSplits;
			this.taskInputSplitSize = taskInputSplitSize;
			this.taskInputSplitStartIndex = taskInputSplitStartIndex;
			this.taskInputSplitNextIndex = new int[taskInputSplitSize.length];

			for (int i = 0; i < this.taskInputSplitStartIndex.length; ++i) {
				this.taskInputSplitNextIndex[i] = this.taskInputSplitStartIndex[i];
			}
		}

		public InputSplit getNextInputSplit(String location, int taskIndex) {
			Preconditions.checkArgument(taskIndex >= 0 && taskIndex < this.taskInputSplitSize.length, "Fail to create");
			InputSplit result = null;
			if (this.taskInputSplitNextIndex[taskIndex] < this.taskInputSplitStartIndex[taskIndex] + this.taskInputSplitSize[taskIndex]) {
				result = this.inputSplits[this.taskInputSplitNextIndex[taskIndex]];
				++this.taskInputSplitNextIndex[taskIndex];
			}

			return result;
		}

		@Override
		public void inputSplitsAssigned(int taskId, List<InputSplit> inputSplits) {

		}
	}
}
