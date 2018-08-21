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

package org.apache.flink.table.runtime.operator.bundle;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.runtime.state2.keyed.KeyedValueState;
import org.apache.flink.runtime.state2.keyed.KeyedValueStateDescriptor;
import org.apache.flink.streaming.api.bundle.BundleTriggerCallback;
import org.apache.flink.streaming.api.bundle.CoBundleTrigger;
import org.apache.flink.streaming.api.graph.OperatorContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.InputElementSelection;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.table.runtime.operator.StreamRecordCollector;
import org.apache.flink.util.Collector;
import org.apache.flink.util.LockAndCondition;
import org.apache.flink.util.LockGetReleaseWrapper;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Used for MiniBatch Join.
 */
public abstract class KeyedCoBundleOperator<K, L, R, OUT>
	extends AbstractStreamOperator<OUT>
	implements TwoInputStreamOperator<L, R, OUT>, BundleTriggerCallback {

	private static final String LEFT_STATE_NAME = "_keyed_co_bundle_operator_left_state_";

	private static final String RIGHT_STATE_NAME = "_keyed_co_bundle_operator_right_state_";

	private final CoBundleTrigger<L, R> coBundleTrigger;

	private transient LockAndCondition checkpointingLock;

	private transient StreamRecordCollector<OUT> collector;

	private transient Map<K, List<L>> leftBuffer;
	private transient Map<K, List<R>> rightBuffer;

	private transient KeyedValueState<K, List<L>> leftBufferState;
	private transient KeyedValueState<K, List<R>> rightBufferState;

	// We keep track of watermarks from both inputs, the combined input is the minimum
	// Once the minimum advances we emit a new watermark for downstream operators
	private long input1Watermark = Long.MIN_VALUE;
	private long input2Watermark = Long.MIN_VALUE;
	private long currentWatermark = Long.MIN_VALUE;

	public KeyedCoBundleOperator(CoBundleTrigger<L, R> coBundleTrigger) {
		Preconditions.checkNotNull(coBundleTrigger, "coBundleTrigger is null");
		this.coBundleTrigger = coBundleTrigger;
	}

	@SuppressWarnings("unchecked")
	@Override
	public InputElementSelection processElement1(StreamRecord<L> element) throws Exception {
		K key = (K) getCurrentKey();
		L row = element.getValue();
		List<L> records = leftBuffer.computeIfAbsent(key, k -> new ArrayList<>());
		records.add(row);
		coBundleTrigger.onLeftElement(row);
		return InputElementSelection.ANY;
	}

	@SuppressWarnings("unchecked")
	@Override
	public InputElementSelection processElement2(StreamRecord<R> element) throws Exception {
		K key = (K) getCurrentKey();
		R row = element.getValue();
		List<R> records = rightBuffer.computeIfAbsent(key, k -> new ArrayList<>());
		records.add(row);
		coBundleTrigger.onRightElement(row);
		return InputElementSelection.ANY;
	}

	@Override
	public void processWatermark1(Watermark mark) throws Exception {
		input1Watermark = mark.getTimestamp();
		long newMin = Math.min(input1Watermark, input2Watermark);
		if (newMin > currentWatermark) {
			currentWatermark = newMin;
			finishBundle();
			output.emitWatermark(new Watermark(newMin));
		}
	}

	@Override
	public void processWatermark2(Watermark mark) throws Exception {
		input2Watermark = mark.getTimestamp();
		long newMin = Math.min(input1Watermark, input2Watermark);
		if (newMin > currentWatermark) {
			currentWatermark = newMin;
			finishBundle();
			output.emitWatermark(new Watermark(newMin));
		}
	}

	@Override
	public InputElementSelection endInput1() throws Exception {
		finishBundle();
		return InputElementSelection.ANY;
	}

	@Override
	public InputElementSelection endInput2() throws Exception {
		finishBundle();
		return InputElementSelection.ANY;
	}

	@Override
	public void finishBundle() throws Exception {
		try (LockGetReleaseWrapper wrapper = new LockGetReleaseWrapper(checkpointingLock.getLock())) {
			if (!leftBuffer.isEmpty() || !rightBuffer.isEmpty()) {
				this.processBundles(leftBuffer, rightBuffer, collector);
				leftBuffer.clear();
				rightBuffer.clear();
			}
			coBundleTrigger.reset();
		}
	}

	protected abstract void processBundles(
		Map<K, List<L>> left,
		Map<K, List<R>> right,
		Collector<OUT> out) throws Exception;

	@Override
	public void setup(StreamTask<?, ?> containingTask, OperatorContext context, Output<StreamRecord<OUT>> output) {
		super.setup(containingTask, context, output);
		this.checkpointingLock = getContainingTask().getCheckpointLock();
	}

	@Override
	public void open() throws Exception {
		super.open();
		this.checkpointingLock = getContainingTask().getCheckpointLock();
		this.collector = new StreamRecordCollector<>(output);

		// create & restore state
		//noinspection unchecked
		KeyedValueStateDescriptor<K, List<L>> leftBufferStateDesc = new KeyedValueStateDescriptor<>(
			LEFT_STATE_NAME,
			(TypeSerializer<K>) getKeySerializer(),
			new ListSerializer<>(config.getTypeSerializerIn1()));
		this.leftBufferState = getKeyedState(leftBufferStateDesc);
		this.leftBuffer = new HashMap<>();
		this.leftBuffer.putAll(leftBufferState.getAll());

		//noinspection unchecked
		KeyedValueStateDescriptor<K, List<R>> rightBufferStateDesc = new KeyedValueStateDescriptor<>(
			RIGHT_STATE_NAME,
			(TypeSerializer<K>) getKeySerializer(),
			new ListSerializer<>(config.getTypeSerializerIn2()));
		this.rightBufferState = getKeyedState(rightBufferStateDesc);
		this.rightBuffer = new HashMap<>();
		this.rightBuffer.putAll(rightBufferState.getAll());

		coBundleTrigger.registerBundleTriggerCallback(
			this,
			() -> KeyedCoBundleOperator.super.getProcessingTimeService());
		coBundleTrigger.reset();

		LOG.info("KeyedCoBundleOperator's trigger info: " + coBundleTrigger.explain());
	}

	@Override
	public void close() throws Exception {
		try {
			finishBundle();

		} finally {
			Exception exception = null;

			try {
				super.close();
			} catch (InterruptedException interrupted) {
				exception = interrupted;

				Thread.currentThread().interrupt();
			} catch (Exception e) {
				exception = e;
			}

			if (exception != null) {
				LOG.warn("Errors occurred while closing the KeyedCoBundleOperator.", exception);
			}
		}
	}

	@Override
	public void snapshotState(StateSnapshotContext context) throws Exception {
		super.snapshotState(context);

		// clear state first
		leftBufferState.removeAll();
		rightBufferState.removeAll();

		// update state
		leftBufferState.putAll(leftBuffer);
		rightBufferState.putAll(rightBuffer);
	}

	@Override
	public boolean requireState() {
		return true;
	}
}
