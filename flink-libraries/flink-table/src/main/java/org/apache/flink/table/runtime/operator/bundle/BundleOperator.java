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

import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.state2.ListState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.runtime.state2.partitioned.PartitionedListStateDescriptor;
import org.apache.flink.streaming.api.bundle.BundleTrigger;
import org.apache.flink.streaming.api.bundle.BundleTriggerCallback;
import org.apache.flink.streaming.api.graph.OperatorContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.table.runtime.functions.ExecutionContextImpl;
import org.apache.flink.table.runtime.functions.bundle.BundleFunction;
import org.apache.flink.table.runtime.operator.StreamRecordCollector;
import org.apache.flink.util.Collector;
import org.apache.flink.util.LockAndCondition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The {@link BundleOperator} allows to process incoming stream records
 * as a bundle on <strong>non-keyed-stream</strong>.
 *
 * <p>In case of chaining of this operator, it has to be made sure that the operators in the chain are
 * opened tail to head. The reason for this is that an opened
 * {@link org.apache.flink.table.runtime.operator.bundle.BundleOperator} starts
 * already emitting recovered {@link StreamElement} to downstream operators.
 *
 * @param <K> The type of the key in the bundle buffer
 * @param <V> The type of the value in the bundle buffer
 * @param <IN>  Input type for the operator.
 * @param <OUT> Output type for the operator.
 */
public class BundleOperator<K, V, IN, OUT>
	extends AbstractStreamOperator<OUT>
	implements OneInputStreamOperator<IN, OUT>, BundleTriggerCallback {
	private static final long serialVersionUID = 5081841938324118594L;

	/** The trigger that determines how many elements should be put into a bundle. */
	private final BundleTrigger<IN> bundleTrigger;

	private final BundleFunction<K, V, IN, OUT> function;

	private final TypeInformation<V> valueType;

	private final TypeInformation<K> keyType;

	private final KeySelector<IN, K> keySelector;

	/** The buffer in heap to store elements. */
	private transient Map<K, V> buffer;

	/** The state to store buffer to make it exactly once. */
	private transient ListState<Tuple2<K, V>> bufferState;

	private transient LockAndCondition checkpointingLock;

	/** Output for stream records. */
	private transient Collector<OUT> collector;

	private transient int numOfElements = 0;

	public BundleOperator(
		BundleFunction<K, V, IN, OUT> function,
		BundleTrigger<IN> bundleTrigger,
		TypeInformation<K> keyType,
		TypeInformation<V> valueType,
		KeySelector<IN, K> keySelector) {

		chainingStrategy = ChainingStrategy.ALWAYS;

		this.function = checkNotNull(function, "function is null");
		this.bundleTrigger = checkNotNull(bundleTrigger, "bundleTrigger is null");
		this.keyType = checkNotNull(keyType, "key type is null");
		this.valueType = checkNotNull(valueType, "value type is null");
		this.keySelector = checkNotNull(keySelector, "key selector is null");
	}

	@Override
	public void setup(StreamTask<?, ?> containingTask, OperatorContext context, Output<StreamRecord<OUT>> output) {
		super.setup(containingTask, context, output);

		this.checkpointingLock = getContainingTask().getCheckpointLock();
	}

	@Override
	public void open() throws Exception {
		super.open();

		function.open(new ExecutionContextImpl(
			this,
			getRuntimeContext()));

		this.collector = new StreamRecordCollector<>(output);
		this.buffer = new HashMap<>();

		// create & restore state
		TypeInformation<Tuple2<K, V>> tupleType = new TupleTypeInfo<>(keyType, valueType);
		PartitionedListStateDescriptor<Tuple2<K, V>> stateDesc = new PartitionedListStateDescriptor<>(
			"localBufferState",
			tupleType.createSerializer(getExecutionConfig()));
		this.bufferState = getPartitionedState(stateDesc);
		// recover buffer from partition state
		recoverBundleBuffer();

		bundleTrigger.registerBundleTriggerCallback(this,
			() -> BundleOperator.super.getProcessingTimeService());
		// reset trigger
		bundleTrigger.reset();

		// counter metric to get the size of bundle
		getRuntimeContext().getMetricGroup().gauge("bundleSize", (Gauge<Integer>) () -> numOfElements);
		getRuntimeContext().getMetricGroup().gauge("bundleRatio", (Gauge<Double>) () -> {
			int numOfKeys = buffer.size();
			if (numOfKeys == 0) {
				return 0.0;
			} else {
				return 1.0 * numOfElements / numOfKeys;
			}
		});
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		K key = keySelector.getKey(element.getValue());
		V value = buffer.get(key);  // maybe null
		// accumulate to value
		V newValue = function.addInput(value, element.getValue());
		// update to buffer
		buffer.put(key, newValue);
		numOfElements++;
		bundleTrigger.onElement(element.getValue());
	}

	/** build bundle and invoke BundleFunction. */
	@Override
	public void finishBundle() throws Exception {
		assert checkpointingLock.getLock().isHeldByCurrentThread();
		if (!buffer.isEmpty()) {
			numOfElements = 0;
			function.finishBundle(buffer, collector);
			buffer.clear();
		}
		// reset trigger
		bundleTrigger.reset();
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		// bundle operator only used in unbounded group by which not need to handle watermark
		finishBundle();
		super.processWatermark(mark);
	}

	@Override
	public void endInput() throws Exception {
		finishBundle();
	}

	@Override
	public void snapshotState(StateSnapshotContext context) throws Exception {
		super.snapshotState(context);
		// clear state first
		bufferState.clear();

		Iterator<Map.Entry<K, V>> iter = buffer.entrySet().iterator();
		List<Tuple2<K, V>> stateToPut = new ArrayList<>(buffer.size());
		while (iter.hasNext()) {
			Map.Entry<K, V> entry = iter.next();
			K key = entry.getKey();
			V value = entry.getValue();
			stateToPut.add(Tuple2.of(key, value));
		}

		// batch put
		bufferState.addAll(stateToPut);
	}

	private void recoverBundleBuffer() throws Exception {
		Iterator<Tuple2<K, V>> iter = bufferState.iterator();
		while (iter.hasNext()) {
			Tuple2<K, V> tuple = iter.next();
			K key = tuple.f0;
			V value = tuple.f1;
			V prevValue = buffer.get(key);
			V newValue = function.mergeValue(prevValue, value);
			buffer.put(key, newValue);
			// recovering number
			numOfElements++;
		}
	}

	@Override
	public void close() throws Exception {
		assert checkpointingLock.getLock().isHeldByCurrentThread();
		try {
			finishBundle();

		} finally {
			Exception exception = null;

			try {
				super.close();
				if (function != null) {
					FunctionUtils.closeFunction(function);
				}
			} catch (InterruptedException interrupted) {
				exception = interrupted;

				Thread.currentThread().interrupt();
			} catch (Exception e) {
				exception = e;
			}

			if (exception != null) {
				LOG.warn("Errors occurred while closing the BundleOperator.", exception);
			}
		}
	}

	@Override
	public boolean requireState() {
		return true;
	}
}
