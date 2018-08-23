/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProviderException;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A {@link SourceFunction} that reads data using an {@link InputFormat}.
 *
 * @param <OUT> the type parameter
 */
@Internal
public class InputFormatSourceFunctionV2<OUT> extends RichParallelSourceFunctionV2<OUT> {
	private static final long serialVersionUID = 1L;

	private TypeInformation<OUT> typeInfo;
	private transient TypeSerializer<OUT> serializer;

	private InputFormat<OUT, InputSplit> format;

	private transient StreamingRuntimeContext context;

	private transient InputSplitProvider provider;
	private transient Iterator<InputSplit> splitIterator;

	private transient Counter completedSplitsCounter;

	private transient OUT reusableElement;

	private transient SourceRecord<OUT> sourceRecord = new SourceRecord<>();

	private transient boolean isObjectReuse;

	private transient boolean hasMoreData;

	/**
	 * Instantiates a new Input format source function V2.
	 *
	 * @param format   the format
	 * @param typeInfo the type info
	 */
	@SuppressWarnings("unchecked")
	public InputFormatSourceFunctionV2(InputFormat<OUT, ?> format, TypeInformation<OUT> typeInfo) {
		this.format = (InputFormat<OUT, InputSplit>) format;
		this.typeInfo = typeInfo;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void open(Configuration parameters) throws Exception {
		context = (StreamingRuntimeContext) getRuntimeContext();

		if (format instanceof RichInputFormat) {
			((RichInputFormat) format).setRuntimeContext(context);
		}
		format.configure(parameters);

		provider = context.getInputSplitProvider();
		serializer = typeInfo.createSerializer(getRuntimeContext().getExecutionConfig());
		splitIterator = getInputSplits();
		completedSplitsCounter = getRuntimeContext().getMetricGroup().counter("numSplitsProcessed");

		if (format instanceof RichInputFormat) {
			((RichInputFormat) format).openInputFormat();
		}
		hasMoreData = splitIterator.hasNext();
		if (hasMoreData) {
			format.open(splitIterator.next());
		}
		reusableElement = serializer.createInstance();
		isObjectReuse = getRuntimeContext().getExecutionConfig().isObjectReuseEnabled();
	}

	@Override
	public boolean isIdle() {
		return false;
	}

	@Override
	public boolean isFinished() {
		return !hasMoreData;
	}

	@Override
	public SourceRecord<OUT> next() throws Exception {
		Preconditions.checkNotNull(format, "InputFormat should not be null");

		if (!isObjectReuse) {
			reusableElement = serializer.createInstance();
		}

		while (hasMoreData) {
			if (!format.reachedEnd()) {
				reusableElement = format.nextRecord(reusableElement);
				if (reusableElement != null) {
					return sourceRecord.replace(reusableElement);
				} else {
					completedSplitsCounter.inc();
					requestNextSplit();
				}
			} else {
				requestNextSplit();
			}
		}
		return null;
	}

	private void requestNextSplit() throws IOException {
		format.close();
		if (splitIterator.hasNext()) {
			format.open(splitIterator.next());
		} else {
			hasMoreData = false;
		}
	}

	@Override
	public void close() throws Exception {
		if (format != null) {
			format.close();
			if (format instanceof RichInputFormat) {
				((RichInputFormat) format).closeInputFormat();
			}
			format = null;
		}
	}

	/**
	 * Returns the {@code InputFormat}. This is only needed because we need to set the input
	 * split assigner on the {@code StreamGraph}.
	 *
	 * @return the format
	 */
	public InputFormat<OUT, InputSplit> getFormat() {
		return format;
	}

	private Iterator<InputSplit> getInputSplits() {

		return new Iterator<InputSplit>() {

			private InputSplit nextSplit;

			private boolean exhausted;

			@Override
			public boolean hasNext() {
				if (exhausted) {
					return false;
				}

				if (nextSplit != null) {
					return true;
				}

				final InputSplit split;
				try {
					split = provider.getNextInputSplit(context.getOperatorID(), context.getUserCodeClassLoader());
				} catch (InputSplitProviderException e) {
					throw new RuntimeException("Could not retrieve next input split.", e);
				}

				if (split != null) {
					this.nextSplit = split;
					return true;
				} else {
					exhausted = true;
					return false;
				}
			}

			@Override
			public InputSplit next() {
				if (this.nextSplit == null && !hasNext()) {
					throw new NoSuchElementException();
				}

				final InputSplit tmp = this.nextSplit;
				this.nextSplit = null;
				return tmp;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}
}
