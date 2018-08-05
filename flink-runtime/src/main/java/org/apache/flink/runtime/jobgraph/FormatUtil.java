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

package org.apache.flink.runtime.jobgraph;

import org.apache.flink.api.common.io.FinalizeOnMaster;
import org.apache.flink.api.common.io.InitializeOnMaster;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.operators.util.UserCodeObjectWrapper;
import org.apache.flink.api.common.operators.util.UserCodeWrapper;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DelegatingConfiguration;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.runtime.util.EmptyIterator;
import org.apache.flink.runtime.util.SingleElementIterator;
import org.apache.flink.types.DefaultPair;
import org.apache.flink.types.Pair;

import javax.annotation.Nullable;

import java.util.Iterator;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Utilities for deserialize input/output formats and initializing/finalizing them on master.
 */
public class FormatUtil {

	public static <STUB_KEY> void initializeInputFormatsOnMaster(
		JobVertex jobVertex,
		AbstractFormatStub<STUB_KEY, ?, ?> stub,
		final Map<STUB_KEY, String> formatDescriptions) throws RuntimeException {

		// set user classloader before calling user code
		final ClassLoader original = Thread.currentThread().getContextClassLoader();
		Thread.currentThread().setContextClassLoader(stub.getClassLoader());

		try {
			Iterator<? extends Pair<STUB_KEY, ?>> it = stub.getFormat(FormatType.INPUT);
			it.forEachRemaining(
				(pair) -> {
					STUB_KEY key = pair.getKey();
					InputFormat inputFormat = (InputFormat) pair.getValue();
					try {
						inputFormat.configure(stub.getParameters(key));
					} catch (Throwable t) {
						throw new RuntimeException("Configuring the OutputFormat ("
							+ "description: " + formatDescriptions.get(key)
							+ ", stubKey: " + key + ") failed: " + t.getMessage(), t);
					}

					// TODO: support multi InputFormat on multi-head chaining mode
					jobVertex.setInputSplitSource(inputFormat);
				}
			);
		} finally {
			// restore previous classloader
			Thread.currentThread().setContextClassLoader(original);
		}
	}

	public static <STUB_KEY> void initializeOutputFormatsOnMaster(
		JobVertex jobVertex,
		AbstractFormatStub<STUB_KEY, ?, ?> stub,
		final Map<STUB_KEY, String> formatDescriptions)	throws RuntimeException {

		// set user classloader before calling user code
		final ClassLoader original = Thread.currentThread().getContextClassLoader();
		Thread.currentThread().setContextClassLoader(stub.getClassLoader());

		try {
			Iterator<? extends Pair<STUB_KEY, ?>> it = stub.getFormat(FormatType.OUTPUT);
			it.forEachRemaining(
				(pair) -> {
					STUB_KEY key = pair.getKey();
					OutputFormat outputFormat = (OutputFormat) pair.getValue();
					if (outputFormat instanceof InitializeOnMaster) {
						try {
							outputFormat.configure(stub.getParameters(key));

							((InitializeOnMaster) outputFormat).initializeGlobal(jobVertex.getParallelism());
						} catch (Throwable t) {
							throw new RuntimeException("Configuring the OutputFormat ("
								+ "description: " + formatDescriptions.get(key)
								+ ", stubKey: " + key + ") failed: " + t.getMessage(), t);
						}
					}
				}
			);
		} finally {
			// restore previous classloader
			Thread.currentThread().setContextClassLoader(original);
		}
	}

	public static <STUB_KEY> void finalizeOutputFormatsOnMaster(
		JobVertex jobVertex,
		AbstractFormatStub<STUB_KEY, ?, ?> stub,
		final Map<STUB_KEY, String> formatDescriptions)	throws RuntimeException {

		// set user classloader before calling user code
		final ClassLoader original = Thread.currentThread().getContextClassLoader();
		Thread.currentThread().setContextClassLoader(stub.getClassLoader());

		try {
			Iterator<? extends Pair<STUB_KEY, ?>> it = stub.getFormat(FormatType.OUTPUT);
			it.forEachRemaining(
				(pair) -> {
					STUB_KEY key = pair.getKey();
					OutputFormat outputFormat = (OutputFormat) pair.getValue();
					if (outputFormat instanceof FinalizeOnMaster) {
						try {
							outputFormat.configure(stub.getParameters(key));

							((FinalizeOnMaster) outputFormat).finalizeGlobal(jobVertex.getParallelism());
						} catch (Throwable t) {
							throw new RuntimeException("Configuring the OutputFormat ("
								+ "description: " + formatDescriptions.get(key)
								+ ", stubKey: " + key + ") failed: " + t.getMessage(), t);
						}
					}
				}
			);
		} finally {
			// restore previous classloader
			Thread.currentThread().setContextClassLoader(original);
		}
	}

	/**
	 * Types for {@link InputFormat} and {@link OutputFormat}.
	 */
	public enum FormatType {
		INPUT,
		OUTPUT
	}

	/**
	 * Wrapper of {@link InputFormat} stub.
	 */
	public static class InputFormatStub extends AbstractFormatStub<Integer, InputFormat, InputFormat> {
		public static final Integer STUB_KEY = 0;

		public InputFormatStub(TaskConfig config, ClassLoader classLoader) throws Exception {
			super(config, classLoader);
		}

		@Override
		public Iterator<Pair<Integer, InputFormat>> getFormat(FormatType type) {
			if (type != FormatType.INPUT) {
				return EmptyIterator.get();
			}

			SingleElementIterator<Pair<Integer, InputFormat>> iterator = new SingleElementIterator<>();
			iterator.set(new DefaultPair<>(STUB_KEY, wrapper.getUserCodeObject(InputFormat.class, classLoader)));
			return iterator;
		}

		@Override
		public Configuration getParameters(Integer key) {
			if (!STUB_KEY.equals(key)) {
				return new Configuration();
			}

			return parameters;
		}

		public static void setStubFormat(TaskConfig config, InputFormat inputFormat) {
			config.setStubWrapper(new UserCodeObjectWrapper<>(inputFormat));
		}

		public static void setStubParameters(TaskConfig config, Configuration parameters) {
			config.setStubParameters(parameters);
		}
	}

	/**
	 * Wrapper of {@link OutputFormat} stub.
	 */
	public static class OutputFormatStub extends AbstractFormatStub<Integer, OutputFormat, OutputFormat> {
		public static final Integer STUB_KEY = 0;

		public OutputFormatStub(TaskConfig config, ClassLoader classLoader) throws Exception {
			super(config, classLoader);
		}

		@Override
		public Iterator<Pair<Integer, OutputFormat>> getFormat(FormatType type) {
			if (type != FormatType.OUTPUT) {
				return EmptyIterator.get();
			}

			SingleElementIterator<Pair<Integer, OutputFormat>> iterator = new SingleElementIterator<>();
			iterator.set(new DefaultPair<>(STUB_KEY, wrapper.getUserCodeObject(OutputFormat.class, classLoader)));
			return iterator;
		}

		@Override
		public Configuration getParameters(Integer key) {
			if (!STUB_KEY.equals(key)) {
				return new Configuration();
			}

			return parameters;
		}

		public static void setStubFormat(TaskConfig config, OutputFormat outputFormat) {
			config.setStubWrapper(new UserCodeObjectWrapper<>(outputFormat));
		}

		public static void setStubParameters(TaskConfig config, Configuration parameters) {
			config.setStubParameters(parameters);
		}
	}

	/**
	 * Wrapper of multi {@link InputFormat} and {@link OutputFormat} stubs.
	 */
	public static class MultiFormatStub<F> extends AbstractFormatStub<OperatorID, F, Tuple2<Map<OperatorID, InputFormat>, Map<OperatorID, OutputFormat>>> {

		public MultiFormatStub(TaskConfig config, ClassLoader classLoader) throws Exception {
			super(config, classLoader);
		}

		@Override
		public Iterator<Pair<OperatorID, F>> getFormat(FormatType type) {
			Tuple2<Map<OperatorID, InputFormat>, Map<OperatorID, OutputFormat>> tuple = wrapper.getUserCodeObject(Tuple2.class, classLoader);
			if (type == FormatType.INPUT && tuple.f0 != null) {
				final Iterator<Map.Entry<OperatorID, InputFormat>> iterator = tuple.f0.entrySet().iterator();
				return new Iterator<Pair<OperatorID, F>>() {
					@Override
					public boolean hasNext() {
						return iterator.hasNext();
					}

					@SuppressWarnings("unchecked")
					@Override
					public Pair<OperatorID, F> next() {
						Map.Entry<OperatorID, InputFormat> entry = iterator.next();
						return new DefaultPair(entry.getKey(), entry.getValue());
					}
				};
			} else if (type == FormatType.OUTPUT && tuple.f1 != null) {
				final Iterator<Map.Entry<OperatorID, OutputFormat>> iterator = tuple.f1.entrySet().iterator();
				return new Iterator<Pair<OperatorID, F>>() {
					@Override
					public boolean hasNext() {
						return iterator.hasNext();
					}

					@SuppressWarnings("unchecked")
					@Override
					public Pair<OperatorID, F> next() {
						Map.Entry<OperatorID, OutputFormat> entry = iterator.next();
						return new DefaultPair(entry.getKey(), entry.getValue());
					}
				};
			}

			return EmptyIterator.get();
		}

		@Override
		public Configuration getParameters(OperatorID operatorId) {
			return new DelegatingConfiguration(config.getStubParameters(), operatorId + ".");
		}

		public static void setStubFormats(
			TaskConfig config,
			@Nullable Map<OperatorID, InputFormat> idInputFormatMap,
			@Nullable Map<OperatorID, OutputFormat> idOutputFormatMap) {

			config.setStubWrapper(new UserCodeObjectWrapper<>(new Tuple2<>(idInputFormatMap, idOutputFormatMap)));
		}

		public static void setStubParameters(TaskConfig config, OperatorID operatorId, Configuration parameters) {
			for (String key : parameters.keySet()) {
				config.setStubParameter(operatorId + "." + key, parameters.getString(key, null));
			}
		}
	}

	/**
	 * Abstract wrapper for {@link InputFormat} and {@link OutputFormat} stubs.
	 */
	public abstract static class AbstractFormatStub<STUB_KEY, F, W> {

		protected final TaskConfig config;
		protected final ClassLoader classLoader;

		protected final UserCodeWrapper<W> wrapper;
		protected final Configuration parameters;

		public AbstractFormatStub(TaskConfig config, ClassLoader classLoader) throws Exception {
			checkState(config != null && classLoader != null);
			this.config = config;
			this.classLoader = classLoader;

			try {
				this.wrapper = config.getStubWrapper(classLoader);
				if (wrapper == null) {
					throw new Exception("No " + getClass().getSimpleName() + " present in task configuration.");
				}
			}
			catch (Throwable t) {
				throw new Exception("Deserializing " + getClass().getSimpleName() + " failed: " + t.getMessage(), t);
			}

			parameters = config.getStubParameters();
		}

		public ClassLoader getClassLoader() {
			return classLoader;
		}

		public abstract Iterator<Pair<STUB_KEY, F>> getFormat(FormatType type);

		public abstract Configuration getParameters(STUB_KEY key);

	}

	/**
	 * Private constructor to prevent instantiation.
	 */
	private FormatUtil() {
		throw new AssertionError();
	}
}
