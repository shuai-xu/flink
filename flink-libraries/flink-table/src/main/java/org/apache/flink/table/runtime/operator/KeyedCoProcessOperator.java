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

package org.apache.flink.table.runtime.operator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.operators.TwoInputSelection;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.codegen.GeneratedCoProcessFunction;
import org.apache.flink.table.runtime.functions.CoProcessFunction;
import org.apache.flink.table.runtime.functions.ExecutionContextImpl;

/**
 * A {@link org.apache.flink.streaming.api.operators.StreamOperator} for executing keyed
 * {@link CoProcessFunction CoProcessFunctions}.
 */
@Internal
public class KeyedCoProcessOperator<K, IN1, IN2, OUT>
		extends AbstractUdfStreamOperator<OUT, CoProcessFunction<IN1, IN2, OUT>>
		implements TwoInputStreamOperator<IN1, IN2, OUT> {

	private static final long serialVersionUID = 1L;

	private GeneratedCoProcessFunction funcCode;

	/** Flag to prevent duplicate function.close() calls in close() and dispose(). */
	private transient boolean functionsClosed = false;

	public KeyedCoProcessOperator(GeneratedCoProcessFunction funcCode) {
		super(null);
		this.funcCode = funcCode;
	}

	public KeyedCoProcessOperator(CoProcessFunction<IN1, IN2, OUT> function) {
		super(function);
	}

	@Override
	public void open() throws Exception {
		super.open();

		if (funcCode != null) {
			userFunction = (CoProcessFunction<IN1, IN2, OUT>) funcCode.newInstance(
				getContainingTask().getUserCodeClassLoader());
			funcCode = null;
		}
		userFunction.open(new ExecutionContextImpl(this, getRuntimeContext()));
	}

	@Override
	public TwoInputSelection processRecord1(StreamRecord<IN1> element) throws Exception {
		userFunction.processElement1(element.getValue(), context, collector);
		return TwoInputSelection.ANY;
	}

	@Override
	public void endInput1() throws Exception {
		userFunction.endInput1(collector);
	}

	@Override
	public TwoInputSelection processRecord2(StreamRecord<IN2> element) throws Exception {
		userFunction.processElement2(element.getValue(), context, collector);
		return TwoInputSelection.ANY;
	}

	@Override
	public void endInput2() throws Exception {
		userFunction.endInput2(collector);
	}
}
