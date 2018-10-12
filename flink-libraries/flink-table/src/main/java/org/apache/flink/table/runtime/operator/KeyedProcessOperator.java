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

import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.codegen.GeneratedProcessFunction;
import org.apache.flink.table.runtime.functions.ExecutionContextImpl;
import org.apache.flink.table.runtime.functions.ProcessFunction;

/**
 * A {@link org.apache.flink.streaming.api.operators.StreamOperator} for executing keyed
 * {@link ProcessFunction ProcessFunctions}.
 */
public class KeyedProcessOperator<K, IN, OUT>
	extends AbstractUdfStreamOperator<OUT, ProcessFunction<IN, OUT>>
	implements OneInputStreamOperator<IN, OUT>{

	private static final long serialVersionUID = 1L;

	protected GeneratedProcessFunction funcCode;

	public KeyedProcessOperator(GeneratedProcessFunction funcCode) {
		super(null);
		this.funcCode = funcCode;
	}

	public KeyedProcessOperator(ProcessFunction<IN, OUT> function) {
		super(function);
	}

	@Override
	public void open() throws Exception {
		super.open();

		if (funcCode != null) {
			userFunction = (ProcessFunction<IN, OUT>) funcCode.newInstance(
				getContainingTask().getUserCodeClassLoader());
			funcCode = null;
		}
		userFunction.open(new ExecutionContextImpl(this, getRuntimeContext()));
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		userFunction.processElement(element.getValue(), context, collector);
	}

	@Override
	public void endInput() throws Exception {
		userFunction.endInput(collector);
	}
}
