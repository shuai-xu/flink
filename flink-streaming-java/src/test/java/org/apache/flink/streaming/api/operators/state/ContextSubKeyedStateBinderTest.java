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

package org.apache.flink.streaming.api.operators.state;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.subkeyed.SubKeyedState;
import org.apache.flink.runtime.state.subkeyed.SubKeyedStateDescriptor;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

/**
 * Tests for {@link ContextSubKeyedStateBinder}.
 */
public class ContextSubKeyedStateBinderTest {

	@Test
	public void testStateCache() throws Exception {
		final SubKeyedStateCacheOperator<Integer, Integer> operator = new SubKeyedStateCacheOperator<>();
		KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> testHarness =
			new KeyedOneInputStreamOperatorTestHarness<>(operator, new IdentityKeySelector<>(), BasicTypeInfo.INT_TYPE_INFO);

		testHarness.open();

		final ContextSubKeyedStateBinder binder = new ContextSubKeyedStateBinder(operator);

		TypeSerializer<Integer> nameSpaceSerializer = IntSerializer.INSTANCE;

		ValueStateDescriptor<Integer> valueStateDescriptor1 =
			new ValueStateDescriptor<>("v1", IntSerializer.INSTANCE);

		ValueStateDescriptor<Integer> valueStateDescriptor2 =
			new ValueStateDescriptor<>("v2", IntSerializer.INSTANCE);

		ValueStateDescriptor<Integer> valueStateDescriptor3 =
			new ValueStateDescriptor<>("v3", IntSerializer.INSTANCE);

		ValueState<Integer> valueState1 =
			binder.getSubKeyedStateWithNamespace(valueStateDescriptor1, 0, nameSpaceSerializer);
		assertEquals(1, operator.getCount());

		assertSame(valueState1,
			binder.getSubKeyedStateWithNamespace(valueStateDescriptor1, 0, nameSpaceSerializer));
		assertEquals(1, operator.getCount());

		ValueState<Integer> valueState2 =
			binder.getSubKeyedStateWithNamespace(valueStateDescriptor2, 0, nameSpaceSerializer);
		assertNotSame(valueState1, valueState2);
		assertEquals(2, operator.getCount());

		valueState1 =
			binder.getSubKeyedStateWithNamespace(valueStateDescriptor1, 0, nameSpaceSerializer);
		assertEquals(3, operator.getCount());

		ValueState<Integer> valueState3 =
			binder.getSubKeyedStateWithNamespace(valueStateDescriptor3, 0, nameSpaceSerializer);
		assertNotSame(valueState1, valueState3);
		assertNotSame(valueState2, valueState3);
		assertEquals(4, operator.getCount());
	}

	private static class SubKeyedStateCacheOperator<IN, OUT>
		extends AbstractStreamOperator<OUT>
		implements OneInputStreamOperator<IN, OUT> {

		private int count = 0;

		@Override
		public void processElement(StreamRecord<IN> element) throws Exception {
		}

		@Override
		public <K, N, V, S extends SubKeyedState<K, N, V>> S getSubKeyedState(
			final SubKeyedStateDescriptor<K, N, V, S> descriptor
		) {
			count++;
			return super.getSubKeyedState(descriptor);
		}

		public int getCount() {
			return count;
		}
	}

	private static final class IdentityKeySelector<T> implements KeySelector<T, T> {

		@Override
		public T getKey(T value) {
			return value;
		}
	}

}
