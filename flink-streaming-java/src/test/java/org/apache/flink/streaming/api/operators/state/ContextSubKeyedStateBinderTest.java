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

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.FoldingState;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

/**
 * Tests for {@link ContextSubKeyedStateBinder}.
 */
public class ContextSubKeyedStateBinderTest {

	@Test
	public void testStateCache() throws Exception {
		final IdleOperator<Integer, Integer> operator = new IdleOperator<>();
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

		assertNull(binder.getLastState());
		assertNull(binder.getLastStateName());

		ValueState<Integer> valueState1 =
			binder.getSubKeyedStateWithNamespace(valueStateDescriptor1, 0, nameSpaceSerializer);
		assertEquals(valueStateDescriptor1.getName(), binder.getLastStateName());
		assertSame(valueState1, binder.getLastState());

		ValueState<Integer> valueState2 =
			binder.getSubKeyedStateWithNamespace(valueStateDescriptor2, 0, nameSpaceSerializer);
		assertNotSame(valueState1, valueState2);
		assertEquals(valueStateDescriptor2.getName(), binder.getLastStateName());
		assertSame(valueState2, binder.getLastState());

		assertSame(valueState1,
			binder.getSubKeyedStateWithNamespace(valueStateDescriptor1, 1, nameSpaceSerializer));
		assertEquals(valueStateDescriptor1.getName(), binder.getLastStateName());
		assertSame(valueState1, binder.getLastState());

		ValueState<Integer> valueState3 =
			binder.getSubKeyedStateWithNamespace(valueStateDescriptor3, 0, nameSpaceSerializer);
		assertNotSame(valueState1, valueState3);
		assertNotSame(valueState2, valueState3);
		assertEquals(valueStateDescriptor3.getName(), binder.getLastStateName());
		assertSame(valueState3, binder.getLastState());

		testHarness.close();
	}

	@Test
	public void testCreateState() throws Exception {
		final IdleOperator<Integer, Integer> idleOperator = new IdleOperator<>();

		final KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> testHarness =
			new KeyedOneInputStreamOperatorTestHarness<>(idleOperator, new IdentityKeySelector<>(), BasicTypeInfo.INT_TYPE_INFO);

		testHarness.open();

		final ContextSubKeyedStateBinder binder = new ContextSubKeyedStateBinder(idleOperator);

		TypeSerializer<Integer> namespaceSerializer = IntSerializer.INSTANCE;

		ValueStateDescriptor<Integer> valueStateDescriptor1 =
			new ValueStateDescriptor<>("v1", IntSerializer.INSTANCE);

		ValueStateDescriptor<Integer> valueStateDescriptor2 =
			new ValueStateDescriptor<>("v2", IntSerializer.INSTANCE);

		ValueState<Integer> valueState1;

		ValueState<Integer> valueState2;

		ListStateDescriptor<Integer> listStateDescriptor1 =
			new ListStateDescriptor<>("l1", IntSerializer.INSTANCE);

		ListStateDescriptor<Integer> listStateDescriptor2 =
			new ListStateDescriptor<>("l2", IntSerializer.INSTANCE);

		ListState<Integer> listState1;

		ListState<Integer> listState2;

		MapStateDescriptor<Integer, Integer> mapStateDescriptor1 =
			new MapStateDescriptor<>("m1", IntSerializer.INSTANCE, IntSerializer.INSTANCE);

		MapStateDescriptor<Integer, Integer> mapStateDescriptor2 =
			new MapStateDescriptor<>("m2", IntSerializer.INSTANCE, IntSerializer.INSTANCE);

		MapState<Integer, Integer> mapState1;

		MapState<Integer, Integer> mapState2;

		FoldingStateDescriptor<Integer, Integer> foldingStateDescriptor1 =
			new FoldingStateDescriptor<>("f1", 0, new AddFoldFunction(), IntSerializer.INSTANCE);

		FoldingStateDescriptor<Integer, Integer> foldingStateDescriptor2 =
			new FoldingStateDescriptor<>("f2", 0, new AddFoldFunction(), IntSerializer.INSTANCE);

		FoldingState<Integer, Integer> foldingState1;

		FoldingState<Integer, Integer> foldingState2;

		ReducingStateDescriptor<Integer>reducingStateDescriptor1 =
			new ReducingStateDescriptor<>("r1", new AddReduceFunction(), IntSerializer.INSTANCE);

		ReducingStateDescriptor<Integer>reducingStateDescriptor2 =
			new ReducingStateDescriptor<>("r2", new AddReduceFunction(), IntSerializer.INSTANCE);

		ReducingState<Integer> reducingState1;

		ReducingState<Integer> reducingState2;

		AggregatingStateDescriptor<Integer, Integer, Integer> aggregatingStateDescriptor1 =
			new AggregatingStateDescriptor<>("a1", new AddAggFunction(), IntSerializer.INSTANCE);

		AggregatingStateDescriptor<Integer, Integer, Integer> aggregatingStateDescriptor2 =
			new AggregatingStateDescriptor<>("a2", new AddAggFunction(), IntSerializer.INSTANCE);

		AggregatingState<Integer, Integer> aggregatingState1;

		AggregatingState<Integer, Integer> aggregatingState2;

		// test getSubKeyedStateWithNamespace
		valueState1 = binder.getSubKeyedStateWithNamespace(valueStateDescriptor1, 0, namespaceSerializer);
		valueState2 = binder.getSubKeyedStateWithNamespace(valueStateDescriptor2, 0, namespaceSerializer);
		assertNotSame(valueState1, valueState2);
		assertSame(valueState1,
			binder.getSubKeyedStateWithNamespace(valueStateDescriptor1, 0, namespaceSerializer));
		assertSame(valueState2,
			binder.getSubKeyedStateWithNamespace(valueStateDescriptor2, 0, namespaceSerializer));

		listState1 = binder.getSubKeyedStateWithNamespace(listStateDescriptor1, 0, namespaceSerializer);
		listState2 = binder.getSubKeyedStateWithNamespace(listStateDescriptor2, 0, namespaceSerializer);
		assertNotSame(listState1, listState2);
		assertSame(listState1,
			binder.getSubKeyedStateWithNamespace(listStateDescriptor1, 0, namespaceSerializer));
		assertSame(listState2,
			binder.getSubKeyedStateWithNamespace(listStateDescriptor2, 0, namespaceSerializer));

		mapState1 = binder.getSubKeyedStateWithNamespace(mapStateDescriptor1, 0, namespaceSerializer);
		mapState2 = binder.getSubKeyedStateWithNamespace(mapStateDescriptor2, 0, namespaceSerializer);
		assertNotSame(mapState1, mapState2);
		assertSame(mapState1,
			binder.getSubKeyedStateWithNamespace(mapStateDescriptor1, 0, namespaceSerializer));
		assertSame(mapState2,
			binder.getSubKeyedStateWithNamespace(mapStateDescriptor2, 0, namespaceSerializer));

		foldingState1 = binder.getSubKeyedStateWithNamespace(foldingStateDescriptor1, 0, namespaceSerializer);
		foldingState2 = binder.getSubKeyedStateWithNamespace(foldingStateDescriptor2, 0, namespaceSerializer);
		assertNotSame(foldingState1, foldingState2);
		assertSame(foldingState1,
			binder.getSubKeyedStateWithNamespace(foldingStateDescriptor1, 0, namespaceSerializer));
		assertSame(foldingState2,
			binder.getSubKeyedStateWithNamespace(foldingStateDescriptor2, 0, namespaceSerializer));

		reducingState1 = binder.getSubKeyedStateWithNamespace(reducingStateDescriptor1, 0, namespaceSerializer);
		reducingState2 = binder.getSubKeyedStateWithNamespace(reducingStateDescriptor2, 0, namespaceSerializer);
		assertNotSame(reducingState1, reducingState2);
		assertSame(reducingState1,
			binder.getSubKeyedStateWithNamespace(reducingStateDescriptor1, 0, namespaceSerializer));
		assertSame(reducingState2,
			binder.getSubKeyedStateWithNamespace(reducingStateDescriptor2, 0, namespaceSerializer));

		aggregatingState1 = binder.getSubKeyedStateWithNamespace(aggregatingStateDescriptor1, 0, namespaceSerializer);
		aggregatingState2 = binder.getSubKeyedStateWithNamespace(aggregatingStateDescriptor2, 0, namespaceSerializer);
		assertNotSame(aggregatingState1, aggregatingState2);
		assertSame(aggregatingState1,
			binder.getSubKeyedStateWithNamespace(aggregatingStateDescriptor1, 0, namespaceSerializer));
		assertSame(aggregatingState2,
			binder.getSubKeyedStateWithNamespace(aggregatingStateDescriptor2, 0, namespaceSerializer));

		// test getContextSubKeyedAppendingState
		ListStateDescriptor<Integer> listStateDescriptor3 =
			new ListStateDescriptor<>("l3", IntSerializer.INSTANCE);

		ContextSubKeyedAppendingState<?, ?, ?> listState3;

		FoldingStateDescriptor<Integer, Integer> foldingStateDescriptor3 =
			new FoldingStateDescriptor<>("f3", 0, new AddFoldFunction(), IntSerializer.INSTANCE);

		ContextSubKeyedAppendingState<?, ?, ?> foldingState3;

		ReducingStateDescriptor<Integer>reducingStateDescriptor3 =
			new ReducingStateDescriptor<>("r3", new AddReduceFunction(), IntSerializer.INSTANCE);

		ContextSubKeyedAppendingState<?, ?, ?> reducingState3;

		AggregatingStateDescriptor<Integer, Integer, Integer> aggregatingStateDescriptor3 =
			new AggregatingStateDescriptor<>("a3", new AddAggFunction(), IntSerializer.INSTANCE);

		ContextSubKeyedAppendingState<?, ?, ?> aggregatingState3;

		listState3 = binder.getContextSubKeyedAppendingState(listStateDescriptor3, namespaceSerializer);
		assertNotSame(listState1, listState3);
		assertNotSame(listState2, listState3);
		assertSame(listState1,
			binder.getContextSubKeyedAppendingState(listStateDescriptor1, namespaceSerializer));
		assertSame(listState2,
			binder.getContextSubKeyedAppendingState(listStateDescriptor2, namespaceSerializer));
		assertSame(listState3,
			binder.getContextSubKeyedAppendingState(listStateDescriptor3, namespaceSerializer));
		assertSame(listState3,
			binder.getSubKeyedStateWithNamespace(listStateDescriptor3, 0, namespaceSerializer));

		foldingState3 = binder.getContextSubKeyedAppendingState(foldingStateDescriptor3, namespaceSerializer);
		assertNotSame(foldingState1, foldingState3);
		assertNotSame(foldingState2, foldingState3);
		assertSame(foldingState1,
			binder.getContextSubKeyedAppendingState(foldingStateDescriptor1, namespaceSerializer));
		assertSame(foldingState2,
			binder.getContextSubKeyedAppendingState(foldingStateDescriptor2, namespaceSerializer));
		assertSame(foldingState3,
			binder.getContextSubKeyedAppendingState(foldingStateDescriptor3, namespaceSerializer));
		assertSame(foldingState3,
			binder.getSubKeyedStateWithNamespace(foldingStateDescriptor3, 0, namespaceSerializer));

		reducingState3 = binder.getContextSubKeyedAppendingState(reducingStateDescriptor3, namespaceSerializer);
		assertNotSame(reducingState1, reducingState3);
		assertNotSame(reducingState2, reducingState3);
		assertSame(reducingState1,
			binder.getContextSubKeyedAppendingState(reducingStateDescriptor1, namespaceSerializer));
		assertSame(reducingState2,
			binder.getContextSubKeyedAppendingState(reducingStateDescriptor2, namespaceSerializer));
		assertSame(reducingState3,
			binder.getContextSubKeyedAppendingState(reducingStateDescriptor3, namespaceSerializer));
		assertSame(reducingState3,
			binder.getSubKeyedStateWithNamespace(reducingStateDescriptor3, 0, namespaceSerializer));

		aggregatingState3 = binder.getContextSubKeyedAppendingState(aggregatingStateDescriptor3, namespaceSerializer);
		assertNotSame(aggregatingState1, aggregatingState3);
		assertNotSame(aggregatingState2, aggregatingState3);
		assertSame(aggregatingState1,
			binder.getContextSubKeyedAppendingState(aggregatingStateDescriptor1, namespaceSerializer));
		assertSame(aggregatingState2,
			binder.getContextSubKeyedAppendingState(aggregatingStateDescriptor2, namespaceSerializer));
		assertSame(aggregatingState3,
			binder.getContextSubKeyedAppendingState(aggregatingStateDescriptor3, namespaceSerializer));
		assertSame(aggregatingState3,
			binder.getSubKeyedStateWithNamespace(aggregatingStateDescriptor3, 0, namespaceSerializer));
	}

	private static class IdleOperator<IN, OUT>
		extends AbstractStreamOperator<OUT>
		implements OneInputStreamOperator<IN, OUT> {

		@Override
		public void processElement(StreamRecord<IN> elements) throws Exception {

		}

		@Override
		public void endInput() throws Exception {

		}
	}

	private static final class IdentityKeySelector<T> implements KeySelector<T, T> {

		@Override
		public T getKey(T value) {
			return value;
		}
	}

	private static final class AddFoldFunction implements FoldFunction<Integer, Integer>	{

		@Override
		public Integer fold(Integer acc, Integer value) {
			return acc + value;
		}
	}

	private static final class AddReduceFunction implements ReduceFunction<Integer> {

		@Override
		public Integer reduce(Integer value1, Integer value2) {
			return value1 + value2;
		}
	}

	private static final class AddAggFunction implements AggregateFunction<Integer, Integer, Integer> {

		@Override
		public Integer createAccumulator() {
			return 0;
		}

		@Override
		public Integer add(Integer value, Integer accumulator) {
			return value + accumulator;
		}

		@Override
		public Integer getResult(Integer accumulator) {
			return accumulator;
		}

		@Override
		public Integer merge(Integer a, Integer b) {
			return a + b;
		}
	}

}
