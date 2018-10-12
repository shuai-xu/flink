/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.operator.join.batch;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.streaming.api.operators.TwoInputSelection;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.codegen.GeneratedCoTableValuedAggHandleFunction;
import org.apache.flink.table.codegen.GeneratedProjection;
import org.apache.flink.table.codegen.GeneratedSorter;
import org.apache.flink.table.codegen.Projection;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.runtime.aggregate.AppendGroupKeyCollector;
import org.apache.flink.table.runtime.functions.CoTableValuedAggHandleFunction;
import org.apache.flink.table.runtime.functions.ExecutionContext;
import org.apache.flink.table.runtime.functions.ExecutionContextImpl;
import org.apache.flink.table.runtime.operator.AbstractStreamOperatorWithMetrics;
import org.apache.flink.table.runtime.operator.StreamRecordCollector;
import org.apache.flink.table.runtime.sort.BinaryExternalSorter;
import org.apache.flink.table.runtime.sort.NormalizedKeyComputer;
import org.apache.flink.table.runtime.sort.RecordComparator;
import org.apache.flink.table.typeutils.AbstractRowSerializer;
import org.apache.flink.table.typeutils.BinaryRowSerializer;
import org.apache.flink.table.util.BinaryRowUtil;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

import org.codehaus.commons.compiler.CompileException;

import static org.apache.flink.table.codegen.CodeGenUtils.compile;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An implementation that realizes the co-table valued aggregate through a sort-merge strategy.
 */
public class SortMergeCoTableValuedAggOperator extends AbstractStreamOperatorWithMetrics<BaseRow>
		implements TwoInputStreamOperator<BaseRow, BaseRow, BaseRow> {

	private final long reservedSortMemory1;
	private final long preferredSortMemory1;
	private final long reservedSortMemory2;
	private final long preferredSortMemory2;
	private final long perRequestMemory;

	// generated code to cook
	private GeneratedProjection projectionCode1;
	private GeneratedProjection projectionCode2;
	private GeneratedSorter gSorter1;
	private GeneratedSorter gSorter2;
	private GeneratedSorter keyGSorter;
	private GeneratedCoTableValuedAggHandleFunction generateAggHandler;
	private CoTableValuedAggHandleFunction function;

	private transient CookedClasses classes;
	private transient MemoryManager memManager;
	private transient IOManager ioManager;
	private transient TypeSerializer<BaseRow> inputSerializer1;
	private transient TypeSerializer<BaseRow> inputSerializer2;
	private transient BinaryRowSerializer serializer1;
	private transient BinaryRowSerializer serializer2;
	private transient BinaryExternalSorter sorter1;
	private transient BinaryExternalSorter sorter2;
	private transient SortMergeCoTableValuedAggIterator joinIterator1;
	private transient Collector<BaseRow> collector;
	private transient boolean[] isFinished;
	private transient RecordComparator keyComparator;
	private transient BinaryRow lastKey;

	public SortMergeCoTableValuedAggOperator(
			long reservedSortMemory1, long preferredSortMemory1,
			long reservedSortMemory2, long preferredSortMemory2, long perRequestMemory,
			GeneratedProjection projectionCode1, GeneratedProjection projectionCode2,
			GeneratedSorter gSorter1, GeneratedSorter gSorter2, GeneratedSorter keyGSorter,
			GeneratedCoTableValuedAggHandleFunction generateAggHandler) {
		this.reservedSortMemory1 = reservedSortMemory1;
		this.preferredSortMemory1 = preferredSortMemory1;
		this.reservedSortMemory2 = reservedSortMemory2;
		this.preferredSortMemory2 = preferredSortMemory2;
		this.perRequestMemory = perRequestMemory;
		this.projectionCode1 = projectionCode1;
		this.projectionCode2 = projectionCode2;
		this.gSorter1 = checkNotNull(gSorter1);
		this.gSorter2 = checkNotNull(gSorter2);
		this.keyGSorter = checkNotNull(keyGSorter);
		this.generateAggHandler = generateAggHandler;
	}

	@Override
	public void open() throws Exception {
		super.open();

		// code gen classes.
		this.classes = cookGeneratedClasses(getContainingTask().getUserCodeClassLoader());
		this.function = (CoTableValuedAggHandleFunction) this.generateAggHandler
			.newInstance(getContainingTask().getUserCodeClassLoader());
		ExecutionContext executionContext = new ExecutionContextImpl(this, getRuntimeContext());
		this.function.open(executionContext);

		isFinished = new boolean[2];
		isFinished[0] = false;
		isFinished[1] = false;

		collector = new StreamRecordCollector<>(output);

		this.inputSerializer1 = getOperatorConfig().getTypeSerializerIn1(getUserCodeClassloader());
		this.serializer1 =
				new BinaryRowSerializer(((AbstractRowSerializer) inputSerializer1).getTypes());

		this.inputSerializer2 = getOperatorConfig().getTypeSerializerIn2(getUserCodeClassloader());
		this.serializer2 =
				new BinaryRowSerializer(((AbstractRowSerializer) inputSerializer2).getTypes());

		this.memManager = this.getContainingTask().getEnvironment().getMemoryManager();
		this.ioManager = this.getContainingTask().getEnvironment().getIOManager();

		initSorter();
		initKeyComparator();

		keyGSorter = null;
		projectionCode1 = null;
		projectionCode2 = null;
		gSorter1 = null;
		gSorter2 = null;
		this.generateAggHandler = null;
		lastKey = null;

		getMetricGroup().gauge("memoryUsedSizeInBytes",
			(Gauge<Long>) () -> sorter1.getUsedMemoryInBytes() + sorter2.getUsedMemoryInBytes());

		getMetricGroup().gauge("numSpillFiles",
			(Gauge<Long>) () -> sorter1.getNumSpillFiles() + sorter2.getNumSpillFiles());

		getMetricGroup().gauge("spillInBytes",
			(Gauge<Long>) () -> sorter1.getSpillInBytes() + sorter2.getSpillInBytes());
	}

	private void initSorter() throws Exception {
		// sorter1
		NormalizedKeyComputer computer1 = classes.computerClass1.newInstance();
		RecordComparator comparator1 = classes.comparatorClass1.newInstance();
		computer1.init(gSorter1.serializers(), gSorter1.comparators());
		comparator1.init(gSorter1.serializers(), gSorter1.comparators());
		this.sorter1 = new BinaryExternalSorter(this.getContainingTask(),
				memManager, reservedSortMemory1, preferredSortMemory1, perRequestMemory,
				ioManager, inputSerializer1, serializer1, computer1, comparator1);
		this.sorter1.startThreads();

		// sorter2
		NormalizedKeyComputer computer2 = classes.computerClass2.newInstance();
		RecordComparator comparator2 = classes.comparatorClass2.newInstance();
		computer2.init(gSorter2.serializers(), gSorter2.comparators());
		comparator2.init(gSorter2.serializers(), gSorter2.comparators());
		this.sorter2 = new BinaryExternalSorter(this.getContainingTask(), memManager, reservedSortMemory2,
				preferredSortMemory2, perRequestMemory, ioManager, inputSerializer2, serializer2, computer2,
				comparator2);
		this.sorter2.startThreads();
	}

	private void initKeyComparator() throws Exception {
		keyComparator = classes.keyComparatorClass.newInstance();
		keyComparator.init(keyGSorter.serializers(), keyGSorter.comparators());
	}

	protected CookedClasses cookGeneratedClasses(ClassLoader cl) throws CompileException {
		return new CookedClasses(
				compile(cl, keyGSorter.comparator().name(), keyGSorter.comparator().code()),
				compile(cl, projectionCode1.name(), projectionCode1.code()),
				compile(cl, projectionCode2.name(), projectionCode2.code()),
				compile(cl, gSorter1.computer().name(), gSorter1.computer().code()),
				compile(cl, gSorter2.computer().name(), gSorter2.computer().code()),
				compile(cl, gSorter1.comparator().name(), gSorter1.comparator().code()),
				compile(cl, gSorter2.comparator().name(), gSorter2.comparator().code())
		);
	}

	@Override
	public TwoInputSelection firstInputSelection() {
		return TwoInputSelection.ANY;
	}

	@Override
	public TwoInputSelection processRecord1(StreamRecord<BaseRow> element) throws Exception {
		this.sorter1.write(element.getValue());
		return TwoInputSelection.ANY;
	}

	@Override
	public TwoInputSelection processRecord2(StreamRecord<BaseRow> element) throws Exception {
		this.sorter2.write(element.getValue());
		return TwoInputSelection.ANY;
	}

	@Override
	public void endInput1() throws Exception {
		isFinished[0] = true;
		if (isFinished[1]) {
			doSortMergeCoTableValuedAgg();
		}
	}

	@Override
	public void endInput2() throws Exception {
		isFinished[1] = true;
		if (isFinished[0]) {
			doSortMergeCoTableValuedAgg();
		}
	}

	private void doSortMergeCoTableValuedAgg() throws Exception {

		Projection projection1 = classes.projectionClass1.newInstance();
		Projection projection2 = classes.projectionClass2.newInstance();
		MutableObjectIterator<BinaryRow> iterator1 = sorter1.getIterator();
		MutableObjectIterator<BinaryRow> iterator2 = sorter2.getIterator();

		joinIterator1 = new SortMergeCoTableValuedAggIterator(
				serializer1, serializer2, projection1, projection2,
				keyComparator, iterator1, iterator2);
		process(joinIterator1, true);
	}

	/**
	 * Sort merge and do accumulate.
	 */
	private void process(
		SortMergeCoTableValuedAggIterator iterator, boolean reverseInvoke) throws Exception {

		int inputSide;
		BaseRow accumulator;
		AppendGroupKeyCollector appendCollector = new AppendGroupKeyCollector();
		while ((inputSide = iterator.getNextSmallest()) != 0) {
			BinaryRow element = iterator.getElement();
			BinaryRow key = iterator.getKey();

			if (lastKey == null) {
				// create accumulator and set accumulators
				accumulator = function.createAccumulators();
				function.setAccumulators(accumulator);

			} else if (key.getSizeInBytes() != lastKey.getSizeInBytes() ||
				!(BinaryRowUtil.byteArrayEquals(key.getMemorySegment().getHeapMemory(),
					lastKey.getMemorySegment().getHeapMemory(),
					key.getSizeInBytes()))) {

				// key has been changed, emit resuts and set accumulators
				appendCollector.reSet(collector, lastKey, false);
				function.emitValue(appendCollector);
				accumulator = function.createAccumulators();
				function.setAccumulators(accumulator);
			}

			lastKey = key.copy();
			if (inputSide == 1) {
				function.accumulateLeft(element);
				iterator.nextLeft();
			} else {
				function.accumulateRight(element);
				iterator.nextRight();
			}
		}
		if (lastKey != null) {
			appendCollector.reSet(collector, lastKey, false);
			function.emitValue(appendCollector);
		}
	}

	@Override
	public void close() throws Exception {
		super.close();
		if (this.sorter1 != null) {
			this.sorter1.close();
		}
		if (this.sorter2 != null) {
			this.sorter2.close();
		}
		if (this.joinIterator1 != null) {
			this.joinIterator1.close();
		}
	}

	/**
	 * Generated classes.
	 */
	protected static class CookedClasses {

		protected CookedClasses(
				Class<RecordComparator> keyComparatorClass,
				Class<Projection> projectionClass1,
				Class<Projection> projectionClass2,
				Class<NormalizedKeyComputer> computerClass1,
				Class<NormalizedKeyComputer> computerClass2,
				Class<RecordComparator> comparatorClass1,
				Class<RecordComparator> comparatorClass2) {
			this.keyComparatorClass = keyComparatorClass;
			this.projectionClass1 = projectionClass1;
			this.projectionClass2 = projectionClass2;
			this.computerClass1 = computerClass1;
			this.computerClass2 = computerClass2;
			this.comparatorClass1 = comparatorClass1;
			this.comparatorClass2 = comparatorClass2;
		}

		protected final Class<RecordComparator> keyComparatorClass;
		protected final Class<Projection> projectionClass1;
		protected final Class<Projection> projectionClass2;
		protected final Class<NormalizedKeyComputer> computerClass1;
		protected final Class<NormalizedKeyComputer> computerClass2;
		protected final Class<RecordComparator> comparatorClass1;
		protected final Class<RecordComparator> comparatorClass2;
	}
}
