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
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.streaming.api.operators.TwoInputSelection;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.codegen.GeneratedJoinConditionFunction;
import org.apache.flink.table.codegen.GeneratedProjection;
import org.apache.flink.table.codegen.GeneratedSorter;
import org.apache.flink.table.codegen.JoinConditionFunction;
import org.apache.flink.table.codegen.Projection;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.dataformat.JoinedRow;
import org.apache.flink.table.plan.FlinkJoinRelType;
import org.apache.flink.table.runtime.operator.AbstractStreamOperatorWithMetrics;
import org.apache.flink.table.runtime.operator.StreamRecordCollector;
import org.apache.flink.table.runtime.sort.BinaryExternalSorter;
import org.apache.flink.table.runtime.sort.NormalizedKeyComputer;
import org.apache.flink.table.runtime.sort.RecordComparator;
import org.apache.flink.table.typeutils.AbstractRowSerializer;
import org.apache.flink.table.typeutils.BinaryRowSerializer;
import org.apache.flink.table.util.ResettableExternalBuffer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

import org.codehaus.commons.compiler.CompileException;

import java.util.BitSet;
import java.util.List;

import static org.apache.flink.table.codegen.CodeGenUtils.compile;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An implementation that realizes the joining through a sort-merge join strategy.
 */
public class SortMergeJoinOperator extends AbstractStreamOperatorWithMetrics<BaseRow>
		implements TwoInputStreamOperator<BaseRow, BaseRow, BaseRow> {

	private final long reservedSortMemory1;
	private final long preferredSortMemory1;
	private final long reservedSortMemory2;
	private final long preferredSortMemory2;
	private final long perRequestMemory;
	private final long externalBufferMemory;
	private final FlinkJoinRelType type;
	private final boolean leftIsSmaller;
	private final int maxNumFileHandles;

	// generated code to cook
	private GeneratedJoinConditionFunction condFuncCode;
	private GeneratedProjection projectionCode1;
	private GeneratedProjection projectionCode2;
	private GeneratedSorter gSorter1;
	private GeneratedSorter gSorter2;
	private GeneratedSorter keyGSorter;
	private final boolean[] filterNulls;

	private transient CookedClasses classes;

	private transient MemoryManager memManager;
	private transient IOManager ioManager;
	private transient TypeSerializer<BaseRow> inputSerializer1;
	private transient TypeSerializer<BaseRow> inputSerializer2;
	private transient BinaryRowSerializer serializer1;
	private transient BinaryRowSerializer serializer2;
	private transient BinaryExternalSorter sorter1;
	private transient BinaryExternalSorter sorter2;
	private transient SortMergeJoinIterator joinIterator1;
	private transient SortMergeJoinIterator joinIterator2;
	private transient SortMergeFullOuterJoinIterator fullOuterJoinIterator;
	private transient Collector<BaseRow> collector;
	private transient boolean[] isFinished;
	private transient JoinConditionFunction condFunc;
	private transient RecordComparator keyComparator;

	private transient BaseRow leftNullRow;
	private transient BaseRow rightNullRow;
	private transient JoinedRow joinedRow;

	public SortMergeJoinOperator(
			long reservedSortMemory, long preferredSortMemory,
			long perRequestMemory, long externalBufferMemory, FlinkJoinRelType type, boolean leftIsSmaller,
			GeneratedJoinConditionFunction condFuncCode,
			GeneratedProjection projectionCode1, GeneratedProjection projectionCode2,
			GeneratedSorter gSorter1, GeneratedSorter gSorter2, GeneratedSorter keyGSorter,
			boolean[] filterNulls, int maxNumFileHandles) {
		this(reservedSortMemory, preferredSortMemory, reservedSortMemory, preferredSortMemory, perRequestMemory,
				externalBufferMemory, type, leftIsSmaller, condFuncCode, projectionCode1, projectionCode2, gSorter1,
				gSorter2, keyGSorter, filterNulls, maxNumFileHandles);
	}

	public SortMergeJoinOperator(
			long reservedSortMemory1, long preferredSortMemory1,
			long reservedSortMemory2, long preferredSortMemory2,
			long perRequestMemory, long externalBufferMemory, FlinkJoinRelType type, boolean leftIsSmaller,
			GeneratedJoinConditionFunction condFuncCode,
			GeneratedProjection projectionCode1, GeneratedProjection projectionCode2,
			GeneratedSorter gSorter1, GeneratedSorter gSorter2, GeneratedSorter keyGSorter,
			boolean[] filterNulls, int maxNumFileHandles) {
		this.reservedSortMemory1 = reservedSortMemory1;
		this.preferredSortMemory1 = preferredSortMemory1;
		this.reservedSortMemory2 = reservedSortMemory2;
		this.preferredSortMemory2 = preferredSortMemory2;
		this.perRequestMemory = perRequestMemory;
		this.externalBufferMemory = externalBufferMemory;
		this.type = type;
		this.leftIsSmaller = leftIsSmaller;
		this.condFuncCode = condFuncCode;
		this.projectionCode1 = projectionCode1;
		this.projectionCode2 = projectionCode2;
		this.gSorter1 = checkNotNull(gSorter1);
		this.gSorter2 = checkNotNull(gSorter2);
		this.keyGSorter = checkNotNull(keyGSorter);
		this.filterNulls = filterNulls;
		this.maxNumFileHandles = maxNumFileHandles;
	}

	@Override
	public void open() throws Exception {
		super.open();

		// code gen classes.
		this.classes = cookGeneratedClasses(getContainingTask().getUserCodeClassLoader());

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

		this.condFunc = classes.condFuncClass.newInstance();

		this.leftNullRow = new GenericRow(serializer1.getNumFields());
		this.rightNullRow = new GenericRow(serializer2.getNumFields());
		this.joinedRow = new JoinedRow();

		condFuncCode = null;
		keyGSorter = null;
		projectionCode1 = null;
		projectionCode2 = null;
		gSorter1 = null;
		gSorter2 = null;

		getMetricGroup().gauge("memoryUsedSizeInBytes", new Gauge<Long>() {
			@Override
			public Long getValue() {
				return sorter1.getUsedMemoryInBytes() + sorter2.getUsedMemoryInBytes();
			}
		});

		getMetricGroup().gauge("numSpillFiles", new Gauge<Long>() {
			@Override
			public Long getValue() {
				return sorter1.getNumSpillFiles() + sorter2.getNumSpillFiles();
			}
		});

		getMetricGroup().gauge("spillInBytes", new Gauge<Long>() {
			@Override
			public Long getValue() {
				return sorter1.getSpillInBytes() + sorter2.getSpillInBytes();
			}
		});
	}

	private void initSorter() throws Exception {
		// sorter1
		NormalizedKeyComputer computer1 = classes.computerClass1.newInstance();
		RecordComparator comparator1 = classes.comparatorClass1.newInstance();
		computer1.init(gSorter1.serializers(), gSorter1.comparators());
		comparator1.init(gSorter1.serializers(), gSorter1.comparators());
		this.sorter1 = new BinaryExternalSorter(this.getContainingTask(),
				memManager, reservedSortMemory1, preferredSortMemory1, perRequestMemory,
				ioManager, inputSerializer1, serializer1, computer1, comparator1, maxNumFileHandles);
		this.sorter1.startThreads();

		// sorter2
		NormalizedKeyComputer computer2 = classes.computerClass2.newInstance();
		RecordComparator comparator2 = classes.comparatorClass2.newInstance();
		computer2.init(gSorter2.serializers(), gSorter2.comparators());
		comparator2.init(gSorter2.serializers(), gSorter2.comparators());
		this.sorter2 = new BinaryExternalSorter(this.getContainingTask(), memManager, reservedSortMemory2,
				preferredSortMemory2, perRequestMemory, ioManager, inputSerializer2, serializer2, computer2,
				comparator2, maxNumFileHandles);
		this.sorter2.startThreads();
	}

	private void initKeyComparator() throws Exception {
		keyComparator = classes.keyComparatorClass.newInstance();
		keyComparator.init(keyGSorter.serializers(), keyGSorter.comparators());
	}

	protected CookedClasses cookGeneratedClasses(ClassLoader cl) throws CompileException {
		return new CookedClasses(
				compile(cl, condFuncCode.name(), condFuncCode.code()),
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
		if (isAllFinished()) {
			doSortMergeJoin();
		}
	}

	@Override
	public void endInput2() throws Exception {
		isFinished[1] = true;
		if (isAllFinished()) {
			doSortMergeJoin();
		}
	}

	private void doSortMergeJoin() throws Exception {

		Projection projection1 = classes.projectionClass1.newInstance();
		Projection projection2 = classes.projectionClass2.newInstance();
		MutableObjectIterator<BinaryRow> iterator1 = sorter1.getIterator();
		MutableObjectIterator<BinaryRow> iterator2 = sorter2.getIterator();

		if (type.equals(FlinkJoinRelType.INNER)) {
			if (!leftIsSmaller) {
				joinIterator2 = new SortMergeInnerJoinIterator(
						serializer1, serializer2, projection1, projection2,
						keyComparator, iterator1, iterator2, newBuffer(serializer2), filterNulls);
				innerJoin((SortMergeInnerJoinIterator) joinIterator2, false);
			} else {
				joinIterator1 = new SortMergeInnerJoinIterator(
						serializer2, serializer1, projection2, projection1,
						keyComparator, iterator2, iterator1, newBuffer(serializer1), filterNulls);
				innerJoin((SortMergeInnerJoinIterator) joinIterator1, true);
			}
		} else if (type.equals(FlinkJoinRelType.LEFT)) {
			joinIterator2 = new SortMergeOneSideOuterJoinIterator(
					serializer1, serializer2, projection1, projection2,
					keyComparator, iterator1, iterator2, newBuffer(serializer2), filterNulls);
			oneSideOuterJoin((SortMergeOneSideOuterJoinIterator) joinIterator2, false, rightNullRow);
		} else if (type.equals(FlinkJoinRelType.RIGHT)) {
			joinIterator1 = new SortMergeOneSideOuterJoinIterator(
					serializer2, serializer1, projection2, projection1,
					keyComparator, iterator2, iterator1, newBuffer(serializer1), filterNulls);
			oneSideOuterJoin((SortMergeOneSideOuterJoinIterator) joinIterator1, true, leftNullRow);
		} else if (type.equals(FlinkJoinRelType.FULL)) {
			fullOuterJoinIterator = new SortMergeFullOuterJoinIterator(
					serializer1, serializer2, projection1, projection2,
					keyComparator, iterator1, iterator2,
					newBuffer(serializer1), newBuffer(serializer2), filterNulls);
			fullOuterJoin(fullOuterJoinIterator);
		} else if (type.equals(FlinkJoinRelType.SEMI)) {
			joinIterator2 = new SortMergeInnerJoinIterator(
					serializer1, serializer2, projection1, projection2,
					keyComparator, iterator1, iterator2, newBuffer(serializer2), filterNulls);
			while (((SortMergeInnerJoinIterator) joinIterator2).nextInnerJoin()) {
				BinaryRow probeRow = joinIterator2.getProbeRow();
				boolean matched = false;
				try (ResettableExternalBuffer.BufferIterator iter = joinIterator2.getMatchBuffer().newIterator()) {
					while (iter.advanceNext()) {
						BaseRow row = iter.getRow();
						if (condFunc.apply(probeRow, row)) {
							matched = true;
							break;
						}
					}
				}
				if (matched) {
					collector.collect(probeRow);
				}
			}
		} else if (type.equals(FlinkJoinRelType.ANTI)) {
			joinIterator2 = new SortMergeOneSideOuterJoinIterator(
					serializer1, serializer2, projection1, projection2,
					keyComparator, iterator1, iterator2, newBuffer(serializer2), filterNulls);
			while (((SortMergeOneSideOuterJoinIterator) joinIterator2).nextOuterJoin()) {
				BinaryRow probeRow = joinIterator2.getProbeRow();
				ResettableExternalBuffer matchBuffer = joinIterator2.getMatchBuffer();
				boolean matched = false;
				if (matchBuffer != null) {
					try (ResettableExternalBuffer.BufferIterator iter = matchBuffer.newIterator()) {
						while (iter.advanceNext()) {
							BaseRow row = iter.getRow();
							if (condFunc.apply(probeRow, row)) {
								matched = true;
								break;
							}
						}
					}
				}
				if (!matched) {
					collector.collect(probeRow);
				}
			}
		} else {
			throw new RuntimeException("Not support yet!");
		}
	}

	private ResettableExternalBuffer newBuffer(BinaryRowSerializer serializer) throws MemoryAllocationException {
		List<MemorySegment> externalBufferSegments = memManager.allocatePages(
				this.getContainingTask(), (int) (externalBufferMemory / memManager.getPageSize()));
		return new ResettableExternalBuffer(memManager, ioManager, externalBufferSegments, serializer);
	}

	private void innerJoin(
			SortMergeInnerJoinIterator iterator, boolean reverseInvoke) throws Exception {
		while (iterator.nextInnerJoin()) {
			BinaryRow probeRow = iterator.getProbeRow();
			try (ResettableExternalBuffer.BufferIterator iter = iterator.getMatchBuffer().newIterator()) {
				while (iter.advanceNext()) {
					BaseRow row = iter.getRow();
					joinWithCondition(probeRow, row, reverseInvoke);
				}
			}
		}
	}

	private boolean joinWithCondition(
			BaseRow row1, BaseRow row2, boolean reverseInvoke) throws Exception {
		if (reverseInvoke) {
			if (condFunc.apply(row2, row1)) {
				collector.collect(joinedRow.replace(row2, row1));
				return true;
			}
		} else {
			if (condFunc.apply(row1, row2)) {
				collector.collect(joinedRow.replace(row1, row2));
				return true;
			}
		}
		return false;
	}

	private void collect(
			BaseRow row1, BaseRow row2, boolean reverseInvoke) throws Exception {
		if (reverseInvoke) {
			collector.collect(joinedRow.replace(row2, row1));
		} else {
			collector.collect(joinedRow.replace(row1, row2));
		}
	}

	private void oneSideOuterJoin(
			SortMergeOneSideOuterJoinIterator iterator, boolean reverseInvoke,
			BaseRow buildNullRow) throws Exception {
		while (iterator.nextOuterJoin()) {
			BinaryRow probeRow = iterator.getProbeRow();
			boolean found = false;

			if (iterator.getMatchKey() != null) {
				try (ResettableExternalBuffer.BufferIterator iter = iterator.getMatchBuffer().newIterator()) {
					while (iter.advanceNext()) {
						BaseRow row = iter.getRow();
						found |= joinWithCondition(probeRow, row, reverseInvoke);
					}
				}
			}

			if (!found) {
				collect(probeRow, buildNullRow, reverseInvoke);
			}
		}
	}

	private void fullOuterJoin(SortMergeFullOuterJoinIterator iterator) throws Exception {
		BitSet bitSet = new BitSet();

		while (iterator.nextOuterJoin()) {

			bitSet.clear();
			BinaryRow matchKey = iterator.getMatchKey();
			ResettableExternalBuffer buffer1 = iterator.getBuffer1();
			ResettableExternalBuffer buffer2 = iterator.getBuffer2();

			if (matchKey == null && buffer1.size() > 0) { // left outer join.
				try (ResettableExternalBuffer.BufferIterator iter = buffer1.newIterator()) {
					while (iter.advanceNext()) {
						BaseRow row1 = iter.getRow();
						collector.collect(joinedRow.replace(row1, rightNullRow));
					}
				}
			} else if (matchKey == null && buffer2.size() > 0) { // right outer join.
				try (ResettableExternalBuffer.BufferIterator iter = buffer2.newIterator()) {
					while (iter.advanceNext()) {
						BaseRow row2 = iter.getRow();
						collector.collect(joinedRow.replace(leftNullRow, row2));
					}
				}
			} else if (matchKey != null) { // match join.
				try (ResettableExternalBuffer.BufferIterator iter1 = buffer1.newIterator()) {
					while (iter1.advanceNext()) {
						BaseRow row1 = iter1.getRow();
						boolean found = false;
						int index = 0;
						try (ResettableExternalBuffer.BufferIterator iter2 = buffer2.newIterator()) {
							while (iter2.advanceNext()) {
								BaseRow row2 = iter2.getRow();
								if (condFunc.apply(row1, row2)) {
									collector.collect(joinedRow.replace(row1, row2));
									found = true;
									bitSet.set(index);
								}
								index++;
							}
						}
						if (!found) {
							collector.collect(joinedRow.replace(row1, rightNullRow));
						}
					}
				}

				// row2 outer
				int index = 0;
				try (ResettableExternalBuffer.BufferIterator iter2 = buffer2.newIterator()) {
					while (iter2.advanceNext()) {
						BaseRow row2 = iter2.getRow();
						if (!bitSet.get(index)) {
							collector.collect(joinedRow.replace(leftNullRow, row2));
						}
						index++;
					}
				}
			} else { // bug...
				throw new RuntimeException("There is a bug.");
			}
		}
	}

	private boolean isAllFinished() {
		return isFinished[0] && isFinished[1];
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
		if (this.joinIterator2 != null) {
			this.joinIterator2.close();
		}
		if (this.fullOuterJoinIterator != null) {
			this.fullOuterJoinIterator.close();
		}
	}

	/**
	 * Generated classes.
	 */
	protected static class CookedClasses {

		protected CookedClasses(
				Class<JoinConditionFunction> condFuncClass,
				Class<RecordComparator> keyComparatorClass,
				Class<Projection> projectionClass1,
				Class<Projection> projectionClass2,
				Class<NormalizedKeyComputer> computerClass1,
				Class<NormalizedKeyComputer> computerClass2,
				Class<RecordComparator> comparatorClass1,
				Class<RecordComparator> comparatorClass2) {
			this.condFuncClass = condFuncClass;
			this.keyComparatorClass = keyComparatorClass;
			this.projectionClass1 = projectionClass1;
			this.projectionClass2 = projectionClass2;
			this.computerClass1 = computerClass1;
			this.computerClass2 = computerClass2;
			this.comparatorClass1 = comparatorClass1;
			this.comparatorClass2 = comparatorClass2;
		}

		protected final Class<JoinConditionFunction> condFuncClass;
		protected final Class<RecordComparator> keyComparatorClass;
		protected final Class<Projection> projectionClass1;
		protected final Class<Projection> projectionClass2;
		protected final Class<NormalizedKeyComputer> computerClass1;
		protected final Class<NormalizedKeyComputer> computerClass2;
		protected final Class<RecordComparator> comparatorClass1;
		protected final Class<RecordComparator> comparatorClass2;
	}
}
