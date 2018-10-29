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

package org.apache.flink.table.resource.batch;

import org.apache.flink.runtime.io.network.DataExchangeMode;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.plan.batch.BatchExecRelVisitor;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecBoundedDataStreamScan;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecCalc;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecCorrelate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecExchange;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecExpand;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecHashAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecHashJoinBase;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecHashWindowAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecJoinBase;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecJoinTable;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecLimit;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecLocalHashAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecLocalHashWindowAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecLocalSortAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecLocalSortWindowAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecNestedLoopJoinBase;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecOverAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecRank;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecRel;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecReused;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecSort;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecSortAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecSortLimit;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecSortMergeJoinBase;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecSortWindowAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecTableSourceScan;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecUnion;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecValues;
import org.apache.flink.table.plan.nodes.physical.batch.RowBatchExecRel;
import org.apache.flink.table.resource.batch.RunningUnitGenerator.RelStageExchangeInfo;

import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Visit every batchExecRel to build runningUnits.
 */
public class RunningUnitGenerator implements BatchExecRelVisitor<List<RelStageExchangeInfo>> {

	private final Map<RowBatchExecRel, List<RelStageExchangeInfo>> outputInfoMap = new LinkedHashMap<>();
	private final List<RelRunningUnit> runningUnits = new LinkedList<>();
	private final TableConfig tableConfig;

	public RunningUnitGenerator(TableConfig tableConfig) {
		this.tableConfig = tableConfig;
	}

	public List<RelRunningUnit> getRunningUnits() {
		return runningUnits;
	}

	private void addIntoInputRunningUnit(List<RelStageExchangeInfo> inputInfoList, BatchExecRelStage relStage) {
		for (RelStageExchangeInfo inputInfo : inputInfoList) {
			if (inputInfo.exchangeMode == DataExchangeMode.BATCH) {
				relStage.addDependStage(inputInfo.outStage, BatchExecRelStage.DependType.DATA_TRIGGER);
			} else {
				for (RelRunningUnit inputRunningUnit : inputInfo.outStage.getRunningUnitList()) {
					inputRunningUnit.addRelStage(relStage);
					relStage.addRunningUnit(inputRunningUnit);
				}
			}
			if (relStage.getRunningUnitList().isEmpty()) {
				newRunningUnitWithRelStage(relStage);
			}
		}
	}

	private void newRunningUnitWithRelStage(BatchExecRelStage relStage) {
		RelRunningUnit runningUnit = new RelRunningUnit();
		runningUnits.add(runningUnit);
		runningUnit.addRelStage(relStage);
		relStage.addRunningUnit(runningUnit);
	}

	@Override
	public List<RelStageExchangeInfo> visit(BatchExecBoundedDataStreamScan boundedStreamScan) {
		return visitSourceRel(boundedStreamScan);
	}

	@Override
	public List<RelStageExchangeInfo> visit(BatchExecTableSourceScan scanTableSource) {
		return visitSourceRel(scanTableSource);
	}

	@Override
	public List<RelStageExchangeInfo> visit(BatchExecValues values) {
		return visitSourceRel(values);
	}

	private List<RelStageExchangeInfo> visitSourceRel(RowBatchExecRel sourceRel) {
		List<RelStageExchangeInfo> outputInfoList = outputInfoMap.get(sourceRel);
		if (outputInfoList == null) {
			BatchExecRelStage relStage = new BatchExecRelStage(sourceRel, 0);
			newRunningUnitWithRelStage(relStage);
			outputInfoList = Collections.singletonList(new RelStageExchangeInfo(relStage));
			outputInfoMap.put(sourceRel, outputInfoList);
		}
		return outputInfoList;
	}

	private <T extends SingleRel & RowBatchExecRel> List<RelStageExchangeInfo> visitOneStageSingleRel(
			T singleRel) {
		List<RelStageExchangeInfo> outputInfoList = outputInfoMap.get(singleRel);
		if (outputInfoList == null) {
			List<RelStageExchangeInfo> inputInfoList = ((RowBatchExecRel) singleRel.getInput()).accept(this);
			BatchExecRelStage relStage = new BatchExecRelStage(singleRel, 0);
			addIntoInputRunningUnit(inputInfoList, relStage);
			outputInfoList = Collections.singletonList(new RelStageExchangeInfo(relStage));
			outputInfoMap.put(singleRel, outputInfoList);
		}
		return outputInfoList;
	}

	@Override
	public List<RelStageExchangeInfo> visit(BatchExecCalc calc) {
		return visitOneStageSingleRel(calc);
	}

	@Override
	public List<RelStageExchangeInfo> visit(BatchExecCorrelate correlate) {
		return visitOneStageSingleRel(correlate);
	}

	@Override
	public List<RelStageExchangeInfo> visit(BatchExecExpand expand) {
		return visitOneStageSingleRel(expand);
	}

	@Override
	public List<RelStageExchangeInfo> visit(BatchExecLocalSortWindowAggregate localSortAggregate) {
		return visitOneStageSingleRel(localSortAggregate);
	}

	@Override
	public List<RelStageExchangeInfo> visit(BatchExecSortWindowAggregate sortAggregate) {
		return visitOneStageSingleRel(sortAggregate);
	}

	@Override
	public List<RelStageExchangeInfo> visit(BatchExecOverAggregate overWindowAgg) {
		return visitOneStageSingleRel(overWindowAgg);
	}

	@Override
	public List<RelStageExchangeInfo> visit(BatchExecLimit limit) {
		return visitOneStageSingleRel(limit);
	}

	@Override
	public List<RelStageExchangeInfo> visit(BatchExecLocalHashWindowAggregate localHashAggregate) {
		return visitOneStageSingleRel(localHashAggregate);
	}

	@Override
	public List<RelStageExchangeInfo> visit(BatchExecJoinTable joinTable) {
		return visitOneStageSingleRel(joinTable);
	}

	@Override
	public List<RelStageExchangeInfo> visit(BatchExecHashWindowAggregate hashAggregate) {
		return visitTwoStageSingleRel(hashAggregate);
	}

	private <T extends SingleRel & RowBatchExecRel> List<RelStageExchangeInfo> visitTwoStageSingleRel(
			T singleRel) {
		List<RelStageExchangeInfo> outputInfoList = outputInfoMap.get(singleRel);
		if (outputInfoList == null) {
			List<RelStageExchangeInfo> inputInfoList = ((RowBatchExecRel) singleRel.getInput()).accept(this);
			BatchExecRelStage inStage = new BatchExecRelStage(singleRel, 0);
			BatchExecRelStage outStage = new BatchExecRelStage(singleRel, 1);
			outStage.addDependStage(inStage, BatchExecRelStage.DependType.DATA_TRIGGER);
			addIntoInputRunningUnit(inputInfoList, inStage);
			newRunningUnitWithRelStage(outStage);

			outputInfoList = Collections.singletonList(new RelStageExchangeInfo(outStage));
			outputInfoMap.put(singleRel, outputInfoList);
		}
		return outputInfoList;
	}

	@Override
	public List<RelStageExchangeInfo> visit(BatchExecLocalHashAggregate localHashAggregate) {
		if (localHashAggregate.getGrouping().length == 0) {
			return visitTwoStageSingleRel(localHashAggregate);
		} else {
			return visitOneStageSingleRel(localHashAggregate);
		}
	}

	@Override
	public List<RelStageExchangeInfo> visit(BatchExecSortAggregate sortAggregate) {
		if (sortAggregate.getGrouping().length == 0) {
			return visitTwoStageSingleRel(sortAggregate);
		} else {
			return visitOneStageSingleRel(sortAggregate);
		}
	}

	@Override
	public List<RelStageExchangeInfo> visit(BatchExecLocalSortAggregate localSortAggregate) {
		if (localSortAggregate.getGrouping().length == 0) {
			return visitTwoStageSingleRel(localSortAggregate);
		} else {
			return visitOneStageSingleRel(localSortAggregate);
		}
	}

	@Override
	public List<RelStageExchangeInfo> visit(BatchExecSort sort) {
		return visitTwoStageSingleRel(sort);
	}

	@Override
	public List<RelStageExchangeInfo> visit(BatchExecSortLimit sortLimit) {
		return visitTwoStageSingleRel(sortLimit);
	}

	@Override
	public List<RelStageExchangeInfo> visit(BatchExecRank rank) {
		return visitOneStageSingleRel(rank);
	}

	@Override
	public List<RelStageExchangeInfo> visit(BatchExecHashAggregate hashAggregate) {
		return visitTwoStageSingleRel(hashAggregate);
	}

	private List<RelStageExchangeInfo> visitBuildProbeJoin(BatchExecJoinBase hashJoin,
			boolean leftIsBuild) {
		List<RelStageExchangeInfo> outputInfoList = outputInfoMap.get(hashJoin);
		if (outputInfoList == null) {
			BiRel joinRel = (BiRel) hashJoin;
			BatchExecRelStage buildStage = new BatchExecRelStage(hashJoin, 0);
			BatchExecRelStage probeStage = new BatchExecRelStage(hashJoin, 1);
			probeStage.addDependStage(buildStage, BatchExecRelStage.DependType.PRIORITY);
			RowBatchExecRel buildInput = (RowBatchExecRel) (leftIsBuild ? joinRel.getLeft() : joinRel.getRight());
			RowBatchExecRel probeInput = (RowBatchExecRel) (leftIsBuild ? joinRel.getRight() : joinRel.getLeft());

			List<RelStageExchangeInfo> buildInputInfoList = buildInput.accept(this);
			List<RelStageExchangeInfo> probeInputInfoList = probeInput.accept(this);

			addIntoInputRunningUnit(buildInputInfoList, buildStage);
			addIntoInputRunningUnit(probeInputInfoList, probeStage);

			outputInfoList = Collections.singletonList(new RelStageExchangeInfo(probeStage));
			outputInfoMap.put(hashJoin, outputInfoList);
		}
		return outputInfoList;
	}

	@Override
	public List<RelStageExchangeInfo> visit(BatchExecExchange exchange) {
		List<RelStageExchangeInfo> outputInfoList = outputInfoMap.get(exchange);
		if (outputInfoList == null) {
			List<RelStageExchangeInfo>  inputInfoList = ((RowBatchExecRel) exchange.getInput()).accept(this);
			if (exchange.getDataExchangeModeForDeadlockBreakup(tableConfig) == DataExchangeMode.BATCH) {
				outputInfoList = new ArrayList<>(inputInfoList.size());
				for (RelStageExchangeInfo relStageExchangeInfo : inputInfoList) {
					outputInfoList.add(new RelStageExchangeInfo(relStageExchangeInfo.outStage, DataExchangeMode.BATCH));
				}
			} else {
				outputInfoList = inputInfoList;
			}
			outputInfoMap.put(exchange, outputInfoList);
		}
		return outputInfoList;
	}

	@Override
	public List<RelStageExchangeInfo> visit(BatchExecReused reused) {
		return ((RowBatchExecRel) reused.getInput()).accept(this);
	}

	@Override
	public List<RelStageExchangeInfo> visit(BatchExecHashJoinBase hashJoin) {
		if (hashJoin.hashJoinType().buildLeftSemiOrAnti()) {
			List<RelStageExchangeInfo> outputInfoList = outputInfoMap.get(hashJoin);
			if (outputInfoList == null) {
				BiRel joinRel = (BiRel) hashJoin;
				BatchExecRelStage buildStage = new BatchExecRelStage(hashJoin, 0);
				BatchExecRelStage probeStage = new BatchExecRelStage(hashJoin, 1);
				BatchExecRelStage outStage = new BatchExecRelStage(hashJoin, 2);
				probeStage.addDependStage(buildStage, BatchExecRelStage.DependType.PRIORITY);
				outStage.addDependStage(probeStage, BatchExecRelStage.DependType.DATA_TRIGGER);

				RowBatchExecRel buildInput = (RowBatchExecRel) joinRel.getLeft();
				RowBatchExecRel probeInput = (RowBatchExecRel) joinRel.getRight();

				List<RelStageExchangeInfo> buildInputInfoList = buildInput.accept(this);
				List<RelStageExchangeInfo> probeInputInfoList = probeInput.accept(this);

				addIntoInputRunningUnit(buildInputInfoList, buildStage);
				addIntoInputRunningUnit(probeInputInfoList, probeStage);
				newRunningUnitWithRelStage(outStage);

				outputInfoList = Collections.singletonList(new RelStageExchangeInfo(outStage));
				outputInfoMap.put(hashJoin, outputInfoList);
			}
			return outputInfoList;
		}
		return visitBuildProbeJoin(hashJoin, hashJoin.leftIsBuild());
	}

	@Override
	public List<RelStageExchangeInfo> visit(BatchExecSortMergeJoinBase sortMergeJoin) {
		List<RelStageExchangeInfo> outputInfoList = outputInfoMap.get(sortMergeJoin);
		if (outputInfoList == null) {
			BiRel joinRel = (BiRel) sortMergeJoin;
			BatchExecRelStage in0Stage = new BatchExecRelStage(sortMergeJoin, 0);
			BatchExecRelStage in1Stage = new BatchExecRelStage(sortMergeJoin, 1);
			BatchExecRelStage outStage = new BatchExecRelStage(sortMergeJoin, 2);
			// in0Stage and in1Stage can be parallel
			outStage.addDependStage(in0Stage, BatchExecRelStage.DependType.DATA_TRIGGER);
			outStage.addDependStage(in1Stage, BatchExecRelStage.DependType.DATA_TRIGGER);

			List<RelStageExchangeInfo> in0InfoList = ((RowBatchExecRel) joinRel.getLeft()).accept(this);
			List<RelStageExchangeInfo> in1InfoList = ((RowBatchExecRel) joinRel.getRight()).accept(this);
			addIntoInputRunningUnit(in0InfoList, in0Stage);
			addIntoInputRunningUnit(in1InfoList, in1Stage);
			newRunningUnitWithRelStage(outStage);

			outputInfoList = Collections.singletonList(new RelStageExchangeInfo(outStage));
			outputInfoMap.put(sortMergeJoin, outputInfoList);
		}
		return outputInfoList;
	}

	@Override
	public List<RelStageExchangeInfo> visit(BatchExecNestedLoopJoinBase nestedLoopJoin) {
		return visitBuildProbeJoin(nestedLoopJoin, nestedLoopJoin.leftIsBuild());
	}

	@Override
	public List<RelStageExchangeInfo> visit(BatchExecUnion union) {
		List<RelStageExchangeInfo> outputInfoList = outputInfoMap.get(union);
		if (outputInfoList == null) {
			outputInfoList = new LinkedList<>();
			for (RelNode relNode : union.getInputs()) {
				outputInfoList.addAll(((RowBatchExecRel) relNode).accept(this));
			}
		}
		return outputInfoList;
	}

	@Override
	public List<RelStageExchangeInfo> visit(BatchExecRel<?> batchExec) {
		throw new TableException("could not reach here.");
	}

	/**
	 * RelStage with exchange info.
	 */
	protected static class RelStageExchangeInfo {

		private final BatchExecRelStage outStage;
		private final DataExchangeMode exchangeMode;

		public RelStageExchangeInfo(BatchExecRelStage outStage) {
			this(outStage, null);
		}

		public RelStageExchangeInfo(BatchExecRelStage outStage, DataExchangeMode exchangeMode) {
			this.outStage = outStage;
			this.exchangeMode = exchangeMode;
		}
	}
}
