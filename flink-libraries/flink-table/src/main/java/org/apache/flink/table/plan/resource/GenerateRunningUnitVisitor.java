/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.plan.resource;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.plan.BatchExecRelVisitor;
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

import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * Visit every batchExecRel to build runningUnits.
 */
public class GenerateRunningUnitVisitor implements BatchExecRelVisitor<List<ShuffleStageInRunningUnit>> {

	private final Map<RowBatchExecRel, List<ShuffleStageInRunningUnit>> outShuffleStageInRUsMap = new LinkedHashMap<>();
	private final List<RelRunningUnit> runningUnits = new LinkedList<>();
	private final RowBatchExecRel rootNode;

	public GenerateRunningUnitVisitor(RowBatchExecRel rootNode) {
		this.rootNode = rootNode;
	}

	private ShuffleStageInRunningUnit createShuffleStageInNewRU() {
		RelRunningUnit relRunningUnit = new RelRunningUnit();
		runningUnits.add(relRunningUnit);
		return relRunningUnit.newShuffleStageInRU();
	}

	public List<RelRunningUnit> getRunningUnits() {
		return runningUnits;
	}

	@Override
	public List<ShuffleStageInRunningUnit> visit(BatchExecBoundedDataStreamScan boundedStreamScan) {
		return visitSourceRel(boundedStreamScan);
	}

	@Override
	public List<ShuffleStageInRunningUnit> visit(BatchExecTableSourceScan scanTableSource) {
		return visitSourceRel(scanTableSource);
	}

	@Override
	public List<ShuffleStageInRunningUnit> visit(BatchExecValues values) {
		return visitSourceRel(values);
	}

	private List<ShuffleStageInRunningUnit> visitSourceRel(RowBatchExecRel sourceRel) {
		List<ShuffleStageInRunningUnit> outShuffleStageInRUs = outShuffleStageInRUsMap.get(sourceRel);
		if (outShuffleStageInRUs == null) {
			ShuffleStageInRunningUnit shuffleStageInRU = createShuffleStageInNewRU();
			BatchExecRelStage stage = new BatchExecRelStage(sourceRel, 0);
			shuffleStageInRU.addRelStage(stage);
			outShuffleStageInRUs = Collections.singletonList(shuffleStageInRU);
			outShuffleStageInRUsMap.put(sourceRel, outShuffleStageInRUs);
		}
		return outShuffleStageInRUs;
	}

	private void addRelStage(List<ShuffleStageInRunningUnit> shuffleStageInRUs, BatchExecRelStage stage) {
		for (ShuffleStageInRunningUnit shuffleStageInRU : shuffleStageInRUs) {
			shuffleStageInRU.addRelStage(stage);
		}
	}

	private <T extends SingleRel & RowBatchExecRel> List<ShuffleStageInRunningUnit> visitOneStageSingleRel(
			T singleRel) {
		List<ShuffleStageInRunningUnit> outShuffleStageInRUs = outShuffleStageInRUsMap.get(singleRel);
		if (outShuffleStageInRUs == null) {
			List<ShuffleStageInRunningUnit> inputShuffleStageInRUs = ((RowBatchExecRel) singleRel.getInput()).accept(this);
			BatchExecRelStage stage = new BatchExecRelStage(singleRel, 0);
			addRelStage(inputShuffleStageInRUs, stage);
			outShuffleStageInRUs = inputShuffleStageInRUs;
			outShuffleStageInRUsMap.put(singleRel, outShuffleStageInRUs);
		}
		return outShuffleStageInRUs;
	}

	@Override
	public List<ShuffleStageInRunningUnit> visit(BatchExecCalc calc) {
		return visitOneStageSingleRel(calc);
	}

	@Override
	public List<ShuffleStageInRunningUnit> visit(BatchExecCorrelate correlate) {
		return visitOneStageSingleRel(correlate);
	}

	@Override
	public List<ShuffleStageInRunningUnit> visit(BatchExecExpand expand) {
		return visitOneStageSingleRel(expand);
	}

	@Override
	public List<ShuffleStageInRunningUnit> visit(BatchExecLocalSortWindowAggregate localSortAggregate) {
		return visitOneStageSingleRel(localSortAggregate);
	}

	@Override
	public List<ShuffleStageInRunningUnit> visit(BatchExecSortWindowAggregate sortAggregate) {
		return visitOneStageSingleRel(sortAggregate);
	}

	@Override
	public List<ShuffleStageInRunningUnit> visit(BatchExecOverAggregate overWindowAgg) {
		return visitOneStageSingleRel(overWindowAgg);
	}

	@Override
	public List<ShuffleStageInRunningUnit> visit(BatchExecLimit limit) {
		return visitOneStageSingleRel(limit);
	}

	@Override
	public List<ShuffleStageInRunningUnit> visit(BatchExecLocalHashWindowAggregate localHashAggregate) {
		return visitOneStageSingleRel(localHashAggregate);
	}

	@Override
	public List<ShuffleStageInRunningUnit> visit(BatchExecJoinTable joinTable) {
		return visitOneStageSingleRel(joinTable);
	}

	@Override
	public List<ShuffleStageInRunningUnit> visit(BatchExecHashWindowAggregate hashAggregate) {
		return visitTwoStageSingleRel(hashAggregate);
	}

	private <T extends SingleRel & RowBatchExecRel> List<ShuffleStageInRunningUnit> visitTwoStageSingleRel(
			T singleRel) {
		List<ShuffleStageInRunningUnit> outShuffleStageInRUs = outShuffleStageInRUsMap.get(singleRel);
		if (outShuffleStageInRUs == null) {
			List<ShuffleStageInRunningUnit> inputShuffleStageInRUs = ((RowBatchExecRel) singleRel.getInput()).accept(this);
			BatchExecRelStage inStage = new BatchExecRelStage(singleRel, 0);
			BatchExecRelStage outStage = new BatchExecRelStage(singleRel, 1);
			outStage.addDependStage(inStage);
			addRelStage(inputShuffleStageInRUs, inStage);
			ShuffleStageInRunningUnit outShuffleStageInRU = createShuffleStageInNewRU();
			outShuffleStageInRU.addRelStage(outStage);
			outShuffleStageInRUs = Collections.singletonList(outShuffleStageInRU);
			outShuffleStageInRUsMap.put(singleRel, outShuffleStageInRUs);
		}
		return outShuffleStageInRUs;
	}

	@Override
	public List<ShuffleStageInRunningUnit> visit(BatchExecLocalHashAggregate localHashAggregate) {
		if (localHashAggregate.getGrouping().length == 0) {
			return visitTwoStageSingleRel(localHashAggregate);
		} else {
			return visitOneStageSingleRel(localHashAggregate);
		}
	}

	@Override
	public List<ShuffleStageInRunningUnit> visit(BatchExecSortAggregate sortAggregate) {
		if (sortAggregate.getGrouping().length == 0) {
			return visitTwoStageSingleRel(sortAggregate);
		} else {
			return visitOneStageSingleRel(sortAggregate);
		}
	}

	@Override
	public List<ShuffleStageInRunningUnit> visit(BatchExecLocalSortAggregate localSortAggregate) {
		if (localSortAggregate.getGrouping().length == 0) {
			return visitTwoStageSingleRel(localSortAggregate);
		} else {
			return visitOneStageSingleRel(localSortAggregate);
		}
	}

	@Override
	public List<ShuffleStageInRunningUnit> visit(BatchExecSort sort) {
		return visitTwoStageSingleRel(sort);
	}

	@Override
	public List<ShuffleStageInRunningUnit> visit(BatchExecSortLimit sortLimit) {
		return visitTwoStageSingleRel(sortLimit);
	}

	@Override
	public List<ShuffleStageInRunningUnit> visit(BatchExecRank rank) {
		return visitOneStageSingleRel(rank);
	}

	@Override
	public List<ShuffleStageInRunningUnit> visit(BatchExecHashAggregate hashAggregate) {
		return visitTwoStageSingleRel(hashAggregate);
	}

	private List<ShuffleStageInRunningUnit> visitBuildProbeJoin(BatchExecJoinBase hashJoin,
			boolean leftIsBuild) {
		List<ShuffleStageInRunningUnit> outShuffleStageInRUs = outShuffleStageInRUsMap.get(hashJoin);
		if (outShuffleStageInRUs == null) {
			BiRel joinRel = (BiRel) hashJoin;
			BatchExecRelStage buildStage = new BatchExecRelStage(hashJoin, 0);
			BatchExecRelStage probeStage = new BatchExecRelStage(hashJoin, 1);
			probeStage.addDependStage(buildStage);
			RowBatchExecRel buildInput = (RowBatchExecRel) (leftIsBuild ? joinRel.getLeft() : joinRel.getRight());
			RowBatchExecRel probeInput = (RowBatchExecRel) (leftIsBuild ? joinRel.getRight() : joinRel.getLeft());

			List<ShuffleStageInRunningUnit> buildShuffleStageInRUs = buildInput.accept(this);
			List<ShuffleStageInRunningUnit> probeShuffleStageInRUs = probeInput.accept(this);
			addRelStage(buildShuffleStageInRUs, buildStage);
			addRelStage(probeShuffleStageInRUs, probeStage);
			outShuffleStageInRUs = probeShuffleStageInRUs;
			outShuffleStageInRUsMap.put(hashJoin, outShuffleStageInRUs);
		}
		return outShuffleStageInRUs;
	}

	@Override
	public List<ShuffleStageInRunningUnit> visit(BatchExecExchange exchange) {
		List<ShuffleStageInRunningUnit> inputShuffleStageInRUs = ((RowBatchExecRel) exchange.getInput()).accept(this);
		List<ShuffleStageInRunningUnit> results = new LinkedList<>();
		for (ShuffleStageInRunningUnit inputShuffleStageInRU : inputShuffleStageInRUs) {
			results.add(checkEmptyShuffleStageInRU(inputShuffleStageInRU));
		}
		return results;
	}

	@Override
	public List<ShuffleStageInRunningUnit> visit(BatchExecReused reused) {
		return ((RowBatchExecRel) reused.getInput()).accept(this);
	}

	@Override
	public List<ShuffleStageInRunningUnit> visit(BatchExecHashJoinBase hashJoin) {
		if (hashJoin.hashJoinType().buildLeftSemiOrAnti()) {
			List<ShuffleStageInRunningUnit> outShuffleStageInRUs = outShuffleStageInRUsMap.get(hashJoin);
			if (outShuffleStageInRUs == null) {
				BiRel joinRel = (BiRel) hashJoin;
				BatchExecRelStage buildStage = new BatchExecRelStage(hashJoin, 0);
				BatchExecRelStage probeStage = new BatchExecRelStage(hashJoin, 1);
				BatchExecRelStage outStage = new BatchExecRelStage(hashJoin, 2);
				probeStage.addDependStage(buildStage);
				outStage.addDependStage(probeStage);

				RowBatchExecRel buildInput = (RowBatchExecRel) joinRel.getLeft();
				RowBatchExecRel probeInput = (RowBatchExecRel) joinRel.getRight();

				List<ShuffleStageInRunningUnit> buildShuffleStageInRUs = buildInput.accept(this);
				List<ShuffleStageInRunningUnit> probeShuffleStageInRUs = probeInput.accept(this);
				addRelStage(buildShuffleStageInRUs, buildStage);
				addRelStage(probeShuffleStageInRUs, probeStage);
				ShuffleStageInRunningUnit outShuffleStageInRU = createShuffleStageInNewRU();
				outShuffleStageInRUs = Collections.singletonList(outShuffleStageInRU);
				addRelStage(outShuffleStageInRUs, outStage);
				outShuffleStageInRUsMap.put(hashJoin, outShuffleStageInRUs);
			}
			return outShuffleStageInRUs;
		}
		return visitBuildProbeJoin(hashJoin, hashJoin.leftIsBuild());
	}

	@Override
	public List<ShuffleStageInRunningUnit> visit(BatchExecSortMergeJoinBase sortMergeJoin) {
		List<ShuffleStageInRunningUnit> outShuffleStageInRUs = outShuffleStageInRUsMap.get(sortMergeJoin);
		if (outShuffleStageInRUs == null) {
			BiRel joinRel = (BiRel) sortMergeJoin;
			BatchExecRelStage in0Stage = new BatchExecRelStage(sortMergeJoin, 0);
			BatchExecRelStage in1Stage = new BatchExecRelStage(sortMergeJoin, 1);
			BatchExecRelStage outStage = new BatchExecRelStage(sortMergeJoin, 2);
			// in0Stage and in1Stage can be parallel
			outStage.addDependStage(in0Stage);
			outStage.addDependStage(in1Stage);
			List<ShuffleStageInRunningUnit> in0ShuffleStageInRUs = ((RowBatchExecRel) joinRel.getLeft()).accept(this);
			List<ShuffleStageInRunningUnit> in1ShuffleStageInRUs = ((RowBatchExecRel) joinRel.getRight()).accept(this);
			addRelStage(in0ShuffleStageInRUs, in0Stage);
			addRelStage(in1ShuffleStageInRUs, in1Stage);
			ShuffleStageInRunningUnit outShuffleStageInRU = createShuffleStageInNewRU();
			outShuffleStageInRU.addRelStage(outStage);
			outShuffleStageInRUs = Collections.singletonList(outShuffleStageInRU);
			outShuffleStageInRUsMap.put(sortMergeJoin, outShuffleStageInRUs);
		}
		return outShuffleStageInRUs;
	}

	@Override
	public List<ShuffleStageInRunningUnit> visit(BatchExecNestedLoopJoinBase nestedLoopJoin) {
		return visitBuildProbeJoin(nestedLoopJoin, nestedLoopJoin.leftIsBuild());
	}

	@Override
	public List<ShuffleStageInRunningUnit> visit(BatchExecUnion union) {
		List<ShuffleStageInRunningUnit> shuffleStageInRUs = new ArrayList<>();
		for (RelNode relNode : union.getInputs()) {
			shuffleStageInRUs.addAll(((RowBatchExecRel) relNode).accept(this));
		}

		List<ShuffleStageInRunningUnit> results = new ArrayList<>();
		if (union == rootNode) {
			return results;
		}

		results.addAll(shuffleStageInRUs.stream().map(
			this::checkEmptyShuffleStageInRU).collect(Collectors.toList()));
		return results;
	}

	@Override
	public List<ShuffleStageInRunningUnit> visit(BatchExecRel<?> batchExec) {
		throw new TableException("could not reach here.");
	}

	private ShuffleStageInRunningUnit checkEmptyShuffleStageInRU(ShuffleStageInRunningUnit rightShuffleStageInRU) {
		if (rightShuffleStageInRU.getRelStages().isEmpty()) {
			return rightShuffleStageInRU;
		} else {
			return rightShuffleStageInRU.getRelRunningUnit().newShuffleStageInRU();
		}
	}

}
