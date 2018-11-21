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

import org.apache.flink.streaming.api.transformations.StreamTransformation;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecRel;
import org.apache.flink.table.util.FlinkRelOptUtil;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Describe which stage of a batchExecRel.
 * e.g. SortAggRel has two stages, the first stage process input data and the second stage
 * output results. There is a pause between them and the second stage works after the first ends.
 *
 * <p>e.g. CalcRel has only a stage, it receives input data and output result in a stage.
 *
 * <p>BatchExecRelStage has three elements: batchExecRel, stageID, other relStages that the stage
 * depends on.
 * There are two depend type: DATA_TRIGGER, PRIORITY.
 *
 * <p>e.g. When the first stage of sortAggRel ends, the rel needs to output data, trigger the second
 * stage to run. So its depend type is DATA_TRIGGER.
 *
 * <p>e.g. SortAggRel output data to a calcRel, but the exchangeMode between them is batch mode(spilling
 * data to disk), so the calcRel stage depends on the second stage of sortAggRel. Depend type is DATA_TRIGGER.
 *
 * <p>e.g. HashJoinRel has two stages: build stage(receive build input and build hashTable) and
 * probe stage(receive probe input and output result). The hashJoinRel prefers to read build input
 * if its build and probe inputs are all ready. So the depend type is PRIORITY.
 *
 * <p>e.g. SortMergeJoinRel has three stages: two input stages and output stage. its two input stages
 * are parallel. So the output stage depend on two input stages and depend type is DATA_TRIGGER.
 */
public class BatchExecRelStage implements Serializable {

	/**
	 * Depend type.
	 */
	public enum DependType {
		DATA_TRIGGER, PRIORITY
	}

	private final transient BatchExecRel<?> batchExecRel;
	private final Set<RelRunningUnit> runningUnitSet = new LinkedHashSet<>();
	private int stageID;

	// the rel stage may depend on many relStages, e.g. SortMergeJoinRel.
	private final Map<DependType, List<BatchExecRelStage>> dependStagesMap = new LinkedHashMap<>();

	/**
	 * There may be more than one transformation in a BatchExecRel, like BatchExecScan.
	 * But these transformations must chain in a jobVertex in runtime.
	 */
	private final List<Integer> transformationIDList = new LinkedList<>();
	private final String relName;

	public BatchExecRelStage(BatchExecRel<?> batchExecRel, int stageID) {
		this.batchExecRel = batchExecRel;
		this.stageID = stageID;
		this.relName = FlinkRelOptUtil.getDigest(batchExecRel, false);
	}

	public void addTransformation(StreamTransformation<?> transformation) {
		transformationIDList.add(transformation.getId());
	}

	public List<Integer> getTransformationIDList() {
		return transformationIDList;
	}

	public List<BatchExecRelStage> getDependStageList(DependType type) {
		return dependStagesMap.computeIfAbsent(type, k -> new LinkedList<>());
	}

	public void removeDependStage(BatchExecRelStage toRemove) {
		for (List<BatchExecRelStage> stageList : dependStagesMap.values()) {
			stageList.remove(toRemove);
		}
	}

	public void addDependStage(BatchExecRelStage relStage, DependType type) {
		dependStagesMap.computeIfAbsent(type, k -> new LinkedList<>()).add(relStage);
	}

	public BatchExecRel<?> getBatchExecRel() {
		return batchExecRel;
	}

	public Set<RelRunningUnit> getRunningUnitList() {
		return runningUnitSet;
	}

	public void addRunningUnit(RelRunningUnit relRunningUnit) {
		runningUnitSet.add(relRunningUnit);
	}

	public List<BatchExecRelStage> getAllDependStageList() {
		List<BatchExecRelStage> allStageList = new ArrayList<>();
		dependStagesMap.values().forEach(allStageList::addAll);
		return allStageList;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		BatchExecRelStage stage = (BatchExecRelStage) o;

		if (stageID != stage.stageID) {
			return false;
		}
		return batchExecRel != null ? batchExecRel.equals(stage.batchExecRel) : stage.batchExecRel == null;
	}

	@Override
	public int hashCode() {
		int result = batchExecRel != null ? batchExecRel.hashCode() : 0;
		result = 31 * result + stageID;
		return result;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("relStage(");
		sb.append("batchExecRel=").append(relName)
				.append(", stageID=").append(stageID);
		dependStagesMap.forEach((k, v) -> {
			sb.append(", depend type: " + k + " =[");
			for (BatchExecRelStage relStage : v) {
				sb.append("batchExecRel=").append(relStage.relName)
						.append(", stageID=").append(relStage.stageID).append(";");
			}
			sb.append("]");
		});
		return sb.toString();
	}
}
