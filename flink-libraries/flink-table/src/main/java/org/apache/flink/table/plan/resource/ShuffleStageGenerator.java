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

import org.apache.flink.table.plan.nodes.physical.batch.BatchExecExchange;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecReused;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecUnion;
import org.apache.flink.table.plan.nodes.physical.batch.RowBatchExecRel;

import org.apache.calcite.rel.RelNode;

import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;


/**
 * Build shuffleStages.
 */
public class ShuffleStageGenerator {

	private final Map<RowBatchExecRel, ShuffleStage> relShuffleStageMap = new LinkedHashMap<>();

	public static Map<RowBatchExecRel, ShuffleStage> generate(RowBatchExecRel rootRel) {
		ShuffleStageGenerator generator = new ShuffleStageGenerator();
		generator.buildShuffleStages(rootRel);
		Map<RowBatchExecRel, ShuffleStage> result = generator.getRelShuffleStageMap();
		result.values().forEach(s -> {
			List<RowBatchExecRel> virtualRelList = s.getBatchExecRelSet().stream().filter(ShuffleStageGenerator::isVirtualRel).collect(toList());
			virtualRelList.forEach(s::removeRel);
		});
		return generator.getRelShuffleStageMap().entrySet().stream()
				.filter(x -> !isVirtualRel(x.getKey()))
				.collect(Collectors.toMap(Map.Entry::getKey,
						Map.Entry::getValue,
						(e1, e2) -> e1,
						LinkedHashMap::new));
	}

	private void buildShuffleStages(RowBatchExecRel rowBatchExecRel) {
		if (relShuffleStageMap.containsKey(rowBatchExecRel)) {
			return;
		}
		List<RelNode> inputs = rowBatchExecRel.getInputs();
		for (RelNode relNode : inputs) {
			buildShuffleStages(((RowBatchExecRel) relNode));
		}

		if (inputs.isEmpty()) {
			// source rel
			ShuffleStage shuffleStage = new ShuffleStage();
			shuffleStage.addRel(rowBatchExecRel);
			if (rowBatchExecRel.resultPartitionCount() > 0) {
				shuffleStage.setResultParallelism(rowBatchExecRel.resultPartitionCount(), true);
			}
			relShuffleStageMap.put(rowBatchExecRel, shuffleStage);
		} else if (!(rowBatchExecRel instanceof BatchExecExchange)) {
			Set<ShuffleStage> inputShuffleStages = getInputShuffleStages(rowBatchExecRel);
			ShuffleStage inputShuffleStage = mergeInputShuffleStages(inputShuffleStages, rowBatchExecRel.resultPartitionCount());
			inputShuffleStage.addRel(rowBatchExecRel);
			relShuffleStageMap.put(rowBatchExecRel, inputShuffleStage);
		}
	}

	private ShuffleStage mergeInputShuffleStages(Set<ShuffleStage> shuffleStageSet, int relParallelism) {
		if (relParallelism > 0) {
			ShuffleStage resultShuffleStage = new ShuffleStage();
			resultShuffleStage.setResultParallelism(relParallelism, true);
			for (ShuffleStage shuffleStage : shuffleStageSet) {
				if (!shuffleStage.isParallelismFinal() || shuffleStage.getResultParallelism() == relParallelism) {
					mergeShuffleStage(resultShuffleStage, shuffleStage);
				}
			}
			return resultShuffleStage;
		} else {
			ShuffleStage resultShuffleStage = shuffleStageSet.stream()
					.filter(ShuffleStage::isParallelismFinal)
					.max(Comparator.comparing(ShuffleStage::getResultParallelism))
					.orElse(new ShuffleStage());
			for (ShuffleStage shuffleStage : shuffleStageSet) {
				if (!shuffleStage.isParallelismFinal() || shuffleStage.getResultParallelism() == resultShuffleStage.getResultParallelism()) {
					mergeShuffleStage(resultShuffleStage, shuffleStage);
				}
			}
			return resultShuffleStage;
		}
	}

	private void mergeShuffleStage(ShuffleStage shuffleStage, ShuffleStage other) {
		Set<RowBatchExecRel> relSet = other.getBatchExecRelSet();
		shuffleStage.addRelSet(relSet);
		for (RowBatchExecRel r : relSet) {
			relShuffleStageMap.put(r, shuffleStage);
		}
	}

	private Set<ShuffleStage> getInputShuffleStages(RowBatchExecRel rel) {
		Set<ShuffleStage> shuffleStageList = new HashSet<>();
		for (RelNode input : rel.getInputs()) {
			ShuffleStage oneInputShuffleStage = relShuffleStageMap.get((RowBatchExecRel) input);
			if (oneInputShuffleStage != null) {
				shuffleStageList.add(oneInputShuffleStage);
			}
		}
		return shuffleStageList;
	}

	private static boolean isVirtualRel(RowBatchExecRel rel) {
		return (rel instanceof BatchExecUnion || rel instanceof BatchExecReused);
	}

	private Map<RowBatchExecRel, ShuffleStage> getRelShuffleStageMap() {
		return relShuffleStageMap;
	}
}
