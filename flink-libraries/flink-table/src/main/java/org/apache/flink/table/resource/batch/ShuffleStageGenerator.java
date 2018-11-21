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

import org.apache.flink.table.plan.nodes.physical.batch.BatchExecExchange;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecRel;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecUnion;

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

	private final Map<BatchExecRel<?>, ShuffleStage> relShuffleStageMap = new LinkedHashMap<>();

	public static Map<BatchExecRel<?>, ShuffleStage> generate(BatchExecRel<?> rootRel) {
		ShuffleStageGenerator generator = new ShuffleStageGenerator();
		generator.buildShuffleStages(rootRel);
		Map<BatchExecRel<?>, ShuffleStage> result = generator.getRelShuffleStageMap();
		result.values().forEach(s -> {
			List<BatchExecRel<?>> virtualRelList = s.getBatchExecRelSet().stream().filter(ShuffleStageGenerator::isVirtualRel).collect(toList());
			virtualRelList.forEach(s::removeRel);
		});
		return generator.getRelShuffleStageMap().entrySet().stream()
				.filter(x -> !isVirtualRel(x.getKey()))
				.collect(Collectors.toMap(Map.Entry::getKey,
						Map.Entry::getValue,
						(e1, e2) -> e1,
						LinkedHashMap::new));
	}

	private void buildShuffleStages(BatchExecRel<?> batchExecRel) {
		if (relShuffleStageMap.containsKey(batchExecRel)) {
			return;
		}
		List<RelNode> inputs = batchExecRel.getInputs();
		for (RelNode relNode : inputs) {
			buildShuffleStages(((BatchExecRel<?>) relNode));
		}

		if (inputs.isEmpty()) {
			// source rel
			ShuffleStage shuffleStage = new ShuffleStage();
			shuffleStage.addRel(batchExecRel);
			if (batchExecRel.resultPartitionCount() > 0) {
				shuffleStage.setResultParallelism(batchExecRel.resultPartitionCount(), true);
			}
			relShuffleStageMap.put(batchExecRel, shuffleStage);
		} else if (!(batchExecRel instanceof BatchExecExchange)) {
			Set<ShuffleStage> inputShuffleStages = getInputShuffleStages(batchExecRel);
			ShuffleStage inputShuffleStage = mergeInputShuffleStages(inputShuffleStages, batchExecRel.resultPartitionCount());
			inputShuffleStage.addRel(batchExecRel);
			relShuffleStageMap.put(batchExecRel, inputShuffleStage);
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
		Set<BatchExecRel<?>> relSet = other.getBatchExecRelSet();
		shuffleStage.addRelSet(relSet);
		for (BatchExecRel<?> r : relSet) {
			relShuffleStageMap.put(r, shuffleStage);
		}
	}

	private Set<ShuffleStage> getInputShuffleStages(BatchExecRel<?> rel) {
		Set<ShuffleStage> shuffleStageList = new HashSet<>();
		for (RelNode input : rel.getInputs()) {
			ShuffleStage oneInputShuffleStage = relShuffleStageMap.get((BatchExecRel<?>) input);
			if (oneInputShuffleStage != null) {
				shuffleStageList.add(oneInputShuffleStage);
			}
		}
		return shuffleStageList;
	}

	private static boolean isVirtualRel(BatchExecRel<?> rel) {
		return rel instanceof BatchExecUnion;
	}

	private Map<BatchExecRel<?>, ShuffleStage> getRelShuffleStageMap() {
		return relShuffleStageMap;
	}
}
