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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


/**
 * Build shuffleStages.
 */
public class ShuffleStageGenerator {

	private final Map<RowBatchExecRel, ShuffleStage> relShuffleStageMap = new LinkedHashMap<>();

	public static Map<RowBatchExecRel, ShuffleStage> generate(RowBatchExecRel rootRel) {
		ShuffleStageGenerator generator = new ShuffleStageGenerator();
		generator.buildShuffleStages(rootRel);
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
			relShuffleStageMap.put(rowBatchExecRel, shuffleStage);
		} else if (!(rowBatchExecRel instanceof BatchExecExchange)) {
			ShuffleStage inputShuffleStage = getInputShuffleStage(rowBatchExecRel);
			if (inputShuffleStage == null) {
				inputShuffleStage = new ShuffleStage();
			}
			if (!isVirtualRel(rowBatchExecRel)) {
				inputShuffleStage.addRel(rowBatchExecRel);
			}
			relShuffleStageMap.put(rowBatchExecRel, inputShuffleStage);
		}
	}

	private ShuffleStage getInputShuffleStage(RowBatchExecRel rel) {
		ShuffleStage resultShuffleStage = null;
		for (RelNode input : rel.getInputs()) {
			RowBatchExecRel inputRel = (RowBatchExecRel) input;
			ShuffleStage oneInputShuffleStage = relShuffleStageMap.get(inputRel);
			if (oneInputShuffleStage != null) {
				if (resultShuffleStage == null) {
					resultShuffleStage = oneInputShuffleStage;
				} else {
					Set<RowBatchExecRel> oneInputRelSet = oneInputShuffleStage.getBatchExecRelSet();
					resultShuffleStage.addRelSet(oneInputRelSet);
					for (RowBatchExecRel oneInputRel : oneInputRelSet) {
						relShuffleStageMap.put(oneInputRel, resultShuffleStage);
					}
				}
			}
		}
		return resultShuffleStage;
	}

	private static boolean isVirtualRel(RowBatchExecRel rel) {
		if (rel instanceof BatchExecUnion || rel instanceof BatchExecReused) {
			return true;
		}
		return false;
	}

	private Map<RowBatchExecRel, ShuffleStage> getRelShuffleStageMap() {
		return relShuffleStageMap;
	}
}
