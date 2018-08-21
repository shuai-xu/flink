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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * RelRunningUnit contains some batchExecRel that run at the same time.
 */
public class RelRunningUnit implements Serializable {
	private final List<ShuffleStageInRunningUnit> shuffleStageInRUs = new ArrayList<>();

	public ShuffleStageInRunningUnit newShuffleStageInRU() {
		ShuffleStageInRunningUnit shuffleStageInRU = new ShuffleStageInRunningUnit(this);
		shuffleStageInRUs.add(shuffleStageInRU);
		return shuffleStageInRU;
	}

	public List<ShuffleStageInRunningUnit> getShuffleStagesInRunningUnit() {
		return shuffleStageInRUs;
	}

	public List<BatchExecRelStage> getAllRelStages() {
		List<BatchExecRelStage> stages = new LinkedList<>();
		for (ShuffleStageInRunningUnit shuffleStageInRU : shuffleStageInRUs) {
			stages.addAll(shuffleStageInRU.getRelStages());
		}
		return stages;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("RelRunningUnit{");
		for (ShuffleStageInRunningUnit shuffleStageInRU : shuffleStageInRUs) {
			sb.append("\n\t").append("shuffleStage:[");
			for (BatchExecRelStage stage : shuffleStageInRU.getRelStages()) {
				sb.append("\n\t\t").append(stage);
			}
			sb.append("\n\t]");
		}
		sb.append("\n}");
		return sb.toString();
	}
}
