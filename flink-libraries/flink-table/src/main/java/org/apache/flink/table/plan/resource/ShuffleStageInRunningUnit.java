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

import org.apache.flink.table.plan.nodes.physical.batch.RowBatchExecRel;

import java.io.Serializable;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * In a relRunningUnit, some contiguous batchExecRel transfer data without shuffle, so we need
 * to put them together, and set the same parallelism.
 */
public class ShuffleStageInRunningUnit implements Serializable {

	private final List<BatchExecRelStage> stages = new LinkedList<>();
	private final transient Set<RowBatchExecRel> relSet = new LinkedHashSet<>();
	private final transient RelRunningUnit relRunningUnit;
	private transient ShuffleStage shuffleStage;

	public ShuffleStageInRunningUnit(RelRunningUnit relRunningUnit) {
		this.relRunningUnit = relRunningUnit;
	}

	public RelRunningUnit getRelRunningUnit() {
		return relRunningUnit;
	}

	public ShuffleStage getShuffleStage() {
		return shuffleStage;
	}

	public void setShuffleStage(ShuffleStage shuffleStage) {
		this.shuffleStage = shuffleStage;
	}

	public void addRelStage(BatchExecRelStage stage) {
		this.stages.add(stage);
		this.relSet.add(stage.getBatchExecRel());
	}

	public List<BatchExecRelStage> getRelStages() {
		return stages;
	}

	public Set<RowBatchExecRel> getRelSet() {
		return relSet;
	}

}
