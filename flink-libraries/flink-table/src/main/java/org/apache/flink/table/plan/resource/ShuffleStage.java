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

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Forward rel cross runningUnit. There are no shuffle when transferring data in a relForward.
 */
public class ShuffleStage {

	private final Set<ShuffleStageInRunningUnit> shuffleStageInRUSet = new LinkedHashSet<>();
	private final Set<RowBatchExecRel> batchExecRelSet = new LinkedHashSet<>();
	private int resultParallelism = -1;
	private boolean hasFixedParallelism = false;

	public void addShuffleStageInRus(Set<ShuffleStageInRunningUnit> shuffleStageInRUs) {
		this.shuffleStageInRUSet.addAll(shuffleStageInRUs);
		for (ShuffleStageInRunningUnit shuffleStageInRU: shuffleStageInRUs) {
			batchExecRelSet.addAll(shuffleStageInRU.getRelSet());
		}
	}

	public Set<RowBatchExecRel> getBatchExecRelSet() {
		return this.batchExecRelSet;
	}

	public int getResultParallelism() {
		return resultParallelism;
	}

	public void setResultParallelism(int resultParallelism, boolean fixedParallelism) {
		if (this.hasFixedParallelism) {
			if (fixedParallelism && this.resultParallelism != resultParallelism) {
				throw new IllegalArgumentException("both fixed parallelism are not equal, old: " + this.resultParallelism + ", new: " + resultParallelism);
			}
		} else {
			if (fixedParallelism) {
				this.resultParallelism = resultParallelism;
				this.hasFixedParallelism = true;
			} else {
				this.resultParallelism = Math.max(this.resultParallelism, resultParallelism);
			}
		}
	}

	public boolean isParallelismFixed() {
		return hasFixedParallelism;
	}

	public Set<ShuffleStageInRunningUnit> getShuffleStageInRUSet() {
		return this.shuffleStageInRUSet;
	}

}
