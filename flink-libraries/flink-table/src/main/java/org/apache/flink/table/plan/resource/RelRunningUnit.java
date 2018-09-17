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
 * RelRunningUnit contains some batchExecRelStages that run at the same time. It can be considered
 * as schedule unit.
 */
public class RelRunningUnit implements Serializable {

	private final List<BatchExecRelStage> relStageList = new LinkedList<>();
	private transient Set<RowBatchExecRel> relSet = new LinkedHashSet<>();

	public void addRelStage(BatchExecRelStage relStage) {
		this.relStageList.add(relStage);
		this.relSet.add(relStage.getBatchExecRel());
	}

	public Set<RowBatchExecRel> getRelSet() {
		return relSet;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("RelRunningUnit{");
		for (BatchExecRelStage relStage : relStageList) {
			sb.append("\n\t").append(relStage);
		}
		sb.append("\n}");
		return sb.toString();
	}

	public List<BatchExecRelStage> getAllRelStages() {
		return relStageList;
	}
}
