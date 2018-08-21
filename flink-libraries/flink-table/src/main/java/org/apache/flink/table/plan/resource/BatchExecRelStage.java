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

import org.apache.flink.streaming.api.transformations.StreamTransformation;
import org.apache.flink.table.plan.nodes.physical.batch.RowBatchExecRel;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Describe which stage of a batchExecRel. Many batchExecRel run with several stages, some of what
 * begin after other finish.
 * e.g. BatchExecRel: HashJoin has two stages, when probe begins after build ends.
 * And output begins after input ends for hashAgg.
 */
public class BatchExecRelStage implements Serializable {

	private final transient RowBatchExecRel batchExecRel;
	private int stageID;
	private final List<BatchExecRelStage> dependStageList = new ArrayList<>();
	private final List<Integer> transformationIDList = new LinkedList<>();
	private final String relName;

	public void setRelID(Integer relID) {
		this.relID = relID;
	}

	private Integer relID;

	public BatchExecRelStage(RowBatchExecRel batchExecRel, int stageID) {
		this.batchExecRel = batchExecRel;
		this.stageID = stageID;
		this.relName = batchExecRel.toString();
	}

	public void addTransformation(StreamTransformation<?> transformation) {
		transformationIDList.add(transformation.getId());
	}

	public List<Integer> getTransformationIDList() {
		return transformationIDList;
	}

	public Integer getRelID() {
		return relID;
	}

	public void addDependStage(BatchExecRelStage stage) {
		Preconditions.checkArgument(stage.getBatchExecRel() == batchExecRel);
		this.dependStageList.add(stage);
	}

	// to avoid schedule deadlock.
	public void removeDependStage(BatchExecRelStage stage) {
		this.dependStageList.remove(stage);
	}

	public RowBatchExecRel getBatchExecRel() {
		return batchExecRel;
	}

	public int getStageID() {
		return stageID;
	}

	public List<BatchExecRelStage> getDependStageList() {
		return dependStageList;
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
		return "relStage（" +
				"batchExecRel=" + relName +
				", stageID=" + stageID +
				", dependStageIDList=" + dependStageList +
				')';
	}
}
