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

package org.apache.flink.runtime.healthmanager.plugins.actions;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.runtime.jobgraph.JobVertexID;

/**
 * Adjust cpu for given vertex.
 */
public class AdjustJobCpu extends AdjustJobResource {
	public AdjustJobCpu(JobID jobID, long timeoutMs) {
		super(jobID, timeoutMs);
	}

	@Override
	public AdjustJobResource merge(AdjustJobResource anotherAction) {
		if (!this.jobID.equals(anotherAction.jobID)) {
			return null;
		}

		AdjustJobResource mergedAction = new AdjustJobResource(anotherAction);
		mergedAction.timeoutMs = Math.max(mergedAction.timeoutMs, this.timeoutMs);
		if (actionMode == ActionMode.IMMEDIATE) {
			mergedAction.actionMode = ActionMode.IMMEDIATE;
		}

		for (JobVertexID vertexId : targetResource.keySet()) {
			if (mergedAction.targetResource.containsKey(vertexId)) {
				mergedAction.targetResource.put(vertexId,
					new ResourceSpec.Builder(mergedAction.targetResource.get(vertexId))
						.setCpuCores(this.targetResource.get(vertexId).getCpuCores()).build());
			} else {
				mergedAction.currentResource.put(vertexId, this.currentResource.get(vertexId));
				mergedAction.targetResource.put(vertexId, this.targetResource.get(vertexId));
				mergedAction.currentParallelism.put(vertexId, this.currentParallelism.get(vertexId));
				mergedAction.targetParallelism.put(vertexId, this.targetParallelism.get(vertexId));
			}
		}

		return mergedAction;
	}
}
