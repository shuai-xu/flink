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

import java.util.HashMap;

/**
 * Adjust resource config for given vertex.
 */
public class AdjustJobResource extends AdjustJobConfig {
	public AdjustJobResource(JobID jobID, long timeoutMs) {
		super(jobID, timeoutMs);
	}

	public AdjustJobResource(AdjustJobResource anotherAdjustJobResource) {
		super(anotherAdjustJobResource.jobID,
			anotherAdjustJobResource.timeoutMs,
			new HashMap<>(anotherAdjustJobResource.currentParallelism),
			new HashMap<>(anotherAdjustJobResource.targetParallelism),
			new HashMap<>(anotherAdjustJobResource.currentResource),
			new HashMap<>(anotherAdjustJobResource.targetResource),
			anotherAdjustJobResource.actionMode);
	}

	public AdjustJobResource merge(AdjustJobResource anotherAction) {
		throw new UnsupportedOperationException();
	}
}
