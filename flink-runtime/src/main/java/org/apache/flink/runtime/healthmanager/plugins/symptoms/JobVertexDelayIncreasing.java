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

package org.apache.flink.runtime.healthmanager.plugins.symptoms;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.healthmanager.plugins.Symptom;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.List;

/**
 * Symptom indicating job vertices delay increasing.
 */
public class JobVertexDelayIncreasing implements Symptom {

	private JobID jobID;
	private List<JobVertexID> jobVertexIDs;

	public JobVertexDelayIncreasing(JobID jobID, List<JobVertexID> jobVertexIDs) {
		this.jobID = jobID;
		this.jobVertexIDs = jobVertexIDs;
	}

	public JobID getJobID() {
		return jobID;
	}

	public List<JobVertexID> getJobVertexIDs() {
		return jobVertexIDs;
	}
}
