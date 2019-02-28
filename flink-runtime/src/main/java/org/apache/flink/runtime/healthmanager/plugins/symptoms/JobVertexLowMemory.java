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

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Symptom indicating job vertices memory low.
 */
public class JobVertexLowMemory implements Symptom {

	private JobID jobID;
	Map<JobVertexID, Double> heapUtilities;
	Map<JobVertexID, Double> nonHeapUtilities;
	Map<JobVertexID, Double> nativeUtilities;

	public JobVertexLowMemory(JobID jobID) {
		this.jobID = jobID;
		heapUtilities = new HashMap<>();
		nonHeapUtilities = new HashMap<>();
		nativeUtilities = new HashMap<>();
	}

	public Map<JobVertexID, Double> getHeapUtilities() {
		return heapUtilities;
	}

	public Map<JobVertexID, Double> getNonHeapUtilities() {
		return nonHeapUtilities;
	}

	public Map<JobVertexID, Double> getNativeUtilities() {
		return nativeUtilities;
	}

	public boolean isEmpty() {
		return nativeUtilities.isEmpty();
	}

	public void addVertex(JobVertexID vertexID, double heapUtility, double nonHeapUtility, double nativeUtility) {
		heapUtilities.put(vertexID, heapUtility);
		nonHeapUtilities.put(vertexID, nonHeapUtility);
		nativeUtilities.put(vertexID, nativeUtility);
	}

	@Override
	public String toString() {
		String vertices = heapUtilities.keySet().stream().map(vertexId -> "{JobVertexID:" + vertexId + ", "
			+ "heapUtility: " + heapUtilities.get(vertexId) + ", "
			+ "nonHeapUtility: " + nonHeapUtilities.get(vertexId) + ", "
			+ "nativeUtility: " + nativeUtilities.get(vertexId) + "}").collect(
			Collectors.joining(", "));
		return "JobVertexLowMemory{" + vertices + "}";
	}
}
