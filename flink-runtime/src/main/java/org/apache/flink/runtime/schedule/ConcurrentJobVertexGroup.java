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

package org.apache.flink.runtime.schedule;

import org.apache.flink.runtime.jobgraph.ControlType;
import org.apache.flink.runtime.jobgraph.JobControlEdge;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * ConcurrentJobVertexGroup contains the JobVertex that should be scheduled at the same time.
 */
public class ConcurrentJobVertexGroup {

	private List<JobVertex> vertices = new ArrayList<>();

	private boolean hasPrecedingGroup = false;

	private boolean hasInputVertex = false;

	private List<JobVertex> predecessorVertices = new ArrayList<>();

	public ConcurrentJobVertexGroup(Collection<JobVertex> jobVertices, Set<JobControlEdge> ignoredControlEdges) {
		this.vertices.addAll(sortJobVertexTopologically(jobVertices, ignoredControlEdges));
	}

	public List<JobVertex> getVertices() {
		return vertices;
	}

	public List<JobVertex> getPredecessorVertices() {
		return this.predecessorVertices;
	}

	public boolean hasPrecedingGroup() {
		return hasPrecedingGroup;
	}

	public void noPrecedingGroup() {
		this.hasPrecedingGroup = false;
	}

	public  boolean hasInputVertex() {
		return this.hasInputVertex;
	}

	private List<JobVertex> sortJobVertexTopologically(Collection<JobVertex> jobVertices, Set<JobControlEdge> ignoredControlEdges) {
		List<JobVertex> jobVerticesTopologically = new ArrayList<>(jobVertices.size());

		Set<JobVertex> remaining = new LinkedHashSet<>(jobVertices);

		while (!remaining.isEmpty()) {
			Iterator<JobVertex> iter = remaining.iterator();

			int preSize = remaining.size();

			while (iter.hasNext()) {
				JobVertex jobVertex = iter.next();

				if (jobVertex.isInputVertex()) {
					hasInputVertex = true;
				}

				boolean allPredecessorAdded = true;
				boolean hasPredecessorInThisGroup = false;
				for (JobEdge jobEdge : jobVertex.getInputs()) {
					if (jobVertices.contains(jobEdge.getSource().getProducer())) {
						hasPredecessorInThisGroup = true;
					}
					if (remaining.contains(jobEdge.getSource().getProducer())) {
						allPredecessorAdded = false;
						break;
					}
				}
				for (JobControlEdge controlEdge : jobVertex.getInControlEdges()) {
					if (controlEdge.getControlType() == ControlType.START_ON_FINISH &&
							!jobVertices.contains(controlEdge.getSource()) &&
							!ignoredControlEdges.contains(controlEdge)) {
						hasPrecedingGroup = true;
						predecessorVertices.add(controlEdge.getSource());
					}
				}
				if (!hasPredecessorInThisGroup && jobVertex.getNumberOfInputs() > 0) {
					hasPrecedingGroup = true;
					for (JobEdge jobEdge : jobVertex.getInputs()) {
						predecessorVertices.add(jobEdge.getSource().getProducer());
					}
				}
				if (allPredecessorAdded) {
					jobVerticesTopologically.add(jobVertex);
					iter.remove();
				}
			}

			if (preSize == remaining.size()) {
				throw new FlinkRuntimeException("There are circles among the groups.");
			}
		}
		return jobVerticesTopologically;
	}
}
