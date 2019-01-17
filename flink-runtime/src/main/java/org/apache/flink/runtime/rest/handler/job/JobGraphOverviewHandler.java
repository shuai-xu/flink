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

package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.ResourceSpecInfo;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobGraphOverviewInfo;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * JobGraph config.
 */
public class JobGraphOverviewHandler extends AbstractRestHandler<RestfulGateway, EmptyRequestBody, JobGraphOverviewInfo, JobMessageParameters> {

	public JobGraphOverviewHandler(
			CompletableFuture<String> localRestAddress,
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Time timeout,
			Map<String, String> responseHeaders,
			MessageHeaders<EmptyRequestBody, JobGraphOverviewInfo, JobMessageParameters> messageHeaders) {
		super(
			localRestAddress,
			leaderRetriever,
			timeout,
			responseHeaders,
			messageHeaders);
	}

	@Override
	protected CompletableFuture<JobGraphOverviewInfo> handleRequest(@Nonnull HandlerRequest<EmptyRequestBody, JobMessageParameters> request, @Nonnull RestfulGateway gateway) throws RestHandlerException {
		JobID jobID = request.getPathParameter(JobIDPathParameter.class);
		return gateway.requestJobGraph(jobID, timeout).thenApply(
			(JobGraph jobGraph) -> {
				Configuration config = jobGraph.getJobConfiguration();
				Map<String, JobGraphOverviewInfo.VertexConfigInfo> vertexConfigs = new HashMap<>();
				Map<String, List<String>> inputNodes = new HashMap<>();
				for (JobVertex vertex : jobGraph.getVertices()) {
					List<Integer> nodeIds;
					JobVertexID vertexID = vertex.getID();
					if (vertex.getOperatorDescriptors() != null) {
						nodeIds = vertex.getOperatorDescriptors().stream().map(op -> op.getNodeId()).collect(Collectors.toList());
					} else {
						nodeIds = new ArrayList<>();
					}
					List<String> inputVertexId;
					if (vertex.getInputs() != null) {
						inputVertexId = vertex.getInputs().stream().map(edge -> edge.getSource().getProducer().getID().toString()).collect(Collectors.toList());
					} else {
						inputVertexId = new ArrayList<>();
					}
					ResourceSpec resourceSpec = vertex.getMinResources();
					ResourceSpecInfo resourceSpecInfo = new ResourceSpecInfo(
						resourceSpec.getCpuCores(),
						resourceSpec.getHeapMemory(),
						resourceSpec.getDirectMemory(),
						resourceSpec.getNativeMemory(),
						resourceSpec.getStateSize(),
						resourceSpec.getExtendedResources()
					);
					JobGraphOverviewInfo.VertexConfigInfo vertexConfigInfo = new JobGraphOverviewInfo.VertexConfigInfo(vertexID, vertex.getName(),
						vertex.getParallelism(), vertex.getMaxParallelism(), resourceSpecInfo, nodeIds
						);
					vertexConfigs.put(vertexID.toString(), vertexConfigInfo);
					inputNodes.put(vertexID.toString(), inputVertexId);
				}
				return new JobGraphOverviewInfo(config, vertexConfigs, inputNodes);
			}
		);
	}

}
