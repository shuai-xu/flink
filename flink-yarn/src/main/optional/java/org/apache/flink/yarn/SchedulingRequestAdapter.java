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

package org.apache.flink.yarn;

import org.apache.flink.api.common.operators.ResourceConstraints;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;

import java.util.Arrays;
import java.util.Map;

public class SchedulingRequestAdapter extends RequestAdapter {

	public SchedulingRequestAdapter(AMRMClientAsync<AMRMClient.ContainerRequest> rmClient) {
		super(rmClient);
	}

	@Override
	public void addRequest(Resource resource, Priority priority, int pendingNum, ResourceConstraints constraints) {
		updateSchedulingRequest(resource, priority, pendingNum, constraints);
	}

	@Override
	public void removeRequest(Resource resource, Priority priority, int pendingNum, ResourceConstraints constraints) {
		updateSchedulingRequest(resource, priority, pendingNum, constraints);
	}

	@Override
	public void updateExtendedResources(Resource resource,
		Map<String, org.apache.flink.api.common.resources.Resource> extendedResources) {
		// support setting extended resource
		if (extendedResources != null && !extendedResources.isEmpty()) {
			extendedResources.values().forEach(er -> resource.setResourceValue(er.getName(), (long) er.getValue()));
		}
	}

	private void updateSchedulingRequest(Resource resource, Priority priority, int pendingNum,
										 ResourceConstraints constraints) {
		SchedulingRequest schedulingRequest = YarnSchedulingRequestBuilder.build(resource,
			constraints, priority, pendingNum);
		rmClient.addSchedulingRequests(Arrays.asList(schedulingRequest));
	}
}
