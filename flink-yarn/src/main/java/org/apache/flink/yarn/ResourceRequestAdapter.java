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
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;

import java.util.Map;

/**
 * Request adapter implement to communicate with YARN RM which support resource request.
 */
public class ResourceRequestAdapter extends RequestAdapter {

	public ResourceRequestAdapter(AMRMClientAsync<AMRMClient.ContainerRequest> rmClient) {
		super(rmClient);
	}

	@Override
	public void addRequest(Resource resource, Priority priority, int pendingNum, ResourceConstraints constraints) {
		AMRMClient.ContainerRequest containerRequest =
			new AMRMClient.ContainerRequest(resource, null, null, priority);
		rmClient.addContainerRequest(containerRequest);
	}

	@Override
	public void removeRequest(Resource resource, Priority priority, int pendingNum, ResourceConstraints constraints) {
		AMRMClient.ContainerRequest containerRequest =
			new AMRMClient.ContainerRequest(resource, null, null, priority);
		rmClient.removeContainerRequest(containerRequest);
	}

	@Override
	public void updateExtendedResources(Resource resource,
		Map<String, org.apache.flink.api.common.resources.Resource> extendedResources) {
		// do nothing for resource request
	}
}
