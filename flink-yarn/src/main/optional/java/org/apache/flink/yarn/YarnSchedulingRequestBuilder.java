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
import org.apache.flink.configuration.ResourceConstraintsOptions;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceSizing;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint;
import org.apache.hadoop.yarn.util.constraint.PlacementConstraintParseException;
import org.apache.hadoop.yarn.util.constraint.PlacementConstraintParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

 /**
  * Utility class for building yarn SchedulingRequest.
  */
public class YarnSchedulingRequestBuilder {
	protected static final Logger LOG = LoggerFactory.getLogger(YarnSchedulingRequestBuilder.class);

	public static SchedulingRequest build(Resource resource, ResourceConstraints resourceConstraints,
										Priority priority, int requestNum) {
		Preconditions.checkNotNull(resource, "resource should not be null.");
		Preconditions.checkArgument(requestNum >= 0, "requestNum should not be negative.");

		SchedulingRequest.SchedulingRequestBuilder builder = SchedulingRequest.newBuilder();
		builder.priority(priority);
		if (resourceConstraints != null) {
			Map<String, String> constraints = resourceConstraints.getConstraints();

			String sourceTags = constraints.get(ResourceConstraintsOptions.YARN_CONTAINER_TAGS);
			if (sourceTags != null) {
				String[] tagList = sourceTags.split(",");
				Set<String> tags = new HashSet<>();
				for (String tag : tagList) {
					if (!"".equals(tag.trim())) {
						tags.add(tag.trim());
					}
				}
				builder.allocationTags(tags);
			}

			String placementConstraintSpec = constraints.get(ResourceConstraintsOptions.YARN_PLACEMENT_CONSTRAINTS);
			if (placementConstraintSpec != null) {
				PlacementConstraint placementConstraint = null;
				try {
					placementConstraint = PlacementConstraintParser.parseExpression(placementConstraintSpec).build();
					builder.placementConstraintExpression(placementConstraint);
				} catch (PlacementConstraintParseException e) {
					LOG.error("An error occurred while parsing placement constraint: " + placementConstraintSpec, e.toString());
				}
			}

			String executionTypeStr = constraints.get(ResourceConstraintsOptions.YARN_EXECUTION_TYPE);
			if (executionTypeStr != null) {
				try {
					ExecutionType executionType = ExecutionType.valueOf(executionTypeStr);
					builder.executionType(ExecutionTypeRequest.newInstance(executionType));
				} catch (IllegalArgumentException e) {
					LOG.error("Container execution type parse error, execution type should be either " +
						ResourceConstraintsOptions.YARN_EXECUTION_TYPE_GUARANTEED + " or " +
						ResourceConstraintsOptions.YARN_EXECUTION_TYPE_OPPORTUNISTIC, e.toString());
				}
			}
		}

		builder.resourceSizing(ResourceSizing.newInstance(requestNum, resource))
			.nodeLabelExpression(null);

		return builder.build();
	}
}
