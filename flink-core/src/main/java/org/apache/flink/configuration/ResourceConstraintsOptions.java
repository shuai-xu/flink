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

package org.apache.flink.configuration;

/**
 * The set of configuration options relating to resource constraints.
 */
public class ResourceConstraintsOptions {
	public static final String BLINK_RESOURCE_CONSTRAINT_PREFIX = "blink.resource.constraint.";

	/** GUARANTEED or OPPORTUNISTIC.*/
	public static final String YARN_EXECUTION_TYPE = BLINK_RESOURCE_CONSTRAINT_PREFIX + "yarn.container.execution.type";
	/** comma separated strings.*/
	public static final String YARN_CONTAINER_TAGS = BLINK_RESOURCE_CONSTRAINT_PREFIX + "yarn.container.tags";
	public static final String YARN_PLACEMENT_CONSTRAINTS = BLINK_RESOURCE_CONSTRAINT_PREFIX + "yarn.container.placement.constraints";
	public static final String YARN_CONTAINER_PRIORITY = BLINK_RESOURCE_CONSTRAINT_PREFIX + "yarn.container.priority";

	public static final String YARN_EXECUTION_TYPE_GUARANTEED = "GUARANTEED";
	public static final String YARN_EXECUTION_TYPE_OPPORTUNISTIC = "OPPORTUNISTIC";
}
