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

package org.apache.flink.api.common.operators;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ResourceConstraintsOptions;
import org.apache.flink.util.Preconditions;

/**
 * Utility class to generate default ResourceConstraints form flink configuration.
 */
public class ResourceConstraintsConfig {
	private final ResourceConstraints defaultConstraints;

	public ResourceConstraintsConfig(Configuration flinkConf) {
		Preconditions.checkNotNull(flinkConf);
		ResourceConstraints constraints = new StrictlyMatchingResourceConstraints();
		for (String key : flinkConf.keySet()) {
			if (key.startsWith(ResourceConstraintsOptions.BLINK_RESOURCE_CONSTRAINT_PREFIX)) {
				constraints.addConstraint(key, flinkConf.getString(key, null));
			}
		}
		if (constraints.getConstraints().size() > 0) {
			defaultConstraints = constraints;
		} else {
			defaultConstraints = null;
		}
	}

	public ResourceConstraints getDefaultResourceConstraints() {
		return defaultConstraints;
	}
}
