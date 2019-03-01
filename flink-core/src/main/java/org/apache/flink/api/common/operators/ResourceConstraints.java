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

import java.io.Serializable;
import java.util.Map;

/**
 * Operator resource constraints interface.
 */
public interface ResourceConstraints extends Serializable {

	Map<String, String> getConstraints();

	boolean isCompatibleWith(ResourceConstraints other);

	ResourceConstraints merge(ResourceConstraints other);

	ResourceConstraints addConstraint(String key, String val);

	/**
	 * Users may pass the same ResourceConstraints to multi transformations, so we need a clone function
	 * to create a new ResourceConstraints instance when ResourceConstraints is added, which guarantees
	 * that when the ResourceConstraints of one transformation is modified, other transformations are not
	 * affected.
	 * @return A new ResourceConstraints instance with same resource constraints.
	 */
	ResourceConstraints clone();

	@Override
	boolean equals(Object other);
}
