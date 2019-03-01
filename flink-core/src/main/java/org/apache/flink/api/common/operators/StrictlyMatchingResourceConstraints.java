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

import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;

/**
 * An implementation of {@link ResourceConstraints} which matches two ResourceConstraints strictly.
 */
public class StrictlyMatchingResourceConstraints implements ResourceConstraints {
	private final Map<String, String> constraints = new HashMap<>();

	public Map<String, String> getConstraints() {
		return constraints;
	}

	public boolean isCompatibleWith(ResourceConstraints other) {
		return this.equals(other);
	}

	public ResourceConstraints merge(ResourceConstraints other) {
		return this;
	}

	@Override
	public ResourceConstraints addConstraint(String key, String val) {
		Preconditions.checkNotNull(key);
		Preconditions.checkNotNull(val);
		constraints.put(key, val);
		return this;
	}

	private ResourceConstraints addConstraints(Map<String, String> toAdd) {
		constraints.putAll(toAdd);
		return this;
	}

	@Override
	public ResourceConstraints clone() {
		StrictlyMatchingResourceConstraints constraints = new StrictlyMatchingResourceConstraints();
		constraints.addConstraints(this.constraints);
		return constraints;
	}

	/**
	 * Whether the two resource constraints are equal with each other.
	 * @param other another resource constraints.
	 * @return whether the two resource constraints are equal with each other. The
	 * StrictlyMatchingResourceConstraints return false if one is with none constraint
	 * and the other is not.
	 */
	public boolean equals(Object other) {
		if (this == other) {
			return true;
		}
		if (!(other instanceof ResourceConstraints)) {
			return false;
		}
		return constraints.equals(((ResourceConstraints) other).getConstraints());
	}

	@Override
	public int hashCode() {
		return constraints.hashCode();
	}

	@Override
	public String toString() {
		final StringBuilder constraintsStr = new StringBuilder(constraints.size() * 30);
		constraintsStr.append("[");
		for (Map.Entry<String, String> constraint : constraints.entrySet()) {
			constraintsStr.append(constraint.getKey()).append('=').append(constraint.getValue()).append(", ");
		}
		if (constraintsStr.length() >= 3) {
			constraintsStr.replace(constraintsStr.length() - 2, constraintsStr.length(), "]");
		} else {
			constraintsStr.append("]");
		}
		return constraintsStr.toString();
	}
}
