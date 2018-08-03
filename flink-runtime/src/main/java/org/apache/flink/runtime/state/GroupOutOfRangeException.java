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

package org.apache.flink.runtime.state;

/**
 * Exceptions thrown when a state attempts to access a group not assigned to
 * the backend.
 */
public class GroupOutOfRangeException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	/**
	 * Constructor with the name of the state, the groups assigned to the
	 * backend and the group to be accessed.
	 *
	 * @param stateName The name of the state.
	 * @param assignedGroups The group assigned to the backend.
	 * @param group The group to be accessed.
	 */
	public GroupOutOfRangeException(
		String stateName,
		GroupSet assignedGroups,
		int group
	) {
		super("The state " + stateName + " cannot access the values in " +
			"group " + group + " because the group is not " +
			"assigned to the backend. The assigned groups are " + assignedGroups);
	}
}
