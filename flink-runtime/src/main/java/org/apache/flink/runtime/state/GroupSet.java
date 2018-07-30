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

import java.io.Serializable;

/**
 * A set comprising zero or more groups.
 */
public interface GroupSet extends Iterable<Integer>, Serializable {

	/**
	 * Returns true if the set does not contain any group.
	 *
	 * @return True if the set does not contain any group.
	 */
	boolean isEmpty();

	/**
	 * Returns the number of groups in the set.
	 *
	 * @return The number of groups in the set.
	 */
	int getNumGroups();

	/**
	 * Returns true if the given group is contained in the set.
	 *
	 * @param group The group whose presence is to be tested.
	 * @return True if the given group is contained in the set.
	 */
	boolean contains(int group);

	/**
	 * Returns the intersection between this set and the given set.
	 *
	 * @param other The set to be intersected with.
	 * @return The intersection between this set and the given set.
	 */
	GroupSet intersect(GroupSet other);
}

