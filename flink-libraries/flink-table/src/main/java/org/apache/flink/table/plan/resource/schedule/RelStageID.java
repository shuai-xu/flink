/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.plan.resource.schedule;

import java.io.Serializable;

/**
 * Identification of a stage of rel.
 */
public class RelStageID implements Serializable {

	/**
	 * ID of the rel.
	 */
	private int relID;

	/**
	 * ID of a stage in the rel.
	 */
	private int stageID;

	public RelStageID(int relID, int stageID) {
		this.relID = relID;
		this.stageID = stageID;
	}

	public int getStageID() {
		return stageID;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		RelStageID that = (RelStageID) o;

		if (relID != that.relID) {
			return false;
		}
		return stageID == that.stageID;
	}

	@Override
	public int hashCode() {
		int result = relID;
		result = 31 * result + stageID;
		return result;
	}
}
