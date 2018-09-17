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

package org.apache.flink.table.plan.resource;

import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.resources.CommonExtendedResource;
import org.apache.flink.api.common.resources.Resource;

/**
 * Resource for relNode: cpu, heap memory, reserved managed memory, prefer managed memory and
 * max managed memory.
 * Reserved managed memory: needed when an operator init.
 * Prefer managed memory: tell the scheduler how much managed memory an operator may use.
 * Max managed memory: max managed memory that an operator can use.
 */
public class RelResource {

	private double cpu;

	private int heapMem;

	// whether reserved managed memory will change.
	private boolean isReservedManagedFinal;

	// reserved managed mem for rel.
	private int reservedManagedMem;

	// prefer managed mem for rel.
	private int preferManagedMem;

	// max managed mem for rel.
	private int maxManagedMem;

	public void setCpu(double cpu) {
		this.cpu = cpu;
	}

	public double getCpu() {
		return cpu;
	}

	public void setHeapMem(int heapMem) {
		this.heapMem = heapMem;
	}

	public int getReservedManagedMem() {
		return reservedManagedMem;
	}

	public void setManagedMem(int reservedManagedMem, int preferManagedMem, int maxManagedMem, boolean isReservedManagedFinal) {
		this.reservedManagedMem = reservedManagedMem;
		this.preferManagedMem = preferManagedMem;
		this.maxManagedMem = maxManagedMem;
		this.isReservedManagedFinal = isReservedManagedFinal;
	}

	public void setManagedMem(int reservedManagedMem, int preferManagedMem, int maxManagedMem) {
		setManagedMem(reservedManagedMem, preferManagedMem, maxManagedMem, false);
	}

	public int getPreferManagedMem() {
		return preferManagedMem;
	}

	public int getHeapMem() {
		return heapMem;
	}

	public boolean isReservedManagedFinal() {
		return isReservedManagedFinal;
	}

	public int getMaxManagedMem() {
		return maxManagedMem;
	}

	public ResourceSpec getReservedResourceSpec() {
		ResourceSpec.Builder builder = ResourceSpec.newBuilder();
		builder.setCpuCores(cpu);
		builder.setHeapMemoryInMB(heapMem);
		builder.addExtendedResource(new CommonExtendedResource(
				ResourceSpec.MANAGED_MEMORY_NAME,
				getReservedManagedMem(),
				Resource.ResourceAggregateType.AGGREGATE_TYPE_SUM));
		builder.addExtendedResource(new CommonExtendedResource(
				ResourceSpec.FLOATING_MANAGED_MEMORY_NAME,
				getPreferManagedMem() - getReservedManagedMem(),
				Resource.ResourceAggregateType.AGGREGATE_TYPE_SUM));
		return builder.build();
	}

	public ResourceSpec getPreferResourceSpec() {
		ResourceSpec.Builder builder = ResourceSpec.newBuilder();
		builder.setCpuCores(cpu);
		builder.setHeapMemoryInMB(heapMem);
		builder.addExtendedResource(new CommonExtendedResource(
				ResourceSpec.MANAGED_MEMORY_NAME,
				getPreferManagedMem(),
				Resource.ResourceAggregateType.AGGREGATE_TYPE_SUM));
		return builder.build();
	}

	@Override
	public String toString() {
		return "RelResource{" +
				"cpu=" + cpu +
				", heapMem=" + heapMem +
				", reservedManagedMem=" + reservedManagedMem +
				", preferManagedMem=" + preferManagedMem +
				", maxManagedMem=" + maxManagedMem +
				'}';
	}
}
