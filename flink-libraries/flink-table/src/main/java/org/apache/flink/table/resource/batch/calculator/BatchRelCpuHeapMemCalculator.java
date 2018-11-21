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

package org.apache.flink.table.resource.batch.calculator;

import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecExchange;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecRel;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecScan;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecUnion;
import org.apache.flink.table.resource.RelResource;
import org.apache.flink.table.resource.ResourceCalculator;
import org.apache.flink.table.util.ExecResourceUtil;

import org.apache.calcite.rel.RelDistribution;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Cpu and heap memory calculator for relNode.
 */
public class BatchRelCpuHeapMemCalculator extends ResourceCalculator<BatchExecRel<?>> {

	private Map<BatchExecRel<?>, RelResource> relResMap;
	private final Set<BatchExecRel<?>> calculatedRelSet = new HashSet<>();

	public BatchRelCpuHeapMemCalculator(TableEnvironment tEnv) {
		super(tEnv);
	}

	public void setRelResourceMap(Map<BatchExecRel<?>, RelResource> relResMap) {
		this.relResMap = relResMap;
	}

	public void calculate(BatchExecRel<?> batchExecRel) {
		if (!calculatedRelSet.add(batchExecRel)) {
			return;
		}
		if (batchExecRel instanceof BatchExecScan) {
			calculateSource((BatchExecScan) batchExecRel);
		} else if (batchExecRel instanceof BatchExecUnion) {
			calculateInputs(batchExecRel);
		} else if (batchExecRel instanceof BatchExecExchange) {
			calculateExchange((BatchExecExchange) batchExecRel);
		} else {
			calculateDefaultRel(batchExecRel);
		}
	}

	private void calculateSource(BatchExecScan scanBatchExec) {
		// user may have set resource for source transformation.
		RelResource relResource = new RelResource();
		ResourceSpec sourceRes = scanBatchExec.getTableSourceResource(this.tEnv);
		if (sourceRes == ResourceSpec.DEFAULT || sourceRes == null) {
			int heap = ExecResourceUtil.getSourceMem(tConfig);
			sourceRes = ExecResourceUtil.getResourceSpec(tConfig, heap);
		}
		ResourceSpec conversionRes = ResourceSpec.DEFAULT;
		if (scanBatchExec.needInternalConversion()) {
			conversionRes = ExecResourceUtil.getDefaultResourceSpec(tConfig);
		}
		ResourceSpec totalRes = sourceRes.merge(conversionRes);
		relResource.setCpu(totalRes.getCpuCores());
		relResource.setHeapMem(totalRes.getHeapMemory());
		relResMap.put(scanBatchExec, relResource);
		scanBatchExec.setResForSourceAndConversion(sourceRes, conversionRes);
	}

	private void calculateDefaultRel(BatchExecRel<?> rel) {
		calculateInputs(rel);
		RelResource relResource = getDefaultRelResource();
		relResMap.put(rel, relResource);
	}

	// set resource for rangePartition exchange
	private void calculateExchange(BatchExecExchange execExchange) {
		calculateInputs(execExchange);
		if (execExchange.getDistribution().getType() == RelDistribution.Type.RANGE_DISTRIBUTED) {
			RelResource resource = getDefaultRelResource();
			execExchange.setResource(resource);
		}
	}

	private RelResource getDefaultRelResource() {
		double cpu = ExecResourceUtil.getDefaultCpu(tConfig);
		int heap = ExecResourceUtil.getDefaultHeapMem(tConfig);
		RelResource relResource = new RelResource();
		relResource.setCpu(cpu);
		relResource.setHeapMem(heap);
		return relResource;
	}
}
