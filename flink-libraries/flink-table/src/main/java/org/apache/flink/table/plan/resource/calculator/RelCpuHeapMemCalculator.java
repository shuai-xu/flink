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

package org.apache.flink.table.plan.resource.calculator;

import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.table.api.BatchTableEnvironment;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecExchange;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecReused;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecScan;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecUnion;
import org.apache.flink.table.plan.nodes.physical.batch.RowBatchExecRel;
import org.apache.flink.table.plan.resource.RelResource;
import org.apache.flink.table.util.BatchExecResourceUtil;

import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Cpu and heap memory calculator for relNode.
 */
public class RelCpuHeapMemCalculator {

	private final Map<RowBatchExecRel, RelResource> relResMap;
	private final TableConfig tConfig;
	private final BatchTableEnvironment tableEnv;
	private final Set<RowBatchExecRel> calculatedRelSet = new HashSet<>();

	private RelCpuHeapMemCalculator(BatchTableEnvironment tableEnv,
			Map<RowBatchExecRel, RelResource> relResMap) {
		this.relResMap = relResMap;
		this.tConfig = tableEnv.getConfig();
		this.tableEnv = tableEnv;
	}

	public static void calculate(BatchTableEnvironment tableEnv,
			Map<RowBatchExecRel, RelResource> relResMap,
			RowBatchExecRel rowBatchExecRel) {
		new RelCpuHeapMemCalculator(tableEnv, relResMap).calculate(rowBatchExecRel);
	}

	private void calculate(RowBatchExecRel rowBatchExecRel) {
		if (!calculatedRelSet.add(rowBatchExecRel)) {
			return;
		}
		if (rowBatchExecRel instanceof BatchExecScan) {
			calculateSource((BatchExecScan) rowBatchExecRel);
		} else if (rowBatchExecRel instanceof BatchExecUnion || rowBatchExecRel instanceof BatchExecReused) {
			calculateInputs(rowBatchExecRel);
		} else if (rowBatchExecRel instanceof BatchExecExchange) {
			calculateExchange((BatchExecExchange) rowBatchExecRel);
		} else {
			calculateDefaultRel(rowBatchExecRel);
		}
	}

	private void calculateSource(BatchExecScan scanBatchExec) {
		// user may have set resource for source transformation.
		RelResource relResource = new RelResource();
		ResourceSpec sourceRes = scanBatchExec.getTableSourceResource(this.tableEnv);
		if (sourceRes == ResourceSpec.DEFAULT || sourceRes == null) {
			int heap = BatchExecResourceUtil.getSourceMem(tConfig);
			sourceRes = BatchExecResourceUtil.getResourceSpec(tConfig, heap);
		}
		ResourceSpec conversionRes = ResourceSpec.DEFAULT;
		if (scanBatchExec.needInternalConversion()) {
			conversionRes = BatchExecResourceUtil.getDefaultResourceSpec(tConfig);
		}
		ResourceSpec totalRes = sourceRes.merge(conversionRes);
		relResource.setCpu(totalRes.getCpuCores());
		relResource.setHeapMem(totalRes.getHeapMemory());
		relResMap.put(scanBatchExec, relResource);
		scanBatchExec.setResForSourceAndConversion(sourceRes, conversionRes);
	}

	private void calculateDefaultRel(RowBatchExecRel rel) {
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
		double cpu = BatchExecResourceUtil.getCpu(tConfig);
		int heap = BatchExecResourceUtil.getDefaultHeapMem(tConfig);
		RelResource relResource = new RelResource();
		relResource.setCpu(cpu);
		relResource.setHeapMem(heap);
		return relResource;
	}

	private void calculateInputs(RelNode relNode) {
		relNode.getInputs().forEach(i -> calculate((RowBatchExecRel) i));
	}
}
