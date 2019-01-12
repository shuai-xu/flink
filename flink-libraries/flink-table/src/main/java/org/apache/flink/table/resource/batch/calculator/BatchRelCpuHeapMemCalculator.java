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
import org.apache.flink.streaming.api.transformations.StreamTransformation;
import org.apache.flink.table.api.BatchTableEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.plan.nodes.exec.ExecNode;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecBoundedStreamScan;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecExchange;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecRel;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecScan;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecTableSourceScan;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecUnion;
import org.apache.flink.table.plan.nodes.process.DAGProcessContext;
import org.apache.flink.table.plan.nodes.process.DAGProcessor;
import org.apache.flink.table.resource.RelResource;
import org.apache.flink.table.util.ExecResourceUtil;

import org.apache.calcite.rel.RelDistribution;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Cpu and heap memory calculator for relNode.
 */
public class BatchRelCpuHeapMemCalculator implements DAGProcessor {

	private Map<BatchExecRel<?>, RelResource> relResMap;
	private final Set<ExecNode> calculatedRelSet = new HashSet<>();
	private TableEnvironment tEnv;

	public static void calculate(BatchTableEnvironment tEnv, Map<BatchExecRel<?>, RelResource> relResMap, BatchExecRel<?> rootExecRel) {
		new BatchRelCpuHeapMemCalculator(tEnv, relResMap).calculate(rootExecRel);
	}

	public BatchRelCpuHeapMemCalculator() {
	}

	// TODO
	public BatchRelCpuHeapMemCalculator(BatchTableEnvironment tEnv, Map<BatchExecRel<?>, RelResource> relResMap) {
		this.tEnv = tEnv;
		this.relResMap = relResMap;
	}

	@Override
	public List<ExecNode<?, ?>> process(List<ExecNode<?, ?>> sinkNodes, DAGProcessContext context) {
		tEnv = context.getTableEnvironment();
		for (ExecNode sinkNode : sinkNodes) {
			calculate(sinkNode);
		}

		return sinkNodes;
	}

	protected void calculateInputs(ExecNode<?, ?> node) {
		node.getInputNodes().forEach(input -> this.calculate(input));
	}

	public void calculate(ExecNode execNode) {
		if (!calculatedRelSet.add(execNode)) {
			return;
		}
		if (execNode instanceof BatchExecBoundedStreamScan) {
			calculateBoundedStreamScan((BatchExecBoundedStreamScan) execNode);
		} else if (execNode instanceof BatchExecTableSourceScan) {
			calculateTableSourceScan((BatchExecTableSourceScan) execNode);
		} else if (execNode instanceof BatchExecUnion) {
			calculateInputs(execNode);
		} else if (execNode instanceof BatchExecExchange) {
			calculateExchange((BatchExecExchange) execNode);
		} else {
			calculateDefaultRel(execNode);
		}
	}

	private void calculateBoundedStreamScan(BatchExecBoundedStreamScan scanBatchExec) {
		StreamTransformation transformation = scanBatchExec.getSourceTransformation(tEnv.execEnv());
		ResourceSpec sourceRes = transformation.getMinResources();
		if (sourceRes == null) {
			sourceRes = ResourceSpec.DEFAULT;
		}
		calculateBatchScan(scanBatchExec, sourceRes);
	}

	private void calculateTableSourceScan(BatchExecTableSourceScan tableSourceScan) {
		// user may have set resource for source transformation.
		StreamTransformation transformation = tableSourceScan.getSourceTransformation(tEnv.execEnv());
		ResourceSpec sourceRes = transformation.getMinResources();
		if (sourceRes == ResourceSpec.DEFAULT || sourceRes == null) {
			int heap = ExecResourceUtil.getSourceMem(tEnv.getConfig().getConf());
			sourceRes = ExecResourceUtil.getResourceSpec(tEnv.getConfig().getConf(), heap);
		}
		calculateBatchScan(tableSourceScan, sourceRes);
	}

	private void calculateBatchScan(BatchExecScan batchExecScan, ResourceSpec sourceRes) {
		RelResource relResource = new RelResource();
		ResourceSpec conversionRes = ResourceSpec.DEFAULT;
		if (batchExecScan.needInternalConversion()) {
			conversionRes = ExecResourceUtil.getDefaultResourceSpec(tEnv.getConfig().getConf());
		}
		ResourceSpec totalRes = sourceRes.merge(conversionRes);
		relResource.setCpu(totalRes.getCpuCores());
		relResource.setHeapMem(totalRes.getHeapMemory());
		relResMap.put(batchExecScan, relResource);
		batchExecScan.setResource(relResource);
		batchExecScan.setResForSourceAndConversion(sourceRes, conversionRes);
	}

	private void calculateDefaultRel(ExecNode node) {
		calculateInputs(node);
		RelResource relResource = getDefaultRelResource();
		relResMap.put((BatchExecRel<?>) node, relResource);
		node.setResource(relResource);
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
		double cpu = ExecResourceUtil.getDefaultCpu(tEnv.getConfig().getConf());
		int heap = ExecResourceUtil.getDefaultHeapMem(tEnv.getConfig().getConf());
		RelResource relResource = new RelResource();
		relResource.setCpu(cpu);
		relResource.setHeapMem(heap);
		return relResource;
	}
}
