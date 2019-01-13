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

import org.apache.flink.streaming.api.transformations.StreamTransformation;
import org.apache.flink.table.api.BatchTableEnvironment;
import org.apache.flink.table.plan.nodes.exec.ExecNode;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecBoundedStreamScan;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecExchange;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecTableSourceScan;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecValues;

import org.apache.calcite.rel.RelDistribution;

import java.util.HashSet;
import java.util.Set;

/**
 * Set final parallelism if needed at the beginning time.
 */
public class NodeFinalParallelismSetter {

	private final BatchTableEnvironment tEnv;
	private Set<ExecNode<?, ?>> calculatedNodeSet = new HashSet<>();

	private NodeFinalParallelismSetter(BatchTableEnvironment tEnv) {
		this.tEnv = tEnv;
	}

	public static void calculate(BatchTableEnvironment tEnv, ExecNode<?, ?> rootNode) {
		new NodeFinalParallelismSetter(tEnv).calculate(rootNode);
	}

	private void calculate(ExecNode<?, ?> batchExecNode) {
		if (!calculatedNodeSet.add(batchExecNode)) {
			return;
		}
		if (batchExecNode instanceof BatchExecTableSourceScan) {
			calculateTableSource((BatchExecTableSourceScan) batchExecNode);
		} else if (batchExecNode instanceof BatchExecBoundedStreamScan) {
			calculateBoundedStreamScan((BatchExecBoundedStreamScan) batchExecNode);
		} else if (batchExecNode.getInputNodes().size() == 1) {
			calculateSingle(batchExecNode);
		} else if (batchExecNode instanceof BatchExecValues) {
			calculateValues((BatchExecValues) batchExecNode);
		} else {
			calculateInputs(batchExecNode);
		}
	}

	private void calculateTableSource(BatchExecTableSourceScan tableSourceScan) {
		if (tableSourceScan.canLimitPushedDown()) {
			tableSourceScan.getResource().setParallelism(1, 1);
		} else {
			StreamTransformation transformation = tableSourceScan.getSourceTransformation(tEnv.streamEnv());
			if (transformation.getMaxParallelism() > 0) {
				tableSourceScan.getResource().setParallelism(transformation.getMaxParallelism(), transformation.getMaxParallelism());
			}
		}
	}

	private void calculateBoundedStreamScan(BatchExecBoundedStreamScan boundedStreamScan) {
		StreamTransformation transformation = boundedStreamScan.getSourceTransformation(tEnv.streamEnv());
		int parallelism = transformation.getParallelism();
		if (parallelism <= 0) {
			parallelism = tEnv.streamEnv().getParallelism();
		}
		boundedStreamScan.getResource().setParallelism(parallelism, parallelism);
	}

	private void calculateSingle(ExecNode<?, ?> singleNode) {
		calculateInputs(singleNode);
		ExecNode<?, ?> inputNode = singleNode.getInputNodes().get(0);
		if (inputNode instanceof BatchExecExchange) {
			if (((BatchExecExchange) inputNode).getDistribution().getType() == RelDistribution.Type.SINGLETON) {
				singleNode.getResource().setParallelism(1, 1);
				inputNode.getResource().setParallelism(1, 1);
			}
		}
	}

	private void calculateValues(BatchExecValues valuesBatchExec) {
		valuesBatchExec.getResource().setParallelism(1, 1);
	}

	private void calculateInputs(ExecNode<?, ?> node) {
		node.getInputNodes().forEach(this::calculate);
	}
}
