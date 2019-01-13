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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.plan.nodes.exec.ExecNode;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecScan;
import org.apache.flink.table.resource.batch.ShuffleStage;
import org.apache.flink.table.util.ExecResourceUtil;

import org.apache.calcite.rel.metadata.RelMetadataQuery;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Infer result partition count according to statistics.
 */
public class ParallelismCalculatorOnStatistics extends ShuffleStageParallelismCalculator {
	private final RelMetadataQuery mq;
	private final Map<ExecNode<?, ?>, Integer> calculatedResultMap = new HashMap<>();

	public ParallelismCalculatorOnStatistics(
			RelMetadataQuery mq,
			Configuration tableConf,
			int envParallelism) {
		super(mq, tableConf, envParallelism);
		this.mq = mq;
	}

	@Override
	protected void calculate(ShuffleStage shuffleStage) {
		if (shuffleStage.isParallelismFinal()) {
			return;
		}
		Set<ExecNode<?, ?>> nodeSet = shuffleStage.getExecNodeSet();
		for (ExecNode<?, ?> node : nodeSet) {
			shuffleStage.setResultParallelism(calculate(node), false);
		}
	}

	private int calculate(ExecNode<?, ?> execNode) {
		if (calculatedResultMap.containsKey(execNode)) {
			return calculatedResultMap.get(execNode);
		}
		int result;
		if (execNode instanceof BatchExecScan) {
			result = calculateSource((BatchExecScan) execNode);
		} else if (execNode.getInputNodes().size() == 1) {
			result = calculateSingleNode(execNode);
		} else if (execNode.getInputNodes().size() == 2) {
			result = calculateBiNode(execNode);
		} else {
			throw new TableException("could not reach here. " + execNode.getClass());
		}
		calculatedResultMap.put(execNode, result);
		return result;
	}

	private int calculateSingleNode(ExecNode<?, ?> singleNode) {
		double rowCount = mq.getRowCount(singleNode.getInputNodes().get(0).getFlinkPhysicalRel());
		return ExecResourceUtil.calOperatorParallelism(rowCount, getTableConf());
	}

	private int calculateBiNode(ExecNode<?, ?> twoInputNode) {
		double maxRowCount = Math.max(mq.getRowCount(twoInputNode.getInputNodes().get(0).getFlinkPhysicalRel()),
				mq.getRowCount(twoInputNode.getInputNodes().get(1).getFlinkPhysicalRel()));
		return ExecResourceUtil.calOperatorParallelism(maxRowCount, getTableConf());
	}

}
