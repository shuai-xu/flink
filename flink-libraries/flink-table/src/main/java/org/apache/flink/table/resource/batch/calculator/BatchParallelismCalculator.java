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
import org.apache.flink.table.plan.nodes.exec.ExecNode;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecScan;
import org.apache.flink.table.resource.batch.ShuffleStage;
import org.apache.flink.table.util.ExecResourceUtil;

import org.apache.calcite.rel.metadata.RelMetadataQuery;

import java.util.Set;

/**
 * Default parallelism calculator for shuffleStages.
 */
public class BatchParallelismCalculator extends ShuffleStageParallelismCalculator {

	public BatchParallelismCalculator(RelMetadataQuery mq, Configuration tableConf, int envParallelism) {
		super(mq, tableConf, envParallelism);
	}

	@Override
	protected void calculate(ShuffleStage shuffleStage) {
		if (shuffleStage.isParallelismFinal()) {
			return;
		}
		Set<ExecNode<?, ?>> nodeSet = shuffleStage.getExecNodeSet();
		int maxSourceParallelism = -1;
		for (ExecNode<?, ?> node : nodeSet) {
			if (node instanceof BatchExecScan) {
				int result = calculateSource((BatchExecScan) node);
				if (result > maxSourceParallelism) {
					maxSourceParallelism = result;
				}
			}
		}
		if (maxSourceParallelism > 0) {
			shuffleStage.setResultParallelism(maxSourceParallelism, false);
		} else {
			shuffleStage.setResultParallelism(ExecResourceUtil.getOperatorDefaultParallelism(getTableConf(), envParallelism), false);
		}
	}
}
