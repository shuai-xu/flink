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

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecScan;
import org.apache.flink.table.plan.nodes.physical.batch.RowBatchExecRel;
import org.apache.flink.table.plan.resource.ShuffleStage;
import org.apache.flink.table.util.BatchExecResourceUtil;

import org.apache.calcite.rel.metadata.RelMetadataQuery;

import java.util.Set;

/**
 * Default parallelism calculator for shuffleStages.
 */
public class DefaultParallelismCalculator extends ShuffleStageParallelismCalculator {

	public DefaultParallelismCalculator(RelMetadataQuery mq,
			TableConfig tableConfig) {
		super(mq, tableConfig);
	}

	@Override
	protected void calculate(ShuffleStage shuffleStage) {
		if (shuffleStage.isParallelismFinal()) {
			return;
		}
		Set<RowBatchExecRel> relSet = shuffleStage.getBatchExecRelSet();
		int maxSourceParallelism = -1;
		for (RowBatchExecRel rel : relSet) {
			if (rel instanceof BatchExecScan) {
				int result = calculateSource((BatchExecScan) rel);
				if (result > maxSourceParallelism) {
					maxSourceParallelism = result;
				}
			}
		}
		if (maxSourceParallelism > 0) {
			shuffleStage.setResultParallelism(maxSourceParallelism, false);
		} else {
			shuffleStage.setResultParallelism(BatchExecResourceUtil.getOperatorDefaultParallelism(getTableConfig()), false);
		}
	}
}
