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
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecScan;
import org.apache.flink.table.plan.nodes.physical.batch.RowBatchExecRel;
import org.apache.flink.table.plan.resource.ShuffleStage;
import org.apache.flink.table.util.BatchExecResourceUtil;

import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;



/**
 * Infer result partition count according to statistics.
 */
public class ParallelismCalculatorOnStatistics extends ShuffleStageParallelismCalculator {
	private final RelMetadataQuery mq;
	private final Map<RowBatchExecRel, Integer> calculatedResultMap = new HashMap<>();

	public ParallelismCalculatorOnStatistics(
			RelMetadataQuery mq,
			TableConfig tableConfig) {
		super(mq, tableConfig);
		this.mq = mq;
	}

	@Override
	protected void calculate(ShuffleStage shuffleStage) {
		if (shuffleStage.isParallelismFinal()) {
			return;
		}
		Set<RowBatchExecRel> relSet = shuffleStage.getBatchExecRelSet();
		for (RowBatchExecRel rel : relSet) {
			shuffleStage.setResultParallelism(calculate(rel), false);
		}
	}

	private int calculate(RowBatchExecRel rowBatchExecRel) {
		if (calculatedResultMap.containsKey(rowBatchExecRel)) {
			return calculatedResultMap.get(rowBatchExecRel);
		}
		int result;
		if (rowBatchExecRel instanceof BatchExecScan) {
			result = calculateSource((BatchExecScan) rowBatchExecRel);
		} else if (rowBatchExecRel instanceof SingleRel) {
			result = calculateSingle((SingleRel & RowBatchExecRel) rowBatchExecRel);
		} else if (rowBatchExecRel instanceof BiRel) {
			result = calculateBiRel((BiRel & RowBatchExecRel) rowBatchExecRel);
		} else {
			throw new TableException("could not reach here. " + rowBatchExecRel.getClass());
		}
		calculatedResultMap.put(rowBatchExecRel, result);
		return result;
	}

	private <T extends SingleRel & RowBatchExecRel> int calculateSingle(T singleRel) {
		double rowCount = mq.getRowCount(singleRel.getInput());
		return BatchExecResourceUtil.calOperatorParallelism(rowCount, getTableConfig());
	}

	private <T extends BiRel & RowBatchExecRel> int calculateBiRel(T biRel) {
		double maxRowCount = Math.max(mq.getRowCount(biRel.getLeft()), mq.getRowCount(biRel.getRight()));
		return BatchExecResourceUtil.calOperatorParallelism(maxRowCount, getTableConfig());
	}

}
