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
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecRel;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecScan;
import org.apache.flink.table.resource.batch.ShuffleStage;
import org.apache.flink.table.util.ExecResourceUtil;

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
	private final Map<BatchExecRel<?>, Integer> calculatedResultMap = new HashMap<>();

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
		Set<BatchExecRel<?>> relSet = shuffleStage.getBatchExecRelSet();
		for (BatchExecRel rel : relSet) {
			shuffleStage.setResultParallelism(calculate(rel), false);
		}
	}

	private int calculate(BatchExecRel<?> batchExecRel) {
		if (calculatedResultMap.containsKey(batchExecRel)) {
			return calculatedResultMap.get(batchExecRel);
		}
		int result;
		if (batchExecRel instanceof BatchExecScan) {
			result = calculateSource((BatchExecScan) batchExecRel);
		} else if (batchExecRel instanceof SingleRel) {
			result = calculateSingle((SingleRel) batchExecRel);
		} else if (batchExecRel instanceof BiRel) {
			result = calculateBiRel((BiRel) batchExecRel);
		} else {
			throw new TableException("could not reach here. " + batchExecRel.getClass());
		}
		calculatedResultMap.put(batchExecRel, result);
		return result;
	}

	private int calculateSingle(SingleRel singleRel) {
		double rowCount = mq.getRowCount(singleRel.getInput());
		return ExecResourceUtil.calOperatorParallelism(rowCount, getTableConf());
	}

	private int calculateBiRel(BiRel biRel) {
		double maxRowCount = Math.max(mq.getRowCount(biRel.getLeft()), mq.getRowCount(biRel.getRight()));
		return ExecResourceUtil.calOperatorParallelism(maxRowCount, getTableConf());
	}

}
