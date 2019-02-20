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

package org.apache.flink.table.resource.batch;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.BatchTableEnvironment;
import org.apache.flink.table.api.TableConfigOptions;
import org.apache.flink.table.plan.nodes.exec.BatchExecNode;
import org.apache.flink.table.plan.nodes.exec.ExecNode;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecSink;
import org.apache.flink.table.plan.nodes.process.DAGProcessContext;
import org.apache.flink.table.plan.nodes.process.DAGProcessor;
import org.apache.flink.util.Preconditions;

import java.util.LinkedList;
import java.util.List;

/**
 * Build runningUnits and set to {@link RunningUnitKeeper}.
 * TODO moved to {@link org.apache.flink.table.resource.batch.parallelism.BatchParallelismProcessor}
 * after reconstructing schedule.
 */
public class BatchRunningUnitBuildProcessor implements DAGProcessor {

	@Override
	public List<ExecNode<?, ?>> process(List<ExecNode<?, ?>> sinkNodes, DAGProcessContext context) {
		// runningUnit do not support sort range
		if (!context.getTableEnvironment().getConfig().getConf().getBoolean(TableConfigOptions.SQL_EXEC_SORT_RANGE_ENABLED)) {
			List<BatchExecNode<?>> batchExecNodes = new LinkedList<>();
			for (ExecNode<?, ?> sinkNode : sinkNodes) {
				Preconditions.checkArgument(sinkNode instanceof BatchExecNode);
				if (sinkNode instanceof BatchExecSink<?>) {
					batchExecNodes.add((BatchExecNode<?>) ((BatchExecSink) sinkNode).getInput());
				} else {
					batchExecNodes.add((BatchExecNode<?>) sinkNode);
				}
			}
			RunningUnitGenerator visitor = getRunningUnitGenerator(context.getTableEnvironment().getConfig().getConf());
			batchExecNodes.forEach(b -> b.accept(visitor));
			((BatchTableEnvironment) context.getTableEnvironment()).getRUKeeper().setRunningUnits(visitor.getRunningUnits());
		}
		return sinkNodes;
	}

	protected RunningUnitGenerator getRunningUnitGenerator(Configuration tableConf) {
		return new RunningUnitGenerator(tableConf);
	}
}
