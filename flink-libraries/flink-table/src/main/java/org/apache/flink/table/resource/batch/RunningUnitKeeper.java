/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.resource.batch;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.streaming.api.transformations.StreamTransformation;
import org.apache.flink.table.api.TableConfigOptions;
import org.apache.flink.table.plan.nodes.exec.BatchExecNode;
import org.apache.flink.table.resource.batch.schedule.RunningUnitGraphManagerPlugin;
import org.apache.flink.table.util.NodeResourceUtil;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.table.resource.batch.schedule.RunningUnitGraphManagerPlugin.RUNNING_UNIT_CONF_KEY;

/**
 * Holds runningUnits.
 */
public class RunningUnitKeeper {
	private final Configuration tableConf;
	private List<NodeRunningUnit> runningUnits = new ArrayList<>();
	private final Map<BatchExecNode<?>, Set<NodeRunningUnit>> nodeRunningUnitMap = new LinkedHashMap<>();
	private final Map<BatchExecNode<?>, Set<BatchExecNodeStage>> nodeStagesMap = new LinkedHashMap<>();

	public RunningUnitKeeper(Configuration tableConf) {
		this.tableConf = tableConf;
	}

	/**
	 * called when {@link org.apache.flink.table.api.BatchTableEnvironment} translate node dag.
	 */
	public void clear() {
		runningUnits.clear();
		nodeRunningUnitMap.clear();
		nodeStagesMap.clear();
	}

	public void setRunningUnits(List<NodeRunningUnit> runningUnits) {
		Preconditions.checkArgument(runningUnits != null, "runningUnits should not be null.");
		this.runningUnits.addAll(runningUnits);
		for (NodeRunningUnit runningUnit : runningUnits) {
			for (BatchExecNode<?> node : runningUnit.getNodeSet()) {
				nodeRunningUnitMap.computeIfAbsent(node, k -> new LinkedHashSet<>()).add(runningUnit);
			}
		}
		buildNodeStagesMap();
	}

	public void setScheduleConfig(StreamGraphGenerator.Context context) {
		if (runningUnits.isEmpty()) {
			// no build runningUnits.
			return;
		}
		if (NodeResourceUtil.enableRunningUnitSchedule(tableConf) &&
				!tableConf.getBoolean(TableConfigOptions.SQL_EXEC_DATA_EXCHANGE_MODE_ALL_BATCH)) {
			context.getConfiguration().setString(JobManagerOptions.GRAPH_MANAGER_PLUGIN, RunningUnitGraphManagerPlugin.class.getName());
			try {
				InstantiationUtil.writeObjectToConfig(runningUnits, context.getConfiguration(), RUNNING_UNIT_CONF_KEY);
			} catch (IOException e) {
				throw new FlinkRuntimeException("Could not serialize runningUnits to streamGraph config.", e);
			}
		}
	}

	public void addTransformation(BatchExecNode<?> node, StreamTransformation<?> transformation) {
		if (!nodeStagesMap.containsKey(node)) {
			return;
		}
		for (BatchExecNodeStage nodeStage : nodeStagesMap.get(node)) {
			nodeStage.addTransformation(transformation);
		}
	}

	private void buildNodeStagesMap() {
		for (NodeRunningUnit unit : runningUnits) {
			for (BatchExecNodeStage stage : unit.getAllNodeStages()) {
				nodeStagesMap.computeIfAbsent(stage.getBatchExecNode(), k -> new LinkedHashSet<>()).add(stage);
			}
		}
	}

	public Map<BatchExecNode<?>, Set<NodeRunningUnit>> getRunningUnitMap() {
		return nodeRunningUnitMap;
	}

	public List<NodeRunningUnit> getRunningUnits() {
		return runningUnits;
	}
}
