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
import org.apache.flink.table.api.BatchTableEnvironment;
import org.apache.flink.table.api.TableConfigOptions;
import org.apache.flink.table.plan.nodes.exec.BatchExecNode;
import org.apache.flink.table.plan.nodes.exec.ExecNode;
import org.apache.flink.table.plan.nodes.exec.batch.BatchExecNodeVisitor;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecSink;
import org.apache.flink.table.resource.batch.autoconf.NodeManagedCalculatorOnStatistics;
import org.apache.flink.table.resource.batch.autoconf.NodeParallelismAdjuster;
import org.apache.flink.table.resource.batch.calculator.BatchNodeManagedCalculator;
import org.apache.flink.table.resource.batch.calculator.BatchParallelismCalculator;
import org.apache.flink.table.resource.batch.calculator.BatchResultPartitionCalculator;
import org.apache.flink.table.resource.batch.calculator.NodeCpuHeapMemCalculator;
import org.apache.flink.table.resource.batch.calculator.NodeFinalParallelismSetter;
import org.apache.flink.table.resource.batch.calculator.ParallelismCalculatorOnStatistics;
import org.apache.flink.table.resource.batch.calculator.ShuffleStageParallelismCalculator;
import org.apache.flink.table.resource.batch.schedule.RunningUnitGraphManagerPlugin;
import org.apache.flink.table.util.ExecResourceUtil;
import org.apache.flink.table.util.ExecResourceUtil.InferMode;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.InstantiationUtil;

import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.table.resource.batch.schedule.RunningUnitGraphManagerPlugin.RUNNING_UNIT_CONF_KEY;

/**
 * Assign nodes to runningUnits.
 */
public class RunningUnitKeeper {
	private static final Logger LOG = LoggerFactory.getLogger(RunningUnitKeeper.class);
	private final BatchTableEnvironment tableEnv;
	private List<NodeRunningUnit> runningUnits;
	private final Map<BatchExecNode<?>, Set<NodeRunningUnit>> nodeRunningUnitMap = new LinkedHashMap<>();
	// node --> shuffleStage
	private final Map<BatchExecNode<?>, Set<BatchExecNodeStage>> nodeStagesMap = new LinkedHashMap<>();
	private boolean supportRunningUnit = true;

	public RunningUnitKeeper(BatchTableEnvironment tableEnv) {
		this.tableEnv = tableEnv;
	}

	public void clear() {
		if (runningUnits != null) {
			runningUnits.clear();
		}
		nodeRunningUnitMap.clear();
		nodeStagesMap.clear();
	}

	private Configuration getTableConf() {
		return tableEnv.getConfig().getConf();
	}

	public void buildRUs(BatchExecNode<?> rootNode) {
		if (rootNode instanceof BatchExecSink<?>) {
			rootNode = (BatchExecNode<?>) ((BatchExecSink) rootNode).getInput();
		}
		// not support subsectionOptimization or external shuffle temporarily
		if (tableEnv.getConfig().getSubsectionOptimization()
				|| getTableConf().getBoolean(TableConfigOptions.SQL_EXEC_SORT_RANGE_ENABLED)) {
			supportRunningUnit = false;
			return;
		}
		RunningUnitGenerator visitor = new RunningUnitGenerator(getTableConf());
		rootNode.accept(visitor);
		runningUnits = visitor.getRunningUnits();
		for (NodeRunningUnit runningUnit : runningUnits) {
			for (BatchExecNode<?> node : runningUnit.getNodeSet()) {
				nodeRunningUnitMap.computeIfAbsent(node, k -> new LinkedHashSet<>()).add(runningUnit);
			}
		}
		buildNodeStagesMap();
	}

	public void setScheduleConfig(StreamGraphGenerator.Context context) {
		if (supportRunningUnit &&
				ExecResourceUtil.enableRunningUnitSchedule(getTableConf()) &&
				!getTableConf().getBoolean(TableConfigOptions.SQL_EXEC_DATA_EXCHANGE_MODE_ALL_BATCH)) {
			context.getConfiguration().setString(JobManagerOptions.GRAPH_MANAGER_PLUGIN, RunningUnitGraphManagerPlugin.class.getName());
			try {
				InstantiationUtil.writeObjectToConfig(runningUnits, context.getConfiguration(), RUNNING_UNIT_CONF_KEY);
			} catch (IOException e) {
				throw new FlinkRuntimeException("Could not serialize runningUnits to streamGraph config.", e);
			}
		}
	}

	public void calculateNodeResource(BatchExecNode<?> rootNode) {
		if (rootNode instanceof BatchExecSink<?>) {
			rootNode = (BatchExecNode<?>) ((BatchExecSink) rootNode).getInput();
		}
		RelMetadataQuery mq = rootNode.getFlinkPhysicalRel().getCluster().getMetadataQuery();
		NodeCpuHeapMemCalculator.calculate(tableEnv, rootNode);
		if (!supportRunningUnit) {
			// if runningUnit cannot be build, or no statics, we set resource according to config.
			// we are not able to set resource according to statics when runningUnits are not build.
			BatchResultPartitionCalculator.calculate(tableEnv, mq, rootNode);
			rootNode.accept(new BatchNodeManagedCalculator(getTableConf()));
			return;
		}
		InferMode inferMode = ExecResourceUtil.getInferMode(getTableConf());
		NodeFinalParallelismSetter.calculate(tableEnv, rootNode);
		Map<ExecNode<?, ?>, ShuffleStage> nodeShuffleStageMap = ShuffleStageGenerator.generate(rootNode);
		getShuffleStageParallelismCalculator(mq, getTableConf(), inferMode).calculate(nodeShuffleStageMap.values());
		Double cpuLimit = getTableConf().getDouble(TableConfigOptions.SQL_RESOURCE_RUNNING_UNIT_CPU_TOTAL);
		if (cpuLimit > 0) {
			NodeParallelismAdjuster.adjustParallelism(cpuLimit, nodeRunningUnitMap, nodeShuffleStageMap);
		}
		for (ExecNode<?, ?> node : nodeShuffleStageMap.keySet()) {
			node.getResource().setParallelism(nodeShuffleStageMap.get(node).getResultParallelism());
		}
		rootNode.accept(getNodeManagedCalculator(inferMode, mq));
	}

	private ShuffleStageParallelismCalculator getShuffleStageParallelismCalculator(
			RelMetadataQuery mq,
			Configuration tableConf,
			InferMode inferMode) {
		int envParallelism = tableEnv.streamEnv().getParallelism();
		if (inferMode.equals(InferMode.ALL)) {
			return new ParallelismCalculatorOnStatistics(mq, tableConf, envParallelism);
		} else {
			return new BatchParallelismCalculator(mq, tableConf, envParallelism);
		}
	}

	private BatchExecNodeVisitor getNodeManagedCalculator(
			InferMode inferMode, RelMetadataQuery mq) {
		if (inferMode.equals(InferMode.ALL)) {
			return new NodeManagedCalculatorOnStatistics(getTableConf(), mq);
		} else {
			return new BatchNodeManagedCalculator(getTableConf());
		}
	}

	public void addTransformation(BatchExecNode<?> node, StreamTransformation<?> transformation) {
		if (!supportRunningUnit || !nodeStagesMap.containsKey(node)) {
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

	public List<NodeRunningUnit> getRunningUnits() {
		return runningUnits;
	}
}
