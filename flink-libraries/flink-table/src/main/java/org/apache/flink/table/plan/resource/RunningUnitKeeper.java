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

package org.apache.flink.table.plan.resource;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.streaming.api.transformations.StreamTransformation;
import org.apache.flink.table.api.BatchTableEnvironment;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.plan.nodes.physical.batch.RowBatchExecRel;
import org.apache.flink.table.plan.resource.autoconf.RelManagedCalculatorOnStatistics;
import org.apache.flink.table.plan.resource.autoconf.RelParallelismAdjuster;
import org.apache.flink.table.plan.resource.autoconf.RelReservedManagedMemAdjuster;
import org.apache.flink.table.plan.resource.schedule.RunningUnitGraphManagerPlugin;
import org.apache.flink.table.util.BatchExecResourceUtil;
import org.apache.flink.table.util.BatchExecResourceUtil.InferMode;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.InstantiationUtil;

import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.table.plan.resource.schedule.RunningUnitGraphManagerPlugin.RUNNING_UNIT_CONF_KEY;

/**
 * Assign relNodes to runningUnits.
 */
public class RunningUnitKeeper {
	private static final Logger LOG = LoggerFactory.getLogger(RunningUnitKeeper.class);
	private final TableConfig tableConfig;
	private final BatchTableEnvironment tableEnv;
	private List<RelRunningUnit> runningUnits;
	private final Map<RowBatchExecRel, Set<RelRunningUnit>> relRunningUnitMap = new LinkedHashMap<>();
	// rel --> shuffleStage
	private final Map<RowBatchExecRel, Set<BatchExecRelStage>> relStagesMap = new LinkedHashMap<>();
	private  Map<RowBatchExecRel, ShuffleStage> relShuffleStageMap;
	private boolean useRunningUnit = true;

	public RunningUnitKeeper(BatchTableEnvironment tableEnv) {
		this.tableConfig = tableEnv.getConfig();
		this.tableEnv = tableEnv;
	}

	public void clear() {
		if (runningUnits != null) {
			runningUnits.clear();
		}
		if (relShuffleStageMap != null) {
			relShuffleStageMap.clear();
		}
		relRunningUnitMap.clear();
		relStagesMap.clear();
	}

	public ShuffleStage getRelShuffleStage(RowBatchExecRel rowBatchExecRel) {
		return relShuffleStageMap.get(rowBatchExecRel);
	}

	public void buildRUs(RowBatchExecRel rootNode) {
		// not support subsectionOptimization or external shuffle temporarily
		if (tableConfig.getSubsectionOptimization()
				|| tableConfig.enableRangePartition()) {
			useRunningUnit = false;
			return;
		}
		RunningUnitGenerator visitor = new RunningUnitGenerator(tableConfig);
		rootNode.accept(visitor);
		runningUnits = visitor.getRunningUnits();
		for (RelRunningUnit runningUnit : runningUnits) {
			for (RowBatchExecRel rel : runningUnit.getRelSet()) {
				relRunningUnitMap.computeIfAbsent(rel, k -> new LinkedHashSet<>()).add(runningUnit);
			}
		}
		relShuffleStageMap = ShuffleStageGenerator.generate(rootNode);
		buildRelStagesMap();
	}

	public void setScheduleConfig(StreamGraphGenerator.Context context) {
		if (useRunningUnit && BatchExecResourceUtil.enableRunningUnitSchedule(tableConfig)) {
			context.getConfiguration().setString(JobManagerOptions.GRAPH_MANAGER_PLUGIN, RunningUnitGraphManagerPlugin.class.getName());
			try {
				InstantiationUtil.writeObjectToConfig(runningUnits, context.getConfiguration(), RUNNING_UNIT_CONF_KEY);
			} catch (IOException e) {
				throw new FlinkRuntimeException("Could not serialize runningUnits to streamGraph config.", e);
			}
		}
	}

	public void calculateRelResource(RowBatchExecRel rootNode) {
		Map<RowBatchExecRel, RelResource> relResourceMap = new LinkedHashMap<>();
		InferMode inferMode = BatchExecResourceUtil.getInferMode(tableConfig);
		if (!useRunningUnit || !inferMode.equals(InferMode.ALL)) {
			// if runningUnit cannot be build, or no statics, we set resource according to config.
			// we are not able to set resource according to statics when runningUnits are not build.
			rootNode.accept(new DefaultResultPartitionCalculator(tableConfig, tableEnv));
			rootNode.accept(new RelCpuHeapMemCalculator(tableConfig, tableEnv, relResourceMap));
			rootNode.accept(new DefaultRelManagedCalculator(tableConfig, relResourceMap));
			for (Map.Entry<RowBatchExecRel, RelResource> entry : relResourceMap.entrySet()) {
				entry.getKey().setResource(entry.getValue());
				LOG.info(entry.getKey() + " resource: " + entry.getValue());
			}
		} else {
			RelMetadataQuery mq = rootNode.getCluster().getMetadataQuery();
			rootNode.accept(new ResultPartitionCalculatorOnStatistics(tableConfig, tableEnv, this, mq));
			rootNode.accept(new RelCpuHeapMemCalculator(tableConfig, tableEnv, relResourceMap));
			Tuple2<Double, Long> resourceLimit = BatchExecResourceUtil.getRunningUnitResourceLimit(tableConfig);
			if (resourceLimit != null) {
				RelParallelismAdjuster.adjustParallelism(resourceLimit.f0, relResourceMap, relRunningUnitMap, relShuffleStageMap);
			}
			rootNode.accept(new RelManagedCalculatorOnStatistics(tableConfig, this, mq, relResourceMap));
			if (resourceLimit != null) {
				int minManagedMemory = BatchExecResourceUtil.getOperatorMinManagedMem(tableConfig);
				Map<RowBatchExecRel, Integer> relParallelismMap = new HashMap<>();
				for (Map.Entry<RowBatchExecRel, ShuffleStage> entry : relShuffleStageMap.entrySet()) {
					relParallelismMap.put(entry.getKey(), entry.getValue().getResultParallelism());
				}
				RelReservedManagedMemAdjuster adjuster = new RelReservedManagedMemAdjuster(resourceLimit.f1, relResourceMap, relParallelismMap, minManagedMemory);
				adjuster.adjust(relRunningUnitMap);
			}
			for (RowBatchExecRel rel : relShuffleStageMap.keySet()) {
				rel.setResultPartitionCount(relShuffleStageMap.get(rel).getResultParallelism());
				rel.setResource(relResourceMap.get(rel));
				LOG.info(rel + " resource: " + relResourceMap.get(rel));
			}
		}
	}

	public void addTransformation(RowBatchExecRel rel, StreamTransformation<?> transformation) {
		if (!useRunningUnit || !relStagesMap.containsKey(rel)) {
			return;
		}
		for (BatchExecRelStage relStage : relStagesMap.get(rel)) {
			relStage.addTransformation(transformation);
		}
	}

	private void buildRelStagesMap() {
		for (RelRunningUnit unit : runningUnits) {
			for (BatchExecRelStage stage : unit.getAllRelStages()) {
				relStagesMap.computeIfAbsent(stage.getBatchExecRel(), k -> new LinkedHashSet<>()).add(stage);
			}
		}
	}

	public List<RelRunningUnit> getRunningUnits() {
		return runningUnits;
	}
}
