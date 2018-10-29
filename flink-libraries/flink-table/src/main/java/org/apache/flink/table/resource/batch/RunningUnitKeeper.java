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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.streaming.api.transformations.StreamTransformation;
import org.apache.flink.table.api.BatchTableEnvironment;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.plan.batch.BatchExecRelVisitor;
import org.apache.flink.table.plan.nodes.physical.batch.RowBatchExecRel;
import org.apache.flink.table.resource.RelResource;
import org.apache.flink.table.resource.batch.autoconf.RelManagedCalculatorOnStatistics;
import org.apache.flink.table.resource.batch.autoconf.RelParallelismAdjuster;
import org.apache.flink.table.resource.batch.autoconf.RelReservedManagedMemAdjuster;
import org.apache.flink.table.resource.batch.calculator.BatchParallelismCalculator;
import org.apache.flink.table.resource.batch.calculator.BatchRelCpuHeapMemCalculator;
import org.apache.flink.table.resource.batch.calculator.BatchRelManagedCalculator;
import org.apache.flink.table.resource.batch.calculator.BatchResultPartitionCalculator;
import org.apache.flink.table.resource.batch.calculator.ParallelismCalculatorOnStatistics;
import org.apache.flink.table.resource.batch.calculator.RelFinalParallelismSetter;
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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.table.resource.batch.schedule.RunningUnitGraphManagerPlugin.RUNNING_UNIT_CONF_KEY;

/**
 * Assign relNodes to runningUnits.
 */
public class RunningUnitKeeper {
	private static final Logger LOG = LoggerFactory.getLogger(RunningUnitKeeper.class);
	private final TableConfig tableConfig;
	private final BatchTableEnvironment tableEnv;
	private List<RelRunningUnit> runningUnits;
	private BatchResultPartitionCalculator resultPartitionCalculator;
	private BatchRelCpuHeapMemCalculator relCpuHeapMemCalculator;
	private final Map<RowBatchExecRel, Set<RelRunningUnit>> relRunningUnitMap = new LinkedHashMap<>();
	// rel --> shuffleStage
	private final Map<RowBatchExecRel, Set<BatchExecRelStage>> relStagesMap = new LinkedHashMap<>();
	private boolean supportRunningUnit = true;

	public RunningUnitKeeper(BatchTableEnvironment tableEnv) {
		this.tableConfig = tableEnv.getConfig();
		this.tableEnv = tableEnv;
		this.resultPartitionCalculator = new BatchResultPartitionCalculator(tableEnv);
		this.relCpuHeapMemCalculator = new BatchRelCpuHeapMemCalculator(tableEnv);
	}

	public void clear() {
		if (runningUnits != null) {
			runningUnits.clear();
		}
		relRunningUnitMap.clear();
		relStagesMap.clear();
	}

	public void buildRUs(RowBatchExecRel rootNode) {
		// not support subsectionOptimization or external shuffle temporarily
		if (tableConfig.getSubsectionOptimization()
				|| tableConfig.enableRangePartition()) {
			supportRunningUnit = false;
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
		buildRelStagesMap();
	}

	public void setScheduleConfig(StreamGraphGenerator.Context context) {
		if (supportRunningUnit &&
				ExecResourceUtil.enableRunningUnitSchedule(tableConfig) &&
				!tableConfig.enableBatchExternalShuffle()) {
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
		this.relCpuHeapMemCalculator.setRelResourceMap(relResourceMap);
		this.relCpuHeapMemCalculator.calculate(rootNode);
		if (!supportRunningUnit) {
			// if runningUnit cannot be build, or no statics, we set resource according to config.
			// we are not able to set resource according to statics when runningUnits are not build.
			this.resultPartitionCalculator.calculate(rootNode);
			rootNode.accept(new BatchRelManagedCalculator(tableConfig, relResourceMap));
			for (Map.Entry<RowBatchExecRel, RelResource> entry : relResourceMap.entrySet()) {
				entry.getKey().setResource(entry.getValue());
				LOG.info(entry.getKey() + " resource: " + entry.getValue());
			}
			return;
		}
		InferMode inferMode = ExecResourceUtil.getInferMode(tableConfig);
		RelFinalParallelismSetter.calculate(tableEnv, rootNode);
		Map<RowBatchExecRel, ShuffleStage> relShuffleStageMap = ShuffleStageGenerator.generate(rootNode);
		RelMetadataQuery mq = rootNode.getCluster().getMetadataQuery();
		getShuffleStageParallelismCalculator(mq, tableConfig, inferMode).calculate(relShuffleStageMap.values());
		Tuple2<Double, Long> resourceLimit = ExecResourceUtil.getRunningUnitResourceLimit(tableConfig);
		if (resourceLimit != null) {
			RelParallelismAdjuster.adjustParallelism(resourceLimit.f0, relResourceMap, relRunningUnitMap, relShuffleStageMap);
		}
		rootNode.accept(getRelManagedCalculator(relShuffleStageMap, inferMode, mq, relResourceMap));
		if (resourceLimit != null) {
			adjustReservedManagedMem(relShuffleStageMap, relResourceMap, resourceLimit.f1);
		}
		for (RowBatchExecRel rel : relShuffleStageMap.keySet()) {
			rel.setResultPartitionCount(relShuffleStageMap.get(rel).getResultParallelism());
			rel.setResource(relResourceMap.get(rel));
			LOG.info(rel + " resource: " + relResourceMap.get(rel));
		}
	}

	private ShuffleStageParallelismCalculator getShuffleStageParallelismCalculator(
			RelMetadataQuery mq,
			TableConfig tableConfig,
			InferMode inferMode) {
		if (inferMode.equals(InferMode.ALL)) {
			return new ParallelismCalculatorOnStatistics(mq, tableConfig);
		} else {
			return new BatchParallelismCalculator(mq, tableConfig);
		}
	}

	private BatchExecRelVisitor<Void> getRelManagedCalculator(
			Map<RowBatchExecRel, ShuffleStage> relShuffleStageMap,
			InferMode inferMode, RelMetadataQuery mq,
			Map<RowBatchExecRel, RelResource> relResourceMap) {
		if (inferMode.equals(InferMode.ALL)) {
			return new RelManagedCalculatorOnStatistics(tableConfig, relShuffleStageMap, mq, relResourceMap);
		} else {
			return new BatchRelManagedCalculator(tableConfig, relResourceMap);
		}
	}

	private void adjustReservedManagedMem(Map<RowBatchExecRel, ShuffleStage> relShuffleStageMap, Map<RowBatchExecRel, RelResource> relResourceMap, long totalMem) {
		int minManagedMemory = ExecResourceUtil.getOperatorMinManagedMem(tableConfig);
		Map<RowBatchExecRel, Integer> relParallelismMap = new HashMap<>();
		for (Map.Entry<RowBatchExecRel, ShuffleStage> entry : relShuffleStageMap.entrySet()) {
			relParallelismMap.put(entry.getKey(), entry.getValue().getResultParallelism());
		}
		RelReservedManagedMemAdjuster.adjust(totalMem, relResourceMap, relParallelismMap, minManagedMemory, relRunningUnitMap);
	}

	public void addTransformation(RowBatchExecRel rel, StreamTransformation<?> transformation) {
		if (!supportRunningUnit || !relStagesMap.containsKey(rel)) {
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
