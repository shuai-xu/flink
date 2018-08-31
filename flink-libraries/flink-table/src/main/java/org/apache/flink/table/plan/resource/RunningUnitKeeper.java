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

import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.resources.CommonExtendedResource;
import org.apache.flink.api.common.resources.Resource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.transformations.StreamTransformation;
import org.apache.flink.table.api.BatchTableEnvironment;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecRel;
import org.apache.flink.table.plan.nodes.physical.batch.RowBatchExecRel;
import org.apache.flink.table.plan.resource.autoconf.RelManagedCalculatorOnStatistics;
import org.apache.flink.table.plan.resource.autoconf.RelParallelismAdjuster;
import org.apache.flink.table.plan.resource.autoconf.RelReservedManagedMemAdjuster;
import org.apache.flink.table.util.BatchExecResourceUtil;
import org.apache.flink.table.util.BatchExecResourceUtil.InferGranularity;

import org.apache.calcite.rel.metadata.RelMetadataQuery;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Assign relNodes to runningUnits.
 */
public class RunningUnitKeeper {

	private final TableConfig tableConfig;
	private final BatchTableEnvironment tableEnv;
	private List<RelRunningUnit> runningUnits;
	private final Map<RowBatchExecRel, Set<RelRunningUnit>> relRunningUnitMap = new LinkedHashMap<>();
	private final List<ShuffleStage> shuffleStages = new LinkedList<>();
	// rel --> shuffleStage
	private final Map<RowBatchExecRel, Set<BatchExecRelStage>> relStagesMap = new LinkedHashMap<>();
	private final Map<RowBatchExecRel, ShuffleStage> relShuffleStageMap = new LinkedHashMap<>();
	private boolean useRunningUnit = true;

	public RunningUnitKeeper(BatchTableEnvironment tableEnv) {
		this.tableConfig = tableEnv.getConfig();
		this.tableEnv = tableEnv;
	}

	public ShuffleStage getRelShuffleStage(BatchExecRel<?> rel) {
		return relShuffleStageMap.get(rel);
	}

	public void buildRUs(RowBatchExecRel rootNode) {
		// not support subsectionOptimization or external shuffle temporarily
		if (tableConfig.getSubsectionOptimization()
			|| tableConfig.enableBatchExternalShuffle()
			|| tableConfig.enableRangePartition()) {
			useRunningUnit = false;
			return;
		}
		GenerateRunningUnitVisitor visitor = new GenerateRunningUnitVisitor(rootNode);
		rootNode.accept(visitor);
		runningUnits = visitor.getRunningUnits();
		for (RelRunningUnit runningUnit : runningUnits) {
			for (ShuffleStageInRunningUnit shuffleStageInRU : runningUnit.getShuffleStagesInRunningUnit()) {
				for (RowBatchExecRel rel : shuffleStageInRU.getRelSet()) {
					relRunningUnitMap.computeIfAbsent(rel, k->new LinkedHashSet<>()).add(runningUnit);
				}
			}
		}
		buildShuffleStages();
		buildRelStagesMap();
	}

	public void setScheduleConfig(StreamExecutionEnvironment streamEnv, StreamGraph streamGraph) {
//		if (useRunningUnit && BatchExecResourceUtil.enableRunningUnitSchedule(tableConfig)) {
//			streamEnv.setConfiguration(JobManagerOptions.SCHEDULER_EVENT_HANDLER, RunningUnitSchedulerEventHandler.class.getName());
//			try {
//				InstantiationUtil.writeObjectToConfig(runningUnits, streamGraph.getProperties().getConfiguration(), RUNNING_UNIT_CONF_KEY);
//			} catch (IOException e) {
//				throw new FlinkRuntimeException("Could not serialize runningUnits to streamGraph config.", e);
//			}
//		}
	}

	public void calculateRelResource(RowBatchExecRel rootNode) {
		Map<RowBatchExecRel, RelResource> relResourceMap = new LinkedHashMap<>();
		InferGranularity inferGranularity = BatchExecResourceUtil.getInferGranularity(tableConfig);
		if (!useRunningUnit || !inferGranularity.equals(InferGranularity.ALL)) {
			// if runningUnit cannot be build, or no statics, we set resource according to config.
			// we are not able to set resource according to statics when runningUnits are not build.
			rootNode.accept(new DefaultResultPartitionCalculator(tableConfig, tableEnv));
			rootNode.accept(new RelCpuHeapMemCalculator(tableConfig, tableEnv, relResourceMap));
			rootNode.accept(new DefaultRelManagedCalculator(tableConfig, relResourceMap));
			for (Map.Entry<RowBatchExecRel, RelResource> entry : relResourceMap.entrySet()) {
				Tuple2<ResourceSpec, ResourceSpec> resourceTuple = buildResourceSpec(relResourceMap.get(entry.getKey()));
				entry.getKey().setResourceSpec(resourceTuple.f0, resourceTuple.f1);
			}
		} else {
			RelMetadataQuery mq = rootNode.getCluster().getMetadataQuery();
			rootNode.accept(new ResultPartitionCalculatorOnStatistics(tableConfig, tableEnv, this, mq));
			rootNode.accept(new RelCpuHeapMemCalculator(tableConfig, tableEnv, relResourceMap));
			Tuple2<Double, Long> resourceLimit = BatchExecResourceUtil.getAdjustTotalResource(tableConfig);
			if (resourceLimit != null) {
				RelParallelismAdjuster adjuster = new RelParallelismAdjuster(resourceLimit.f0, relResourceMap);
				adjuster.adjust(relShuffleStageMap);
			}
			rootNode.accept(new RelManagedCalculatorOnStatistics(tableConfig, this, mq, relResourceMap));
			if (resourceLimit != null) {
				RelReservedManagedMemAdjuster adjuster = new RelReservedManagedMemAdjuster(resourceLimit.f1, relResourceMap, relShuffleStageMap);
				adjuster.adjust(relRunningUnitMap);
			}
			for (ShuffleStage shuffleStage : shuffleStages) {
				for (BatchExecRel<?> relNode : shuffleStage.getBatchExecRelSet()) {
					relNode.setResultPartitionCount(shuffleStage.getResultParallelism());
					Tuple2<ResourceSpec, ResourceSpec> resourceTuple = buildResourceSpec(relResourceMap.get(relNode));
					relNode.setResourceSpec(resourceTuple.f0, resourceTuple.f1);
				}
			}
		}
	}

	public void setRelID(RowBatchExecRel rel, int id) {
		if (!useRunningUnit || !relStagesMap.containsKey(rel)) {
			return;
		}
		for (BatchExecRelStage relStage : relStagesMap.get(rel)) {
			relStage.setRelID(id);
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

	private Tuple2<ResourceSpec, ResourceSpec> buildResourceSpec(RelResource relResource) {
		ResourceSpec.Builder reservedBuilder = ResourceSpec.newBuilder();
		ResourceSpec.Builder preferBuilder = ResourceSpec.newBuilder();
		reservedBuilder.setCpuCores(relResource.getCpu());
		preferBuilder.setCpuCores(relResource.getCpu());
		reservedBuilder.setHeapMemoryInMB(relResource.getHeapMem());
		preferBuilder.setHeapMemoryInMB(relResource.getHeapMem());
		reservedBuilder.addExtendedResource(new CommonExtendedResource(
				ResourceSpec.MANAGED_MEMORY_NAME,
				relResource.getReservedManagedMem(),
				Resource.ResourceAggregateType.AGGREGATE_TYPE_SUM));
		preferBuilder.addExtendedResource(new CommonExtendedResource(
				ResourceSpec.MANAGED_MEMORY_NAME,
				relResource.getPreferManagedMem(),
				Resource.ResourceAggregateType.AGGREGATE_TYPE_SUM));
		reservedBuilder.addExtendedResource(new CommonExtendedResource(
				ResourceSpec.FLOATING_MANAGED_MEMORY_NAME,
				relResource.getPreferManagedMem() - relResource.getReservedManagedMem(),
				Resource.ResourceAggregateType.AGGREGATE_TYPE_SUM));
		return new Tuple2<>(reservedBuilder.build(), preferBuilder.build());
	}

	/**
	 * For example: scan -> calc1 -> agg1 -> calc2 -> agg2 -> calc3 -> agg3.
	 * ShuffleStageInRunningUnits are:
	 * 1) s1 => scan, calc1, agg1
	 * 2) s2 => agg1, calc2, agg2
	 * 3) s3 => agg2, calc3, agg3
	 * 4) s4 => agg3
	 * now relShuffleStageInRUMap may like following:
	 * scan -> s1(scan, calc1, agg1), calc1 -> s1(scan, calc1, agg1), agg1 -> s1 & s2(scan, calc1, agg1, calc2, agg2)
	 * calc2 -> s2(agg1, calc2, agg2), agg2 -> s2 & s3 (agg1, calc2, agg2, calc3, agg3)
	 * calc3 -> s3(agg2, calc3, agg3), agg3 -> s3 & s4(agg2, calc3, agg3)
	 *
	 * <p>So we need visit the relShuffleStageInRUMap to merge these shuffleStageInRU to a shuffleStage.
	 */
	private void buildShuffleStages() {
		// all shuffleStageInRUs
		Set<ShuffleStageInRunningUnit> shuffleStageInRUSet = new LinkedHashSet<>();
		Map<Object, Set<ShuffleStageInRunningUnit>> relShuffleStageInRUMap = new LinkedHashMap<>();
		for (RelRunningUnit unit : runningUnits) {
			for (ShuffleStageInRunningUnit shuffleStageInRU : unit.getShuffleStagesInRunningUnit()) {
				shuffleStageInRUSet.add(shuffleStageInRU);
				for (BatchExecRel<?> rel : shuffleStageInRU.getRelSet()) {
					relShuffleStageInRUMap.computeIfAbsent(rel, k -> new LinkedHashSet<>()).add(shuffleStageInRU);
				}
			}
		}
		while (!shuffleStageInRUSet.isEmpty()) {
			ShuffleStage shuffleStage = new ShuffleStage();
			ShuffleStageInRunningUnit startShuffleStageInRU = shuffleStageInRUSet.iterator().next();
			List<RowBatchExecRel> toVisitRelList = new LinkedList<>(startShuffleStageInRU.getRelSet());
			Set<RowBatchExecRel> visitedRelSubjectSet = new LinkedHashSet<>();

			while (!toVisitRelList.isEmpty()) {
				RowBatchExecRel toVisitRel = toVisitRelList.remove(0);
				if (visitedRelSubjectSet.contains(toVisitRel)) {
					continue;
				} else {
					visitedRelSubjectSet.add(toVisitRel);
				}
				relShuffleStageMap.put(toVisitRel, shuffleStage);
				Set<ShuffleStageInRunningUnit> toVisitShuffleStageInRUSet = relShuffleStageInRUMap.get(toVisitRel);
				shuffleStage.addShuffleStageInRus(toVisitShuffleStageInRUSet);

				for (ShuffleStageInRunningUnit toVisit : toVisitShuffleStageInRUSet) {
					shuffleStageInRUSet.remove(toVisit);
					toVisitRelList.addAll(toVisit.getRelSet());
					toVisit.setShuffleStage(shuffleStage);
				}
			}
			shuffleStages.add(shuffleStage);
		}
	}

	private void buildRelStagesMap() {
		for (RelRunningUnit unit : runningUnits) {
			for (ShuffleStageInRunningUnit shuffleStageInRU : unit.getShuffleStagesInRunningUnit()) {
				for (BatchExecRelStage stage : shuffleStageInRU.getRelStages()) {
					relStagesMap.computeIfAbsent(stage.getBatchExecRel(), k->new LinkedHashSet<>()).add(stage);
				}
			}
		}
	}

	public List<RelRunningUnit> getRunningUnits() {
		return runningUnits;
	}
}
