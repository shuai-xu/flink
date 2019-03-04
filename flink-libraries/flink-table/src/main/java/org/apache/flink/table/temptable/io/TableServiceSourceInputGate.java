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

package org.apache.flink.table.temptable.io;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.PartitionRequestManager;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.taskmanager.TaskActions;
import org.apache.flink.service.LifeCycleAware;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

/**
 * A customized InputGate which used for TableServiceSource.
 */
public class TableServiceSourceInputGate extends SingleInputGate implements LifeCycleAware {

	private static final Logger LOG = LoggerFactory.getLogger(TableServiceSourceInputGate.class);

	private static final TableServiceActions ACTION_INSTANCE = new TableServiceActions();

	public TableServiceSourceInputGate(
		String sourceName,
		IntermediateDataSetID consumedResultId,
		int numberOfInputChannels,
		ExecutorService executorService) {
		super(
			sourceName,
			new JobID(),
			consumedResultId,
			ResultPartitionType.BLOCKING,
			0,
			numberOfInputChannels,
			ACTION_INSTANCE,
			null,
			new PartitionRequestManager(1, 1),
			executorService,
			true,
			false
		);
	}

	@Override
	public void open(Configuration config) throws Exception {
	}

	@Override
	public void close() throws Exception {
		this.releaseAllResources();
	}

	private static class TableServiceActions implements TaskActions {
		@Override
		public void triggerPartitionProducerStateCheck(JobID jobId, IntermediateDataSetID intermediateDataSetId, ResultPartitionID resultPartitionId) {
			LOG.info("triggerPartitionProducerStateCheck");
		}

		@Override
		public void failExternally(Throwable cause) {
			throw new RuntimeException(cause);
		}
	}
}
