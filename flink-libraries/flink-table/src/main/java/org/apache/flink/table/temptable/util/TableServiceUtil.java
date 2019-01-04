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

package org.apache.flink.table.temptable.util;

import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.service.ServiceDescriptor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.temptable.FlinkTableService;
import org.apache.flink.table.temptable.FlinkTableServiceFactory;
import org.apache.flink.table.temptable.FlinkTableServiceFactoryDescriptor;
import org.apache.flink.table.temptable.FlinkTableServiceFunction;
import org.apache.flink.table.temptable.TableServiceException;
import org.apache.flink.table.temptable.rpc.TableServiceClient;
import org.apache.flink.table.util.TableProperties;

import java.util.UUID;

/**
 * Helper class for TableService.
 */
public final class TableServiceUtil {

	private TableServiceUtil() {}

	public static void createTableServiceJob(StreamExecutionEnvironment env, ServiceDescriptor serviceDescriptor) {

		ResourceSpec resourceSpec = ResourceSpec.newBuilder()
			.setCpuCores(serviceDescriptor.getServiceVcore())
			.setHeapMemoryInMB(serviceDescriptor.getServiceHeapMemoryMb())
			.setDirectMemoryInMB(serviceDescriptor.getServiceDirectMemoryMb())
			.setNativeMemoryInMB(serviceDescriptor.getServiceNativeMemoryMb())
			.build();

		DataStream<BaseRow> ds = env.addSource(new FlinkTableServiceFunction(serviceDescriptor, UUID.randomUUID().toString()))
			.setParallelism(serviceDescriptor.getServiceParallelism())
			.setMaxParallelism(serviceDescriptor.getServiceParallelism());

		ds.addSink(new SinkFunction<BaseRow>() {
			@Override
			public void invoke(BaseRow value, Context context) throws Exception {

			}
		}).setParallelism(serviceDescriptor.getServiceParallelism());
	}

	public static ServiceDescriptor getDefaultFlinkTableServiceDescriptor() {
		return getFlinkTableServiceDescriptor(1);
	}

	public static ServiceDescriptor getFlinkTableServiceDescriptor(int parallelism) {
		return new ServiceDescriptor(
			FlinkTableService.class.getCanonicalName())
			.setServiceVcore(0.5)
			.setServiceHeapMemoryMb(256)
			.setServiceNativeMemoryMb(64)
			.setServiceDirectMemoryMb(32)
			.setServiceParallelism(parallelism);
	}

	public static FlinkTableServiceFactoryDescriptor getDefaultTableServiceFactoryDescriptor(){
		return new FlinkTableServiceFactoryDescriptor(
			new FlinkTableServiceFactory(), new TableProperties());
	}

	public static void checkTableServiceReady(TableServiceClient client, int maxRetryTimes, long retryGap) {
		int retryTime = 0;
		while (retryTime++ < maxRetryTimes) {
			if (client.isReady()) {
				return;
			}
			try {
				Thread.sleep(retryGap);
			} catch (InterruptedException e) {}
		}
		throw new TableServiceException(new RuntimeException("TableService is not ready"));
	}
}
