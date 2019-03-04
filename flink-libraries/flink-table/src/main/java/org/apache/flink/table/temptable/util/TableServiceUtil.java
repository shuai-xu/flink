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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.service.ServiceDescriptor;
import org.apache.flink.service.ServiceInstance;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.runtime.functions.aggfunctions.cardinality.MurmurHash;
import org.apache.flink.table.temptable.FlinkTableServiceFactory;
import org.apache.flink.table.temptable.FlinkTableServiceFactoryDescriptor;
import org.apache.flink.table.temptable.FlinkTableServiceFunction;
import org.apache.flink.table.temptable.TableServiceException;
import org.apache.flink.table.temptable.TableServiceOptions;
import org.apache.flink.table.temptable.rpc.TableServiceRegistry;
import org.apache.flink.table.util.TableProperties;
import org.apache.flink.util.InstantiationUtil;

import org.apache.commons.codec.binary.Base64;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.table.temptable.TableServiceOptions.TABLE_SERVICE_CLASS_NAME;
import static org.apache.flink.table.temptable.TableServiceOptions.TABLE_SERVICE_CLIENT_READ_BUFFER_SIZE;
import static org.apache.flink.table.temptable.TableServiceOptions.TABLE_SERVICE_CLIENT_WRITE_BUFFER_SIZE;
import static org.apache.flink.table.temptable.TableServiceOptions.TABLE_SERVICE_CPU_CORES;
import static org.apache.flink.table.temptable.TableServiceOptions.TABLE_SERVICE_DIRECT_MEMORY_MB;
import static org.apache.flink.table.temptable.TableServiceOptions.TABLE_SERVICE_HEAP_MEMORY_MB;
import static org.apache.flink.table.temptable.TableServiceOptions.TABLE_SERVICE_NATIVE_MEMORY_MB;
import static org.apache.flink.table.temptable.TableServiceOptions.TABLE_SERVICE_PARALLELISM;
import static org.apache.flink.table.temptable.TableServiceOptions.TABLE_SERVICE_READY_RETRY_BACKOFF_MS;
import static org.apache.flink.table.temptable.TableServiceOptions.TABLE_SERVICE_READY_RETRY_TIMES;
import static org.apache.flink.table.temptable.TableServiceOptions.TABLE_SERVICE_STORAGE_ROOT_PATH;

/**
 * Helper class for TableService.
 */
public final class TableServiceUtil {

	private TableServiceUtil() {}

	public static void createTableServiceJob(StreamExecutionEnvironment env, ServiceDescriptor serviceDescriptor) {

		ResourceSpec resourceSpec = ResourceSpec.newBuilder()
			.setCpuCores(serviceDescriptor.getServiceCpuCores())
			.setHeapMemoryInMB(serviceDescriptor.getServiceHeapMemoryMb())
			.setDirectMemoryInMB(serviceDescriptor.getServiceDirectMemoryMb())
			.setNativeMemoryInMB(serviceDescriptor.getServiceNativeMemoryMb())
			.build();

		DataStream<BaseRow> ds = env.addSource(new FlinkTableServiceFunction(serviceDescriptor))
			.setParallelism(serviceDescriptor.getServiceParallelism())
			.setMaxParallelism(serviceDescriptor.getServiceParallelism());

		ds.addSink(new SinkFunction<BaseRow>() {
			@Override
			public void invoke(BaseRow value, Context context) {

			}
		}).setParallelism(serviceDescriptor.getServiceParallelism());
	}

	public static ServiceDescriptor createTableServiceDescriptor(Configuration config) {
		ServiceDescriptor tableServiceDescriptor = new ServiceDescriptor()
			.setServiceClassName(config.getString(TABLE_SERVICE_CLASS_NAME))
			.setServiceParallelism(config.getInteger(TABLE_SERVICE_PARALLELISM))
			.setServiceHeapMemoryMb(config.getInteger(TABLE_SERVICE_HEAP_MEMORY_MB))
			.setServiceDirectMemoryMb(config.getInteger(TABLE_SERVICE_DIRECT_MEMORY_MB))
			.setServiceNativeMemoryMb(config.getInteger(TABLE_SERVICE_NATIVE_MEMORY_MB))
			.setServiceCpuCores(config.getDouble(TABLE_SERVICE_CPU_CORES));

		tableServiceDescriptor.getConfiguration().addAll(config);

		tableServiceDescriptor.getConfiguration().setInteger(TABLE_SERVICE_READY_RETRY_TIMES, config.getInteger(TABLE_SERVICE_READY_RETRY_TIMES));
		tableServiceDescriptor.getConfiguration().setLong(TABLE_SERVICE_READY_RETRY_BACKOFF_MS, config.getLong(TABLE_SERVICE_READY_RETRY_BACKOFF_MS));
		if (config.getString(TABLE_SERVICE_STORAGE_ROOT_PATH) != null) {
			tableServiceDescriptor.getConfiguration().setString(TABLE_SERVICE_STORAGE_ROOT_PATH, config.getString(TABLE_SERVICE_STORAGE_ROOT_PATH));
		}
		tableServiceDescriptor.getConfiguration().setInteger(TABLE_SERVICE_CLIENT_READ_BUFFER_SIZE, config.getInteger(TABLE_SERVICE_CLIENT_READ_BUFFER_SIZE));
		tableServiceDescriptor.getConfiguration().setInteger(TABLE_SERVICE_CLIENT_WRITE_BUFFER_SIZE, config.getInteger(TABLE_SERVICE_CLIENT_WRITE_BUFFER_SIZE));

		return tableServiceDescriptor;
	}

	public static FlinkTableServiceFactoryDescriptor getDefaultTableServiceFactoryDescriptor(){
		return new FlinkTableServiceFactoryDescriptor(
			new FlinkTableServiceFactory(), new TableProperties());
	}

	public static void checkTableServiceReady(TableServiceRegistry registry, int maxRetryTimes, long backOffMs) {
		int retryTime = 0;
		while (retryTime++ < maxRetryTimes) {
			if (registry.isTableServiceReady()) {
				return;
			}
			try {
				Thread.sleep(backOffMs);
			} catch (InterruptedException e) {}
		}
		throw new TableServiceException(new RuntimeException("TableService is not ready"));
	}

	public static void checkRegistryServiceReady(TableServiceRegistry registry, int maxRetryTimes, long backOffMs) {
		int retryTime = 0;
		while (retryTime++ < maxRetryTimes) {
			if (registry.getIp() != null && registry.getPort() > 0) {
				return;
			}
			try {
				Thread.sleep(backOffMs);
			} catch (InterruptedException e) {}
		}
		throw new TableServiceException(new RuntimeException("RegistryService is not ready"));
	}

	public static ResultPartitionID tablePartitionToResultPartition(String tableName, int partitionIndex) {
		long lower = MurmurHash.hash(tableName) & Integer.MAX_VALUE;
		long upper = partitionIndex;
		return new ResultPartitionID(
			new IntermediateResultPartitionID(lower, upper),
			new ExecutionAttemptID(0L, 0L)
		);
	}

	public static int tablePartitionToIndex(String tableName, int partitionIndex, int totalCount) {
		int hashCode = Objects.hash(tableName, partitionIndex);
		int index = hashCode % totalCount;
		if (index < 0) {
			index += totalCount;
		}
		return index;
	}

	public static void injectTableServiceInstances(Map<Integer, ServiceInstance> map, Configuration configuration) {

		byte[] serializedBytes;
		try {
			serializedBytes = InstantiationUtil.serializeObject(map);
		} catch (IOException e) {
			throw new TableServiceException(e);
		}
		String serializedString = Base64.encodeBase64URLSafeString(serializedBytes);
		configuration.setString(TableServiceOptions.TABLE_SERVICE_INSTANCES, serializedString);
	}

	public static Map<Integer, ServiceInstance> buildTableServiceInstance(Configuration configuration) {
		String serializedString = configuration.getString(TableServiceOptions.TABLE_SERVICE_INSTANCES);
		Map<Integer, ServiceInstance> map;
		try {
			map = InstantiationUtil.deserializeObject(
				Base64.decodeBase64(serializedString),
				ServiceInstance.class.getClassLoader()
			);
		} catch (Exception e) {
			throw new TableServiceException(e);
		}
		return map;
	}

}
