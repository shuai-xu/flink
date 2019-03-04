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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.netty.NettyConfig;
import org.apache.flink.runtime.io.network.netty.NettyConnectionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.external.ExternalBlockShuffleServiceOptions;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.service.ServiceInstance;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.temptable.TableServiceException;
import org.apache.flink.table.temptable.rpc.TableServiceClient;
import org.apache.flink.table.temptable.util.BytesUtil;
import org.apache.flink.table.temptable.util.TableServiceUtil;
import org.apache.flink.table.types.DataTypes;
import org.apache.flink.table.types.RowType;
import org.apache.flink.table.typeutils.BaseRowSerializer;
import org.apache.flink.table.util.TableProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * TableServiceSourceFunction interact with TableService.
 */
public class TableServiceSourceFunction extends RichParallelSourceFunction<BaseRow> {

	private final TableProperties tableProperties;

	private final String tableName;

	private final RowType resultType;

	private TableServiceClient tableServiceClient;

	private List<Integer> requestTablePartitions;

	private Configuration globalConfig;

	private BaseRowGenerator baseRowGenerator;

	private int memorySizePerBufferInBytes;

	private static final int MAX_SEGMENT_NUMBER = 192;

	private ExecutorService executorService;

	private static final Logger LOG = LoggerFactory.getLogger(TableServiceSourceFunction.class);

	public TableServiceSourceFunction(TableProperties tableProperties, String tableName, RowType resultType) {
		this.tableProperties = tableProperties;
		this.tableName = tableName;
		this.resultType = resultType;
	}

	@Override
	public void open(Configuration configuration) throws Exception {
		BaseRowSerializer<BaseRow> baseRowSerializer =
			(BaseRowSerializer<BaseRow>) DataTypes.createInternalSerializer(resultType);
		baseRowGenerator = new BaseRowGenerator(baseRowSerializer);
		memorySizePerBufferInBytes = configuration.getInteger(
			ExternalBlockShuffleServiceOptions.MEMORY_SIZE_PER_BUFFER_IN_BYTES);
		globalConfig = configuration;
		tableServiceClient = new TableServiceClient();
		tableServiceClient.open(tableProperties);
		executorService = Executors.newSingleThreadExecutor();
		assignPartitions();
		LOG.info("Table Service Source opened");
	}

	@Override
	public void close() throws Exception {
		if (tableServiceClient != null) {
			tableServiceClient.close();
		}
		if (executorService != null) {
			executorService.shutdown();
		}
		LOG.info("Table Service Source closed");
	}

	@Override
	public void run(SourceContext<BaseRow> ctx) throws Exception {
		if (requestTablePartitions != null) {
			for (Integer partitionIndex : requestTablePartitions) {

				NetworkBufferPool networkBufferPool = null;
				TableServiceSourceInputGate inputGate = null;
				NettyConnectionManager nettyConnectionManager = null;
				try {
					/**
					 * 	NetworkBufferPool is a temporary solution
					 * 	ShuffleService will be decoupled with Buffer in the future.
					 */
					networkBufferPool = new NetworkBufferPool(MAX_SEGMENT_NUMBER, memorySizePerBufferInBytes);
					ConnectionID connectionID = createConnectionID(tableName, partitionIndex);
					nettyConnectionManager = createConnectionManager(connectionID);
					// ConnectionManager must start before constructing RemoteInputChannle.
					nettyConnectionManager.start(new ResultPartitionManager(), new TaskEventDispatcher());
					inputGate = createInputGate(tableName, partitionIndex, networkBufferPool, connectionID, nettyConnectionManager);
					inputGate.open(globalConfig);
					handleData(ctx, inputGate);
				} catch (Exception e) {
					LOG.error(e.getMessage(), e);
					throw new TableServiceException(e);
				} finally {
					if (nettyConnectionManager != null) {
						nettyConnectionManager.shutdown();
					}
					if (inputGate != null) {
						inputGate.close();
					}
					if (networkBufferPool != null) {
						networkBufferPool.destroyAllBufferPools();
						networkBufferPool.destroy();
					}
				}
			}
		}
	}

	private void handleData(SourceContext<BaseRow> ctx, TableServiceSourceInputGate inputGate) {
		while (true) {
			try {
				Optional<BufferOrEvent> bufferOrEvent = inputGate.getNextBufferOrEvent();
				if (!bufferOrEvent.isPresent()) {
					LOG.debug("reach end of InputGate");
					break;
				}
				if (bufferOrEvent.get().isBuffer()) {
					LOG.debug("buffer read");

					baseRowGenerator.setBuffer(bufferOrEvent.get().getBuffer());
					while (baseRowGenerator.hasNext()) {
						BaseRow baseRow = baseRowGenerator.getNext();
						ctx.collect(baseRow);
					}

				} else if (bufferOrEvent.get().isEvent()) {
					LOG.debug("event read");
				}
			} catch (Exception e) {
				LOG.error(e.getMessage(), e);
				new TableServiceException(e);
			}
		}
	}

	@Override
	public void cancel() {

	}

	private void assignPartitions() {
		List <Integer> list = tableServiceClient.getPartitions(tableName);
		if (list == null || list.isEmpty()) {
			throw new TableServiceException(new RuntimeException("Table Cache do not exists."));
		}
		Collections.sort(list);
		int workerCount = getRuntimeContext().getNumberOfParallelSubtasks();
		int startIndex = getRuntimeContext().getIndexOfThisSubtask();
		requestTablePartitions = new ArrayList<>();
		while (startIndex < list.size()) {
			requestTablePartitions.add(list.get(startIndex));
			startIndex += workerCount;
		}
	}

	private ConnectionID createConnectionID(String tableName, int partitionIndex) {
		Map<Integer, ServiceInstance> serviceInstanceMap = tableServiceClient.getServiceInstanceMap();
		int targetIndex = TableServiceUtil.tablePartitionToIndex(tableName, partitionIndex, serviceInstanceMap.size());
		ServiceInstance targetInstance = serviceInstanceMap.get(targetIndex);
		if (targetInstance == null) {
			throw new TableServiceException(new RuntimeException("serviceInstanceMap does not contains service instance with instanceId = " + targetIndex));
		}

		final Integer port = globalConfig.getInteger(ExternalBlockShuffleServiceOptions.FLINK_SHUFFLE_SERVICE_PORT_KEY);
		checkArgument(port != null && port > 0 && port < 65536,
			"Invalid port number for ExternalBlockShuffleService: " + port);

		ConnectionID connectionID = new ConnectionID(new InetSocketAddress(targetInstance.getServiceIp(), port), 0);
		return connectionID;
	}

	private NettyConnectionManager createConnectionManager(ConnectionID connectionID) {
		NettyConfig nettyConfig = createNettyConfig(globalConfig, connectionID.getAddress());
		NettyConnectionManager nettyConnectionManager = new NettyConnectionManager(nettyConfig);
		return nettyConnectionManager;
	}

	private TableServiceSourceInputGate createInputGate(
		String tableName,
		int partitionIndex,
		NetworkBufferPool networkBufferPool,
		ConnectionID connectionID,
		NettyConnectionManager nettyConnectionManager) throws IOException {

		ResultPartitionID partitionID =
			TableServiceUtil.tablePartitionToResultPartition(tableName, partitionIndex);

		TableServiceSourceInputGate inputGate =
			new TableServiceSourceInputGate("TableServiceSource_" + tableName,
				new IntermediateDataSetID(partitionID.getPartitionId()),
				1,
				executorService);

		inputGate.setNetworkProperties(networkBufferPool, MAX_SEGMENT_NUMBER / 3 * 2);
		BufferPool bufferPool = networkBufferPool.createBufferPool(MAX_SEGMENT_NUMBER / 3, MAX_SEGMENT_NUMBER / 3);
		inputGate.setBufferPool(bufferPool);
		RemoteInputChannel remoteInputChannel = new RemoteInputChannel(
			inputGate,
			0,
			partitionID,
			connectionID,
			nettyConnectionManager,
			0,
			0,
			new SimpleCounter()
		);

		inputGate.setInputChannel(
			new IntermediateResultPartitionID(
				partitionID.getPartitionId().getLowerPart(),
				partitionID.getPartitionId().getUpperPart()),
			remoteInputChannel
		);

		return inputGate;
	}

	public NettyConfig createNettyConfig(Configuration configuration, InetSocketAddress address) {
		return new NettyConfig(
			address.getAddress(),
			0,
			memorySizePerBufferInBytes, 1, configuration);
	}

	private static class BaseRowGenerator {

		private BaseRowSerializer<BaseRow> serializer;

		private byte[] dataBuffer;

		private byte[] headerBuffer = new byte[Integer.BYTES];

		private byte[] currentBuffer;

		private int dataBufferOffset;

		private int headerBufferOffset;

		private int currentBufferOffset;

		private int status;

		private int headerExpectBytes = Integer.BYTES;

		private int dataExpectBytes;

		private int headerReadBytes;

		private int dataReadBytes;

		private static final int EMPTY = 0;

		private static final int HEADER_HALF_READ = 1;

		private static final int HEADER_READ = 2;

		private static final int DATA_HALF_READ = 3;

		private static final int DATA_READ = 4;

		public BaseRowGenerator(BaseRowSerializer<BaseRow> serializer) {
			this.serializer = serializer;
		}

		public void setBuffer(Buffer buffer) {
			checkState(currentBuffer == null || currentBufferOffset == currentBuffer.length, "There are some unconsumed bytes");
			int bufferReadSize = buffer.readableBytes();
			currentBuffer = new byte[bufferReadSize];
			buffer.asByteBuf().readBytes(currentBuffer);
			currentBufferOffset = 0;
			buffer.recycleBuffer();
		}

		BaseRow getNext() {
			BaseRow baseRow = BytesUtil.deSerialize(dataBuffer, dataBuffer.length, serializer);
			reset();
			return baseRow;
		}

		boolean hasNext() {
			tryChangeStatus();
			return status == DATA_READ;
		}

		private void reset() {
			dataBuffer = null;
			dataBufferOffset = 0;
			dataExpectBytes = 0;
			headerBufferOffset = 0;
			dataExpectBytes = 0;
			dataReadBytes = 0;
			headerReadBytes = 0;
			status = EMPTY;
		}

		private void tryChangeStatus() {
			while (changeStatus()) {}
		}

		private int remaining() {
			return currentBuffer == null ? 0 : currentBuffer.length - currentBufferOffset;
		}

		private boolean changeStatus() {
			boolean statusChanged = false;
			switch (status) {
				case EMPTY:
					if (remaining() > 0) {
						int readCount = Math.min(remaining(), headerExpectBytes);
						System.arraycopy(currentBuffer, currentBufferOffset, headerBuffer, headerBufferOffset, readCount);
						currentBufferOffset += readCount;
						headerBufferOffset += readCount;
						headerReadBytes += readCount;
						status = headerReadBytes == headerExpectBytes ? HEADER_READ : HEADER_HALF_READ;
						statusChanged = true;
					}
					break;
				case HEADER_HALF_READ:
					if (remaining() > 0) {
						int readCount = Math.min(remaining(), headerExpectBytes - headerReadBytes);
						System.arraycopy(currentBuffer, currentBufferOffset, headerBuffer, headerBufferOffset, readCount);
						currentBufferOffset += readCount;
						headerBufferOffset += readCount;
						headerReadBytes += readCount;
						status = headerReadBytes == headerExpectBytes ? HEADER_READ : HEADER_HALF_READ;
						statusChanged = status == HEADER_READ;
					}
					break;
				case HEADER_READ:
					dataExpectBytes = BytesUtil.bytesToInt(headerBuffer);
					dataBuffer = new byte[dataExpectBytes];
					if (remaining() > 0) {
						int readCount = Math.min(remaining(), dataExpectBytes);
						System.arraycopy(currentBuffer, currentBufferOffset, dataBuffer, dataBufferOffset, readCount);
						currentBufferOffset += readCount;
						dataBufferOffset += readCount;
						dataReadBytes += readCount;
						status = dataReadBytes == dataExpectBytes ? DATA_READ : DATA_HALF_READ;
						statusChanged = true;
					}
					break;
				case DATA_HALF_READ:
					if (remaining() > 0) {
						int readCount = Math.min(remaining(), dataExpectBytes);
						System.arraycopy(currentBuffer, currentBufferOffset, dataBuffer, dataBufferOffset, readCount);
						currentBufferOffset += readCount;
						dataBufferOffset += readCount;
						dataReadBytes += readCount;
						status = dataReadBytes == dataExpectBytes ? DATA_READ : DATA_HALF_READ;
						statusChanged = status == DATA_READ;
					}
					break;
				case DATA_READ:
					break;
				default:
					throw new TableServiceException(new RuntimeException("Unsupported status: " + status));
			}
			return statusChanged;
		}
	}
}
