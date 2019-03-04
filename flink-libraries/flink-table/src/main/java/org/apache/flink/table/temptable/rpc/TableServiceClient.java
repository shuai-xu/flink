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

package org.apache.flink.table.temptable.rpc;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.service.LifeCycleAware;
import org.apache.flink.service.ServiceInstance;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.temptable.TableServiceException;
import org.apache.flink.table.temptable.TableServiceOptions;
import org.apache.flink.table.temptable.util.BytesUtil;
import org.apache.flink.table.temptable.util.TableServiceUtil;
import org.apache.flink.table.typeutils.BaseRowSerializer;

import org.apache.flink.shaded.netty4.io.netty.bootstrap.Bootstrap;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelOption;
import org.apache.flink.shaded.netty4.io.netty.channel.EventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Built-in implementation of Table Service Client.
 * Data will be serialized in form of the {@link BinaryRow}.
 */
public class TableServiceClient implements LifeCycleAware {

	private Logger logger = LoggerFactory.getLogger(TableServiceClient.class);

	private String lastTableName = null;

	private int lastPartitionIndex = -1;

	private Bootstrap bootstrap;

	private ChannelFuture channelFuture;

	private EventLoopGroup eventLoopGroup;

	private TableServiceClientHandler clientHandler;

	private volatile TableServiceBuffer writeBuffer;

	private int writeBufferSize;

	private String tableServiceId;

	public Map<Integer, ServiceInstance> getServiceInstanceMap() {
		return serviceInstanceMap;
	}

	private Map<Integer, ServiceInstance> serviceInstanceMap;

	public List<Integer> getPartitions(String tableName) {
		ServiceInstance serviceInstance = serviceInstanceMap.get(0);
		List<Integer> partitions = new ArrayList<>();
		if (serviceInstance != null) {
			TableServiceClientHandler clientHandler = new TableServiceClientHandler();
			Bootstrap bootstrap = getTempBootstrap(clientHandler);
			ChannelFuture channelFuture = null;
			try {
				channelFuture = bootstrap.connect(serviceInstance.getServiceIp(), serviceInstance.getServicePort()).sync();
				List<Integer> subPartitions = clientHandler.getPartitions(tableName);
				if (subPartitions != null && !subPartitions.isEmpty()) {
					partitions.addAll(subPartitions);
				}
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
				throw new TableServiceException(new RuntimeException(e.getMessage(), e));
			} finally {
				if (channelFuture != null) {
					try {
						channelFuture.channel().close().sync();
					} catch (InterruptedException e) {
						logger.error(e.getMessage(), e);
					}
				}
			}
		}
		return partitions;
	}

	public void unregisterPartitions(String tableName) {
		ServiceInstance serviceInstance = serviceInstanceMap.get(0);
		if (serviceInstance != null) {
			TableServiceClientHandler clientHandler = new TableServiceClientHandler();
			Bootstrap bootstrap = getTempBootstrap(clientHandler);
			ChannelFuture channelFuture = null;
			try {
				channelFuture = bootstrap.connect(serviceInstance.getServiceIp(), serviceInstance.getServicePort()).sync();
				clientHandler.unregisterPartitions(tableName);
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
				throw new TableServiceException(new RuntimeException(e.getMessage(), e));
			} finally {
				if (channelFuture != null) {
					try {
						channelFuture.channel().close().sync();
					} catch (InterruptedException e) {
						logger.error(e.getMessage(), e);
					}
				}
			}
		}
	}

	private Bootstrap getTempBootstrap(TableServiceClientHandler handler) {
		Bootstrap bootstrap = new Bootstrap();
		try {
			bootstrap.group(eventLoopGroup)
				.channel(NioSocketChannel.class)
				.option(ChannelOption.TCP_NODELAY, true)
				.handler(new ChannelInitializer<SocketChannel>() {
					@Override
					protected void initChannel(SocketChannel socketChannel) throws Exception {
						socketChannel.pipeline().addLast(new LengthFieldBasedFrameDecoder(
							65536,
							0,
							Integer.BYTES,
							-Integer.BYTES,
							Integer.BYTES)
						);
						socketChannel.pipeline().addLast(handler);
					}
				});
			return bootstrap;
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new TableServiceException(new RuntimeException(e.getMessage(), e));
		}
	}

	public void write(String tableName, int partitionIndex, BaseRow row, BaseRowSerializer baseRowSerializer) throws Exception {
		ensureConnected(tableName, partitionIndex);
		if (writeBuffer == null) {
			writeBuffer = new TableServiceBuffer(tableName, partitionIndex, writeBufferSize);
		}
		byte[] serialized = BytesUtil.serialize(row, baseRowSerializer);
		if (writeBuffer.getByteBuffer().remaining() >= serialized.length) {
			writeBuffer.getByteBuffer().put(serialized);
		} else {
			writeBuffer.getByteBuffer().flip();
			int remaining = writeBuffer.getByteBuffer().remaining();
			byte[] writeBytes = new byte[remaining];
			writeBuffer.getByteBuffer().get(writeBytes);
			writeBuffer.getByteBuffer().clear();
			clientHandler.write(tableName, partitionIndex, writeBytes);
			clientHandler.write(tableName, partitionIndex, serialized);
		}
	}

	public void finish(String tableName, int partitionIndex) throws Exception {
		ensureConnected(tableName, partitionIndex);
		clientHandler.finishPartition(tableName, partitionIndex);
	}

	public void initializePartition(String tableName, int partitionIndex) throws Exception {
		ensureConnected(tableName, partitionIndex);
		clientHandler.initializePartition(tableName, partitionIndex);
	}

	public void deletePartition(String tableName, int partitionIndex) throws Exception {
		ensureConnected(tableName, partitionIndex);
		clientHandler.deletePartition(tableName, partitionIndex);
	}

	public void registerPartition(String tableName, int partitionIndex) throws Exception {
		ServiceInstance serviceInstance = serviceInstanceMap.get(0);
		if (serviceInstance != null) {
			TableServiceClientHandler clientHandler = new TableServiceClientHandler();
			Bootstrap bootstrap = getTempBootstrap(clientHandler);
			ChannelFuture channelFuture = null;
			try {
				channelFuture = bootstrap.connect(serviceInstance.getServiceIp(), serviceInstance.getServicePort()).sync();
				clientHandler.registerPartition(tableName, partitionIndex);
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
				throw new TableServiceException(new RuntimeException(e.getMessage(), e));
			} finally {
				if (channelFuture != null) {
					try {
						channelFuture.channel().close().sync();
					} catch (InterruptedException e) {
						logger.error(e.getMessage(), e);
					}
				}
			}
		}
	}

	@Override
	public void open(Configuration config) throws Exception {

		tableServiceId = config.getString(TableServiceOptions.TABLE_SERVICE_ID);

		logger.info("TableServiceClient open with tableServiceId = " + tableServiceId);

		writeBufferSize = config.getInteger(TableServiceOptions.TABLE_SERVICE_CLIENT_WRITE_BUFFER_SIZE);

		eventLoopGroup = new NioEventLoopGroup();

		serviceInstanceMap = new HashMap<>();
		serviceInstanceMap.putAll(TableServiceUtil.buildTableServiceInstance(config));
	}

	@Override
	public void close() throws Exception {

		flush();

		if (channelFuture != null) {
			try {
				channelFuture.channel().close().sync();
			} catch (InterruptedException e) {
				logger.error(e.getMessage(), e);
			}
		}
		if (eventLoopGroup != null && !eventLoopGroup.isShutdown()) {
			eventLoopGroup.shutdownGracefully();
		}
	}

	public void flush() throws Exception {
		if (writeBuffer != null) {
			writeBuffer.getByteBuffer().flip();
			if (writeBuffer.getByteBuffer().hasRemaining()) {
				ensureConnected(writeBuffer.getTableName(), writeBuffer.getPartitionIndex());
				byte[] writeBytes = new byte[writeBuffer.getByteBuffer().remaining()];
				writeBuffer.getByteBuffer().get(writeBytes);
				clientHandler.write(writeBuffer.getTableName(), writeBuffer.getPartitionIndex(), writeBytes);
				writeBuffer.getByteBuffer().clear();
			}
		}
	}

	private void ensureConnected(String tableName, int partitionIndex) throws Exception {
		if (tableName.equals(lastTableName) && partitionIndex == lastPartitionIndex) {
			return;
		}

		if (bootstrap != null) {
			try {
				channelFuture.channel().close().sync();
			} catch (InterruptedException e) {
				logger.error(e.getMessage(), e);
			} finally {
				bootstrap = null;
				channelFuture = null;
			}
		}

		lastTableName = tableName;
		lastPartitionIndex = partitionIndex;

		if (serviceInstanceMap == null || serviceInstanceMap.isEmpty()) {
			throw new TableServiceException(new RuntimeException("serviceInstanceMap is empty"));
		}

		int pickIndex = TableServiceUtil.tablePartitionToIndex(tableName, partitionIndex, serviceInstanceMap.size());

		ServiceInstance pickedServiceInfo = serviceInstanceMap.get(pickIndex);
		if (pickedServiceInfo == null) {
			throw new TableServiceException(new RuntimeException("serviceInstanceMap does not contains service instance with instanceId = " + pickIndex));
		}

		logger.info("build client with ip = " + pickedServiceInfo.getServiceIp() + ", port = " + pickedServiceInfo.getServicePort());

		bootstrap = new Bootstrap();

		clientHandler = new TableServiceClientHandler();
		bootstrap.group(eventLoopGroup)
			.channel(NioSocketChannel.class)
			.option(ChannelOption.TCP_NODELAY, true)
			.handler(new ChannelInitializer<SocketChannel>() {
				@Override
				protected void initChannel(SocketChannel socketChannel) throws Exception {
					socketChannel.pipeline().addLast(new LengthFieldBasedFrameDecoder(
						65536,
						0,
						Integer.BYTES,
						-Integer.BYTES,
						Integer.BYTES)
					);
					socketChannel.pipeline().addLast(clientHandler);
				}
			});

		try {
			channelFuture = bootstrap.connect(pickedServiceInfo.getServiceIp(), pickedServiceInfo.getServicePort()).sync();
		} catch (InterruptedException e) {
			logger.error(e.getMessage(), e);
			throw new TableServiceException(e);
		}

		logger.info("build client end");
	}

}
