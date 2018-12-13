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
import org.apache.flink.service.ServiceRegistry;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.temptable.FlinkTableService;
import org.apache.flink.table.temptable.util.BytesUtil;
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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Built-in implementation of Table Service Client.
 * Data will be serialized in form of the {@link BinaryRow}.
 */
public class TableServiceClient implements LifeCycleAware {

	private Logger logger = LoggerFactory.getLogger(TableServiceClient.class);

	private List<ServiceInstance> serviceInfoList;

	private String lastTableName = null;

	private int lastPartitionIndex = -1;

	private int lastTablePartionOffset = 0;

	private Bootstrap bootstrap;

	private ChannelFuture channelFuture;

	private EventLoopGroup eventLoopGroup;

	private TableServiceClientHandler clientHandler;

	private volatile TableServiceBuffer writeBuffer;

	private volatile TableServiceBuffer readBuffer;

	private static final int BUFFER_READ_SIZE = 1024;

	private ServiceRegistry registry;

	public final ServiceRegistry getRegistry() {
		return registry;
	}

	List<Integer> getPartitions(String tableName) {
		List<ServiceInstance> serviceInfoList = getRegistry().getAllInstances(FlinkTableService.class.getSimpleName());
		List<Integer> partitions = new ArrayList<>();
		if (serviceInfoList != null) {
			for (ServiceInstance serviceInfo : serviceInfoList) {
				Bootstrap bootstrap = new Bootstrap();
				TableServiceClientHandler clientHandler = new TableServiceClientHandler();
				ChannelFuture channelFuture = null;
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
								socketChannel.pipeline().addLast(clientHandler);
							}
						});
					channelFuture = bootstrap.connect(serviceInfo.getServiceIp(), serviceInfo.getServicePort()).sync();
					List<Integer> subPartitions = clientHandler.getPartitions(tableName);
					if (subPartitions != null && !subPartitions.isEmpty()) {
						partitions.addAll(subPartitions);
					}
				} catch (Exception e) {
					logger.error(e.getMessage(), e);
					throw new RuntimeException(e);
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
		return partitions;
	}

	void write(String tableName, int partitionIndex, BaseRow row, BaseRowSerializer baseRowSerializer) {
		ensureConnected(tableName, partitionIndex);
		if (writeBuffer == null) {
			writeBuffer = new TableServiceBuffer(tableName, partitionIndex, 32000);
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
			try {
				clientHandler.write(tableName, partitionIndex, writeBytes);
				clientHandler.write(tableName, partitionIndex, serialized);
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
				throw new RuntimeException(e);
			}
		}
	}

	BaseRow readNext(String tableName, int partitionIndex, BaseRowSerializer baseRowSerializer) {
		ensureConnected(tableName, partitionIndex);
		if (readBuffer == null) {
			readBuffer = new TableServiceBuffer(tableName, partitionIndex, BUFFER_READ_SIZE);
		}

		byte[] buffer = readFromBuffer(Integer.BYTES);

		if (buffer == null) {
			return null;
		}

		int sizeInBytes = BytesUtil.bytesToInt(buffer);

		buffer = readFromBuffer(sizeInBytes);

		if (buffer == null) {
			return null;
		}

		return BytesUtil.deSerialize(buffer, sizeInBytes, baseRowSerializer);
	}

	@Override
	public void open(Configuration parameters) {

		if (getRegistry() != null) {
			getRegistry().open(parameters);
		}

		eventLoopGroup = new NioEventLoopGroup();
	}

	@Override
	public void close() {

		if (writeBuffer != null) {
			writeBuffer.getByteBuffer().flip();
			if (writeBuffer.getByteBuffer().hasRemaining()) {
				ensureConnected(writeBuffer.getTableName(), writeBuffer.getPartitionIndex());
				byte[] writeBytes = new byte[writeBuffer.getByteBuffer().remaining()];
				writeBuffer.getByteBuffer().get(writeBytes);
				try {
					clientHandler.write(writeBuffer.getTableName(), writeBuffer.getPartitionIndex(), writeBytes);
				} catch (Exception e) {
					logger.error(e.getMessage(), e);
					throw new RuntimeException(e);
				}
				writeBuffer.getByteBuffer().clear();
			}
		}

		if (getRegistry() != null) {
			getRegistry().close();
		}
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

	private void ensureConnected(String tableName, int partitionIndex) {
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
		serviceInfoList = getRegistry().getAllInstances(FlinkTableService.class.getSimpleName());

		if (serviceInfoList == null || serviceInfoList.isEmpty()) {
			logger.error("fetch serviceInfoList fail");
			throw new RuntimeException("serviceInfoList is empty");
		}

		Collections.sort(serviceInfoList, Comparator.comparing(ServiceInstance::getInstanceId));

		int hashCode = Objects.hash(tableName, partitionIndex);

		int pickIndex = (hashCode % serviceInfoList.size());
		if (pickIndex < 0) {
			pickIndex += serviceInfoList.size();
		}

		ServiceInstance pickedServiceInfo = serviceInfoList.get(pickIndex);

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
			throw new RuntimeException("build connection fail", e);
		}

		logger.info("build client end");
	}

	public boolean isReady() {

		List<ServiceInstance> serviceInfoList = getRegistry().getAllInstances(FlinkTableService.class.getSimpleName());

		if (serviceInfoList == null || serviceInfoList.isEmpty()) {
			return false;
		}

		String headInstanceId = serviceInfoList.get(0).getInstanceId();

		int totalInstanceNumber = Integer.valueOf(headInstanceId.split("_")[1]);

		if (serviceInfoList.size() != totalInstanceNumber) {
			logger.info("expected " + totalInstanceNumber + " instance, but only " + serviceInfoList.size() + " found");
			return false;
		}

		boolean misMatch = serviceInfoList.stream()
			.anyMatch(serviceInfo -> !serviceInfo.getInstanceId().split("_")[1].equals(totalInstanceNumber + ""));

		if (misMatch) {
			logger.info("mismatch total number of instance");
			return false;
		}

		boolean countMatch = serviceInfoList.stream()
			.map(serviceInfo -> Integer.valueOf(serviceInfo.getInstanceId().split("_")[0]))
			.collect(Collectors.toSet())
			.size() == totalInstanceNumber;

		if (!countMatch) {
			logger.info("some instance is not ready");
			return false;
		}

		return true;
	}

	private byte[] readFromBuffer(int readCount) {

		readBuffer.getByteBuffer().flip();
		byte[] bytes;
		int remaining = readBuffer.getByteBuffer().remaining();
		if (remaining >= readCount) {
			bytes = new byte[readCount];
			readBuffer.getByteBuffer().get(bytes);
			readBuffer.getByteBuffer().compact();
		} else {
			bytes = new byte[readCount];
			readBuffer.getByteBuffer().get(bytes, 0, remaining);
			int count = remaining;
			if (count < readCount) {
				boolean success = fillReadBufferFromClient();
				if (success) {
					byte[] bufferRead = readFromBuffer(readCount - count);
					if (bufferRead != null) {
						System.arraycopy(bufferRead, 0, bytes, count, readCount - count);
					} else {
						return null;
					}
				} else {
					return null;
				}
			}
		}
		return bytes;
	}

	private boolean fillReadBufferFromClient() {
		byte[] buffer;
		try {
			buffer = clientHandler.read(lastTableName, lastPartitionIndex, lastTablePartionOffset, BUFFER_READ_SIZE);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new RuntimeException(e);
		}

		if (buffer != null && buffer.length > 0) {
			lastTablePartionOffset += buffer.length;
			readBuffer.getByteBuffer().clear();
			readBuffer.getByteBuffer().put(buffer);
			return true;
		}

		return false;
	}

}
