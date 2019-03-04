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

package org.apache.flink.table.temptable;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.partition.external.ExternalBlockShuffleService;
import org.apache.flink.runtime.io.network.partition.external.ExternalBlockShuffleServiceOptions;
import org.apache.flink.service.ServiceInstance;
import org.apache.flink.service.UserDefinedService;
import org.apache.flink.table.temptable.rpc.TableServiceServer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Inet4Address;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * The built-in implementation of TableService.
 */
public class FlinkTableService extends UserDefinedService {

	private static final Logger logger = LoggerFactory.getLogger(FlinkTableService.class);

	private TableServiceServer server;

	private String serviceIP;

	private int servicePort = -1;

	private TableServiceImpl manager;

	private String tableServiceId;

	private ExternalBlockShuffleService shuffleService;

	private String registryIP;

	private int registryPort;

	@Override
	public void open(Configuration config) throws Exception {
		tableServiceId = config.getString(TableServiceOptions.TABLE_SERVICE_ID);
		registryIP = config.getString(TableServiceOptions.TABLE_SERVICE_REGISTRY_ADDRESS);
		registryPort = config.getInteger(TableServiceOptions.TABLE_SERVICE_REGISTRY_PORT);
		config.setString(
			ExternalBlockShuffleServiceOptions.LOCAL_RESULT_PARTITION_RESOLVER_CLASS,
			DefaultExternalResultPartitionResolver.class.getCanonicalName());
		String rootPath = config.getString(TableServiceOptions.TABLE_SERVICE_STORAGE_ROOT_PATH, System.getProperty("user.dir"));
		String storagePath = rootPath + File.separator + "tableservice_" + tableServiceId + File.separator;
		config.setString(ExternalBlockShuffleServiceOptions.LOCAL_DIRS, storagePath);
		logger.info("start table service with id:" + tableServiceId);
		manager = new TableServiceImpl(getServiceContext());
		manager.open(config);
		setUpServer();
		addInstance();
		shuffleService = new ExternalBlockShuffleService(config);
	}

	@Override
	public void close() throws Exception {
		if (server != null) {
			server.stop();
		}
		if (shuffleService != null) {
			shuffleService.stop();
		}

		manager.close();
	}

	private void setUpServer() {
		logger.info("begin set up table service.");

		try {
			serviceIP = Inet4Address.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			logger.error(e.getMessage(), e);
			throw new RuntimeException(e);
		}

		server = new TableServiceServer();
		server.setTableService(manager);

		try {
			servicePort = server.bind();
		} catch (Exception e) {
			logger.error("start server fail.", e);
			throw new RuntimeException(e);
		}

		logger.info("end set up table service.");
	}

	private void addInstance() {
		int instanceId = getServiceContext().getIndexOfCurrentInstance();
		ServiceInstance serviceInstance = new ServiceInstance(instanceId, serviceIP, servicePort);
		Socket socket = null;
		ObjectOutputStream outputStream = null;
		try {
			socket = new Socket(registryIP, registryPort);
			outputStream = new ObjectOutputStream(new BufferedOutputStream(socket.getOutputStream()));
			outputStream.writeObject(serviceInstance);
			outputStream.flush();
		} catch (IOException e) {
			logger.error("add instance fails", e);
		} finally {
			if (outputStream != null) {
				try {
					outputStream.close();
				} catch (IOException e) {
					logger.error(e.getMessage(), e);
					throw new RuntimeException(e);
				}
			}
			if (socket != null) {
				try {
					socket.close();
				} catch (IOException e) {
					logger.error(e.getMessage(), e);
					throw new RuntimeException(e);
				}
			}
		}
		logger.info("add Instance success");
	}

	@Override
	public void start() {

		try {
			shuffleService.start();
		} catch (IOException e) {
			logger.error("error occurs while start shuffle service: " + e);
			throw new RuntimeException("ShuffleService start fails");
		}

		logger.info("TableService begin serving");
		try {
			server.start();
		} catch (Exception e) {
			logger.error("error occurs while serving: " + e);
		}
		logger.error("TableService end serving");
		throw new RuntimeException("TableService stops.");
	}
}
