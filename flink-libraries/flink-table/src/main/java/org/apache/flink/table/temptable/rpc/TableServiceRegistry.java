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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Inet4Address;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This server is responsible for maintaining TableService address and port.
 */
public class TableServiceRegistry implements LifeCycleAware {

	private static final Logger LOG = LoggerFactory.getLogger(TableServiceRegistry.class);

	private String ip;

	private ExecutorService executorService;

	private TableServiceRegistryServer server;

	private final int tableServiceInstanceNum;

	private Map<Integer, ServiceInstance> registedServices;

	public String getIp() {
		return ip;
	}

	public int getPort() {
		return server.getPort();
	}

	public TableServiceRegistry(int tableServiceInstanceNum) {
		this.tableServiceInstanceNum = tableServiceInstanceNum;
	}

	public Map<Integer, ServiceInstance> getRegistedServices() {
		return new HashMap<>(registedServices);
	}

	@Override
	public void open(Configuration config) throws Exception {
		registedServices = new ConcurrentHashMap<>();
		ip = Inet4Address.getLocalHost().getHostAddress();
		server = new TableServiceRegistryServer();
		executorService = Executors.newSingleThreadExecutor();
		executorService.submit(server);
		LOG.info("TableServiceRegistry opened.");
	}

	@Override
	public void close() throws Exception {
		server.stop();
		executorService.shutdown();
		LOG.info("TableServiceRegistry closed.");
	}

	public boolean isTableServiceReady() {
		if (server.hasError()) {
			throw new RuntimeException(server.getError());
		}
		LOG.info("check table service ready, expect: " + tableServiceInstanceNum + ", found: " + registedServices.size());
		return registedServices.size() == tableServiceInstanceNum;
	}

	private class TableServiceRegistryServer implements Runnable {

		private volatile int port = -1;

		private ServerSocket serverSocket;

		private Throwable error;

		private volatile boolean running = true;

		public int getPort() {
			return port;
		}

		@Override
		public void run() {
			LOG.info("TableServiceRegistryServer begin running");

			try {
				serverSocket = new ServerSocket(0);
			} catch (Exception e) {
				error = e;
				LOG.error(e.getMessage(), e);
				return;
			}

			port = serverSocket.getLocalPort();

			while (running) {
				Socket client;
				ObjectInputStream objectInputStream;
				try {
					client = serverSocket.accept();
					objectInputStream = new ObjectInputStream(
						new BufferedInputStream(client.getInputStream()));
					ServiceInstance serviceInstance = (ServiceInstance) objectInputStream.readObject();
					if (serviceInstance != null) {
						registedServices.put(
							Integer.valueOf(serviceInstance.getInstanceId()),
							serviceInstance);
					}
				} catch (IOException ie) {
					if (running) {
						error = ie;
						LOG.error(ie.getMessage(), ie);
					} else {
						// this indicates the server socket already closed.
					}
				} catch (ClassNotFoundException e) {
					error = e;
					LOG.error(e.getMessage(), e);
				}

			}
			LOG.info("TableServiceRegistryServer end running.");
		}

		public void stop() {

			running = false;

			if (serverSocket != null) {
				try {
					serverSocket.close();
				} catch (IOException e) {
					LOG.error(e.getMessage(), e);
				}
			}
		}

		public boolean hasError() {
			return error != null;
		}

		public Throwable getError() {
			return error;
		}

		public boolean isRunning() {
			return running;
		}
	}

}
