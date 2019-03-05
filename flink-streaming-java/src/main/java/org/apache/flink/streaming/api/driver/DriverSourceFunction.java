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

package org.apache.flink.streaming.api.driver;

import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.DriverConfigConstants;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * DriverSourceFunction.
 * As a flink job, Driver will call the main method of user packaged program to submit user jobs via a flink Source.
 * before calling the main method, Driver needs to prepare a DriverStreamEnvironment, then gets user blobs from distributed
 * cache and set into prepared environment with other managed configuration, cluster connection information, etc.
 */
public class DriverSourceFunction extends RichSourceFunction<Byte> {

	private final String clusterIP;

	private final int clusterPort;

	private final Class<?> userMainClass;

	private DriverStreamEnvironment driverStreamEnvironment;

	private final List<URL> libJarURLs;

	private final List<URL> externalFileURLs;

	private final Configuration configuration;

	private final String driverName;

	private final String[] args;

	public DriverSourceFunction(
		String clusterIP,
		int clusterPort,
		String driverName,
		Class<?> userMainClass,
		String[] args,
		List<URL> libjars,
		List<URL> externalFiles,
		Configuration configuration) {
		this.clusterIP = clusterIP;
		this.clusterPort = clusterPort;
		this.driverName = driverName;
		this.userMainClass = userMainClass;
		this.args = args;
		this.libJarURLs = libjars;
		this.externalFileURLs = externalFiles;
		this.configuration = configuration;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		DistributedCache distributedCache = getRuntimeContext().getDistributedCache();
		List<File> jarFiles = new ArrayList<>(libJarURLs.size());
		for (URL libJarURL : libJarURLs) {
			String fileName = FilenameUtils.getName(libJarURL.getPath());
			File file = distributedCache.getFile(fileName);
			if (file != null) {
				jarFiles.add(file);
			}
		}

		List<URL> cachedExternalFileURLs = new ArrayList<>(externalFileURLs.size());
		for (URL externalFileURL : externalFileURLs) {
			String fileName = FilenameUtils.getName(externalFileURL.getPath());
			File file = distributedCache.getFile(fileName);
			if (file != null) {
				cachedExternalFileURLs.add(file.toURI().toURL());
			}
		}

		List<String> jarFilePaths = jarFiles.stream().map(file -> file.getPath()).collect(
			Collectors.toList());
		driverStreamEnvironment = new DriverStreamEnvironment(
			clusterIP,
			clusterPort,
			driverName,
			jarFilePaths.toArray(new String[0]),
			cachedExternalFileURLs.toArray(new URL[0]),
			configuration
		);
		driverStreamEnvironment.setParallelism(configuration.getInteger(DriverConfigConstants.FLINK_DRIVER_PARALLELISM,
			configuration.getInteger(CoreOptions.DEFAULT_PARALLELISM)));
		driverStreamEnvironment.setAsContext();

	}

	@Override
	public void run(SourceContext<Byte> ctx) throws Exception {
		PackagedProgram.callMainMethod(userMainClass, args);
	}

	@Override
	public void close() throws Exception {
		driverStreamEnvironment.resetContextEnvironments();
	}

	@Override
	public void cancel() {

	}
}
