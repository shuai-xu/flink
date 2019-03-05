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

import org.apache.flink.api.common.DriverProgram;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import org.apache.commons.io.FilenameUtils;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.stream.Collectors;

/**
 * DriverEntry is the main entry point of driver mode. It manages package program user provided and provides a driver
 * environment to run user application by preparing a flink source function in which will call the main method of
 * user application.
 */
public class DriverEntry implements DriverProgram {

	private static PackagedProgram packagedProgram;

	private static String clusterIp;

	private static int clusterPort = -1;

	private static Configuration configuration;

	private static String driveName;

	@Override
	public void setParameter(Object parameter) {
		if (parameter == null || parameter instanceof PackagedProgram) {
			packagedProgram = (PackagedProgram) parameter;
		}
	}

	@Override
	public void setClusterInfo(String ip, int port) {
		clusterIp = ip;
		clusterPort = port;
	}

	@Override
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	public static void main(String[] args) throws Exception {
		driveName = "FlinkDriverJob" + "_" + System.currentTimeMillis();
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		manageJobBlobs(env, packagedProgram.getAllLibraries());
		manageJobBlobs(env, packagedProgram.getFiles());
		env.addSource(createSourceFunction()).addSink(new SinkFunction<Byte>() {
			@Override
			public void invoke(Byte value, Context context) throws Exception {

			}
		});
		env.submit(driveName);
	}

	private static SourceFunction<Byte> createSourceFunction() {
		DriverSourceFunction driverSourceFunction = new DriverSourceFunction(
			clusterIp,
			clusterPort,
			driveName,
			packagedProgram.getDriverClass(),
			packagedProgram.getArguments(),
			packagedProgram.getAllLibraries(),
			packagedProgram.getFiles().stream().map(uri -> {
				try {
					return uri.toURL();
				} catch (MalformedURLException e) {
					e.printStackTrace();
				}
				return null;
			}).collect(Collectors.toList()),
			configuration);
		return driverSourceFunction;
	}

	private static void manageJobBlobs(StreamExecutionEnvironment env, List<? extends Object> jobBlobs) {
		for (Object blod : jobBlobs) {
			URL url = null;
			if (blod instanceof URI) {
				try {
					url = ((URI) blod).toURL();
				} catch (MalformedURLException e) {
					e.printStackTrace();
				}
			} else if (blod instanceof URL) {
				url = (URL) blod;
			}
			if (url != null) {
				String fileName = FilenameUtils.getName(url.getPath());
				env.registerCachedFile(url.toString(), fileName);
			}
		}
	}

}
