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

package org.apache.flink.table.client.gateway.local;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.client.cli.CliArgsException;
import org.apache.flink.client.cli.CustomCommandLine;
import org.apache.flink.client.cli.RunOptions;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.externalcatalog.hive.HiveExternalCatalog;
import org.apache.flink.optimizer.plan.FlinkPlan;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.ExternalCatalog;
import org.apache.flink.table.catalog.InMemoryExternalCatalog;
import org.apache.flink.table.client.cli.SingleJobMode;
import org.apache.flink.table.client.config.Deployment;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.config.Execution;
import org.apache.flink.table.client.gateway.SessionContext;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.util.FlinkException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hive.conf.HiveConf;

import java.net.URL;
import java.util.List;

/**
 * Context for executing table programs. This class caches everything that can be cached across
 * multiple queries as long as the session context does not change. This must be thread-safe as
 * it might be reused across different query submissions.
 *
 * @param <T> cluster id
 */
public class ExecutionContext<T> {

	private final SessionContext sessionContext;
	private final Environment mergedEnv;
	private final List<URL> dependencies;
	private final ClassLoader classLoader;
	private final Configuration flinkConfig;
	private final CommandLine commandLine;
	private final CustomCommandLine<T> activeCommandLine;
	private final RunOptions runOptions;
	private final T clusterId;
	private final ClusterSpecification clusterSpec;
	private EnvironmentInstance environmentInstance = null;
	private final boolean needAttach;
	private final boolean needShareEnv;

	private ExternalCatalog externalCatalog = null;

	public ExecutionContext(
			Environment defaultEnvironment,
			SessionContext sessionContext,
			List<URL> dependencies,
			Configuration flinkConfig,
			Options commandLineOptions,
			List<CustomCommandLine<?>> availableCommandLines,
			SingleJobMode singleJobMode) {
		this.sessionContext = sessionContext.copy(); // create internal copy because session context is mutable
		this.mergedEnv = Environment.merge(defaultEnvironment, sessionContext.getEnvironment());
		this.dependencies = dependencies;
		this.flinkConfig = flinkConfig;

		// create class loader
		classLoader = FlinkUserCodeClassLoaders.parentFirst(
			dependencies.toArray(new URL[dependencies.size()]),
			this.getClass().getClassLoader());

		// convert deployment options into command line options that describe a cluster
		commandLine = createCommandLine(mergedEnv.getDeployment(), commandLineOptions);
		activeCommandLine = findActiveCommandLine(availableCommandLines, commandLine);
		runOptions = createRunOptions(commandLine);
		clusterId = activeCommandLine.getClusterId(commandLine);
		clusterSpec = createClusterSpecification(activeCommandLine, commandLine);

		externalCatalog = createExternalCatalog(this.mergedEnv.getExecution().getExternalCatalogType());

		this.needAttach = this.needAttach(this.mergedEnv.getExecution());

		this.needShareEnv = this.needShareEnv(this.mergedEnv.getExecution(), singleJobMode);

		if (this.needShareEnv) {
			// should share environment instance
			this.environmentInstance = new EnvironmentInstance();
		}
	}

	public SessionContext getSessionContext() {
		return sessionContext;
	}

	public ClassLoader getClassLoader() {
		return classLoader;
	}

	public Environment getMergedEnvironment() {
		return mergedEnv;
	}

	public ClusterSpecification getClusterSpec() {
		return clusterSpec;
	}

	public T getClusterId() {
		return clusterId;
	}

	public boolean isNeedAttach() {
		return needAttach;
	}

	public boolean isNeedShareEnv() {
		return needShareEnv;
	}

	public ClusterDescriptor<T> createClusterDescriptor() throws Exception {
		return activeCommandLine.createClusterDescriptor(commandLine);
	}

	public EnvironmentInstance createEnvironmentInstance() {
		if (this.environmentInstance != null) {
			return this.environmentInstance;
		}

		try {
			return new EnvironmentInstance();
		} catch (Throwable t) {
			// catch everything such that a wrong environment does not affect invocations
			throw new SqlExecutionException("Could not create environment instance.", t);
		}
	}
	// --------------------------------------------------------------------------------------------

	private static CommandLine createCommandLine(Deployment deployment, Options commandLineOptions) {
		try {
			return deployment.getCommandLine(commandLineOptions);
		} catch (Exception e) {
			throw new SqlExecutionException("Invalid deployment options.", e);
		}
	}

	@SuppressWarnings("unchecked")
	private static <T> CustomCommandLine<T> findActiveCommandLine(List<CustomCommandLine<?>> availableCommandLines, CommandLine commandLine) {
		for (CustomCommandLine<?> cli : availableCommandLines) {
			if (cli.isActive(commandLine)) {
				return (CustomCommandLine<T>) cli;
			}
		}
		throw new SqlExecutionException("Could not find a matching deployment.");
	}

	private static RunOptions createRunOptions(CommandLine commandLine) {
		try {
			return new RunOptions(commandLine);
		} catch (CliArgsException e) {
			throw new SqlExecutionException("Invalid deployment run options.", e);
		}
	}

	private static ClusterSpecification createClusterSpecification(CustomCommandLine<?> activeCommandLine, CommandLine commandLine) {
		try {
			return activeCommandLine.getClusterSpecification(commandLine);
		} catch (FlinkException e) {
			throw new SqlExecutionException("Could not create cluster specification for the given deployment.", e);
		}
	}

	private boolean needAttach(Execution execution) {
		String attachMode = execution.getAttachMode();
		if (null == attachMode) {
			if (execution.isStreamingExecution()) {
				return false;
			} else if (execution.isBatchExecution()) {
				return true;
			} else {
				throw new IllegalArgumentException("illegal execution mode");
			}
		} else if ("attach".equalsIgnoreCase(attachMode)) {
			return true;
		} else {
			// detach
			return false;
		}
	}

	private boolean needShareEnv(Execution execution, SingleJobMode singleJobMode) {
		switch (singleJobMode) {
			case SINGLE:
				return true;
			case MULTIPLE:
				return false;
			case DEFAULT:
				if (execution.isStreamingExecution()) {
					return true;
				} else if (execution.isBatchExecution()) {
					return false;
				} else {
					throw new IllegalArgumentException("illegal execution mode");
				}
		}

		throw new RuntimeException("Can not determine whether to share environment");
	}

	private ExternalCatalog createExternalCatalog(String externalCatalogType) {
		if (externalCatalogType.equalsIgnoreCase("in-memory")) {
			return new InMemoryExternalCatalog(TableEnvironment.DEFAULT_SCHEMA());
		} else if (externalCatalogType.equalsIgnoreCase("hive")) {
			HiveConf hiveConf = new HiveConf();
			// TODO pass these from the configurations
			hiveConf.setBoolVar(HiveConf.ConfVars.METASTORE_SCHEMA_VERIFICATION, false);
			hiveConf.setBoolean("datanucleus.schema.autoCreateTables", true);
			hiveConf.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE, "file:///tmp/hive");
			return new HiveExternalCatalog(HiveExternalCatalog.DEFAULT, hiveConf);
		} else {
			throw new RuntimeException("No such external catalog supported: " + externalCatalogType);
		}
	}
	// --------------------------------------------------------------------------------------------

	/**
	 * {@link ExecutionEnvironment} and {@link StreamExecutionEnvironment} cannot be reused
	 * across multiple queries because they are stateful. This class abstracts execution
	 * environments and table environments.
	 */
	public class EnvironmentInstance {

		private final StreamExecutionEnvironment streamExecEnv;
		private final TableEnvironment tableEnv;

		private EnvironmentInstance() {
			// create environments
			streamExecEnv = createStreamExecutionEnvironment();
			if (mergedEnv.getExecution().isStreamingExecution()) {
				tableEnv = TableEnvironment.getTableEnvironment(streamExecEnv);
			} else if (mergedEnv.getExecution().isBatchExecution()) {
				tableEnv = TableEnvironment.getBatchTableEnvironment(streamExecEnv);
			} else {
				throw new SqlExecutionException("Unsupported execution type specified.");
			}

			Thread.currentThread().setContextClassLoader(classLoader);

			// set table config
			initTableConfig(tableEnv.getConfig());

			// TODO: use hive catalog when storing ExternalCatalogTable supported.
			tableEnv.registerExternalCatalog(
					TableEnvironment.DEFAULT_SCHEMA(),
					externalCatalog);
		}

		public StreamExecutionEnvironment getStreamExecutionEnvironment() {
			return streamExecEnv;
		}

		public TableEnvironment getTableEnvironment() {
			return tableEnv;
		}

		public ExecutionConfig getExecutionConfig() {
			return streamExecEnv.getConfig();
		}

		public JobGraph createJobGraph(String name) {
			final FlinkPlan plan = createPlan(name, flinkConfig);
			return ClusterClient.getJobGraph(
				flinkConfig,
				plan,
				dependencies,
				runOptions.getClasspaths(),
				runOptions.getSavepointRestoreSettings());
		}

		private FlinkPlan createPlan(String name, Configuration flinkConfig) {
			final StreamGraph graph = streamExecEnv.getStreamGraph();
			graph.setJobName(name);
			return graph;
		}

		private StreamExecutionEnvironment createStreamExecutionEnvironment() {
			final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setRestartStrategy(mergedEnv.getExecution().getRestartStrategy());
			env.setParallelism(mergedEnv.getExecution().getParallelism());
			env.setMaxParallelism(mergedEnv.getExecution().getMaxParallelism());
			env.setStreamTimeCharacteristic(mergedEnv.getExecution().getTimeCharacteristic());
			if (env.getStreamTimeCharacteristic() == TimeCharacteristic.EventTime) {
				env.getConfig().setAutoWatermarkInterval(mergedEnv.getExecution().getPeriodicWatermarksInterval());
			}
			return env;
		}

		private void initTableConfig(TableConfig tableConfig) {
			if (mergedEnv.getExecution().isStreamingExecution()) {
				final long minRetention = mergedEnv.getExecution().getMinStateRetention();
				final long maxRetention = mergedEnv.getExecution().getMaxStateRetention();
				tableConfig.withIdleStateRetentionTime(Time.milliseconds(minRetention), Time.milliseconds(maxRetention));
			} else if (!mergedEnv.getExecution().isBatchExecution()) {
				throw new RuntimeException("Neither Batch Execution nor Streaming Execution");
			}
		}
	}
}
