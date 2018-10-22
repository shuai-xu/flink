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

package com.alibaba.blink.launcher;

import org.apache.flink.client.cli.CliArgsException;

import org.apache.commons.cli.CommandLine;

/**
 * Base class for all options parsed from the command line.
 * Contains options for printing help and the JobManager address.
 */
public abstract class CommandLineOptions {
	private final CommandLine commandLine;
	private final boolean printHelp;
	private final String jobConf;
	private final boolean stream;
	private final boolean batch;
	private final String jarFile;
	private final String libJars;
	private final String clazz;

	protected CommandLineOptions(CommandLine line) throws CliArgsException {
		this.commandLine = line;
		this.printHelp = line.hasOption(CommandLineOptionsParser.HELP_OPTION.getOpt());
		this.jobConf = line.hasOption(CommandLineOptionsParser.JOBCONF_OPTION.getOpt()) ?
			line.getOptionValue(CommandLineOptionsParser.JOBCONF_OPTION.getOpt()) : null;
		this.stream = line.hasOption(CommandLineOptionsParser.STREAM_OPTION.getOpt());
		this.batch = line.hasOption(CommandLineOptionsParser.BATCH_OPTION.getOpt());
		this.jarFile = line.hasOption(CommandLineOptionsParser.JAR_OPTION.getOpt()) ?
			line.getOptionValue(CommandLineOptionsParser.JAR_OPTION.getOpt()) : null;
		this.libJars = line.hasOption(CommandLineOptionsParser.LIBJARS_OPTION.getOpt()) ?
			line.getOptionValue(CommandLineOptionsParser.LIBJARS_OPTION.getOpt()) : null;
		this.clazz = line.hasOption(CommandLineOptionsParser.CLASS_OPTION.getOpt()) ?
			line.getOptionValue(CommandLineOptionsParser.CLASS_OPTION.getOpt()) : null;
	}

	public CommandLine getCommandLine() {
		return commandLine;
	}

	public boolean isPrintHelp() {
		return printHelp;
	}

	public String getJobConf() {
		return jobConf;
	}

	public boolean isStream() {
		return stream;
	}

	public boolean isBatch() {
		return batch;
	}

	public String getJarFile() {
		return jarFile;
	}

	public String getLibJars() {
		return libJars;
	}

	public String getClazz() {
		return clazz;
	}
}
