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

import static com.alibaba.blink.launcher.CommandLineOptionsParser.JOBNAME_OPTION;

/**
 * Command line options for the 'run' action.
 */
public class RunOptions extends CommandLineOptions {
	private final String jobName;

	public RunOptions(CommandLine line) throws CliArgsException {
		super(line);
		this.jobName = line.hasOption(JOBNAME_OPTION.getOpt()) ?
			line.getOptionValue(JOBNAME_OPTION.getOpt()) : null;
	}

	public String getJobName() {
		return jobName;
	}
}
