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
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * A simple command line parser (based on Apache Commons CLI) that extracts command
 * line options.
 */
class CommandLineOptionsParser {

	static final Option HELP_OPTION = new Option(
		"h",
		"help",
		false,
		"Show the help message for the CLI Frontend or the action.");
	static final Option JOBCONF_OPTION = new Option(
		"jobconf",
		true,
		"The location of generateed conf file.");
	static final Option JOBNAME_OPTION = new Option(
		"jobname",
		true,
		"Job name.");
	static final Option STREAM_OPTION = new Option(
		"stream",
		false,
		"Run streaming job.");
	static final Option BATCH_OPTION = new Option(
		"batch",
		false,
		"Run batch job.");
	static final Option JAR_OPTION = new Option(
		"jar",
		true,
		"Specify user jar file.");
	static final Option LIBJARS_OPTION = new Option(
		"libjars",
		true,
		"Specify user library jars.");
	static final Option CLASS_OPTION = new Option(
		"class",
		true,
		"Specify user class.");
	static final Option JOB_ID_OPTION = new Option(
		"jobId",
		true,
		"Specify job id.");
	static final Option KMONITOR_SERVER_OPTION = new Option(
		"kmonitor",
		true,
		"Specify kmonitor restful api server.");
	static final Option MONITOR_INTERVAL_OPTION = new Option(
		"interval",
		true,
		"Metric fetch interval");
	static final Option MONITOR_RANGE_OPTION = new Option(
		"range",
		true,
		"Time range for each time fetching metrics");
	static final Option MONITOR_OFFSET_OPTION = new Option(
		"offset",
		true,
		"Offset between current timestamp and end of the time range");

	static {
		HELP_OPTION.setRequired(false);
		JAR_OPTION.setRequired(true);
		CLASS_OPTION.setRequired(true);
		JOB_ID_OPTION.setRequired(true);
		KMONITOR_SERVER_OPTION.setRequired(true);
		JOBNAME_OPTION.setRequired(false);
	}

	private static final Options COMPILE_OPTIONS = getCompileOptions(buildGeneralOptions(new Options()));
	private static final Options INFO_OPTIONS = getInfoOptions(buildGeneralOptions(new Options()));
	private static final Options EXECUTE_OPTIONS = getExecuteOptions(buildGeneralOptions(new Options()));
	private static final Options MONITOR_OPTIONS = getMonitorOptions(buildGeneralOptions(new Options()));

	private static Options getMonitorOptions(Options options) {
		options.addOption(JOBCONF_OPTION);
		options.addOption(JOBNAME_OPTION);
		options.addOption(MONITOR_INTERVAL_OPTION);
		options.addOption(MONITOR_RANGE_OPTION);
		options.addOption(MONITOR_OFFSET_OPTION);
		return options;
	}

	private static Options getCompileOptions(Options options) {
		options.addOption(JOBCONF_OPTION);
		options.addOption(STREAM_OPTION);
		options.addOption(BATCH_OPTION);
		options.addOption(JAR_OPTION);
		options.addOption(LIBJARS_OPTION);
		options.addOption(CLASS_OPTION);
		return options;
	}

	private static Options getInfoOptions(Options options) {
		options.addOption(STREAM_OPTION);
		options.addOption(BATCH_OPTION);
		options.addOption(JAR_OPTION);
		options.addOption(LIBJARS_OPTION);
		options.addOption(CLASS_OPTION);
		return options;
	}

	private static Options getExecuteOptions(Options options) {
		options.addOption(JOBCONF_OPTION);
		options.addOption(JOBNAME_OPTION);
		options.addOption(STREAM_OPTION);
		options.addOption(BATCH_OPTION);
		options.addOption(JAR_OPTION);
		options.addOption(LIBJARS_OPTION);
		options.addOption(CLASS_OPTION);
		return options;
	}

	private static Options buildGeneralOptions(Options options) {
		options.addOption(HELP_OPTION);
		return options;
	}

	// --------------------------------------------------------------------------------------------
	//  Help
	// --------------------------------------------------------------------------------------------

	/**
	 * Prints the help for the client.
	 */
	static void printHelp() {
		System.out.println("Usage: sqlrunner ACTION [OPTIONS]");
		System.out.println("       where ACTION is one of:");
		System.out.println("  compile    Compile the program to generate a resource file");
		System.out.println("  info       Shows the optimized execution plan of the program (JSON)");
		System.out.println("  run        Run the job");
		System.out.println();
		System.out.println("Use -h to print options for each action");
		System.out.println();
	}

	/**
	 * Prints the help for the compile action.
	 */
	static void printHelpForCompile() {
		System.out.println("Action \"compile\" : compiles the program to generate a " +
			"resource file.");
		System.out.println();
		System.out.println("  Syntax: compile [OPTIONS]");
		System.out.println("  \"compile\" action options:");
		System.out.println("    <-stream|-batch>   Stream or Batch job");
		System.out.println("    -jar               User jar file");
		System.out.println("    -class             User class name which must contains a 'build' " +
			"method, please refer to the documents");
		System.out.println("    -jobconf           Conf path to generate resource file");
		System.out.println("    [-libjars]         User library jars, seprate with ','");
		System.out.println("    [-Dkey=value]      User parameters, e.g. -Dk1=v1 -Dk2=v2");
		System.out.println();
	}

	/**
	 * Prints the help for the info action.
	 */
	static void printHelpForInfo() {
		System.out.println("Action \"info\" : shows the optimized execution plan of the program (JSON).");
		System.out.println();
		System.out.println("  Syntax: info [OPTIONS]");
		System.out.println("  \"info\" action options:");
		System.out.println("    <-stream|-batch>   Stream or Batch job");
		System.out.println("    -jar               User jar file");
		System.out.println("    -class             User class name which must contains a 'build' " +
			"method, please refer to the documents");
		System.out.println("    -jobconf           Conf path to generate resource file");
		System.out.println("    [-libjars]         User library jars, seprate with ','");
		System.out.println("    [-Dkey=value]      User parameters, e.g. -Dk1=v1 -Dk2=v2");
		System.out.println();
	}

	/**
	 * Prints the help for the run action.
	 */
	static void printHelpForRun() {
		System.out.println("Action \"run\" : run the job");
		System.out.println();
		System.out.println("  Syntax: run [OPTIONS]");
		System.out.println("  \"run\" action options:");
		System.out.println("    <-stream|-batch>   Stream or Batch job");
		System.out.println("    -jar               User jar file");
		System.out.println("    -class             User class name which must contains a 'build' " +
			"method, please refer to the documents");
		System.out.println("    -jobconf           Conf path of resource file");
		System.out.println("    -jobname           Job name");
		System.out.println("    [-libjars]         User library jars, seprate with ','");
		System.out.println("    [-Dkey=value]      User parameters, e.g. -Dk1=v1 -Dk2=v2");
		System.out.println("    [-Xkey=value]      Runtime parameters, e.g. -Xyqu=default " +
			"-Xyjm=2048");

		System.out.println();
	}

	/**
	 * Parse compile action.
	 *
	 * @param args Input args
	 * @return Parsed compile options
	 * @throws CliArgsException Parse compile action error
	 */
	static CompileOptions parseCompileAction(String[] args) throws CliArgsException {
		try {
			DefaultParser parser = new DefaultParser();
			CommandLine line = parser.parse(COMPILE_OPTIONS, args, true);
			return new CompileOptions(line);
		} catch (ParseException e) {
			throw new CliArgsException(e.getMessage());
		}
	}

	/**
	 * Parse info action.
	 *
	 * @param args Input args
	 * @return Parsed info options
	 * @throws CliArgsException Parse info action error
	 */
	static InfoOptions parseInfoAction(String[] args) throws CliArgsException {
		try {
			DefaultParser parser = new DefaultParser();
			CommandLine line = parser.parse(INFO_OPTIONS, args, true);
			return new InfoOptions(line);
		} catch (ParseException e) {
			throw new CliArgsException(e.getMessage());
		}
	}

	/**
	 * Parse run action.
	 *
	 * @param args Input args
	 * @return Parsed run options
	 * @throws CliArgsException Parse run action error
	 */
	static RunOptions parseRunAction(String[] args) throws CliArgsException {
		try {
			DefaultParser parser = new DefaultParser();
			CommandLine line = parser.parse(EXECUTE_OPTIONS, args, true);
			return new RunOptions(line);
		} catch (ParseException e) {
			throw new CliArgsException(e.getMessage());
		}
	}

}
