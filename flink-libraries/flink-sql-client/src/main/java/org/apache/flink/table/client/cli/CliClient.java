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

package org.apache.flink.table.client.cli;

import org.apache.flink.sql.parser.util.SqlInfo;
import org.apache.flink.sql.parser.util.SqlLists;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.client.SqlClientException;
import org.apache.flink.table.client.cli.SqlCommandParser.SqlCommandCall;
import org.apache.flink.table.client.gateway.CliMode;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.ProgramTargetDescriptor;
import org.apache.flink.table.client.gateway.ResultDescriptor;
import org.apache.flink.table.client.gateway.SessionContext;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.local.ExecutionContext;

import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.MaskingCallback;
import org.jline.reader.UserInterruptException;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;
import org.jline.utils.InfoCmp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOError;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * SQL CLI client.
 */
public class CliClient {

	private static final Logger LOG = LoggerFactory.getLogger(CliClient.class);

	private final Executor executor;

	private final SessionContext context;

	private final Terminal terminal;

	private final LineReader lineReader;

	private final String prompt;

	private boolean isRunning;

	private static final int PLAIN_TERMINAL_WIDTH = 80;

	private static final int PLAIN_TERMINAL_HEIGHT = 30;

	private static final int SOURCE_MAX_SIZE = 50_000;

	public CliClient(SessionContext context, Executor executor) {
		this.context = context;
		this.executor = executor;

		try {
			// initialize terminal
			terminal = TerminalBuilder.builder()
				.name(CliStrings.CLI_NAME)
				.build();
			// make space from previous output and test the writer
			terminal.writer().println();
			terminal.writer().flush();
		} catch (IOException e) {
			throw new SqlClientException("Error opening command line interface.", e);
		}

		// initialize line lineReader
		lineReader = LineReaderBuilder.builder()
			.terminal(terminal)
			.appName(CliStrings.CLI_NAME)
			.parser(new SqlMultiLineParser())
			.build();
		// this option is disabled for now for correct backslash escaping
		// a "SELECT '\'" query should return a string with a backslash
		lineReader.option(LineReader.Option.DISABLE_EVENT_EXPANSION, true);

		// create prompt
		prompt = new AttributedStringBuilder()
			.style(AttributedStyle.DEFAULT.foreground(AttributedStyle.GREEN))
			.append("Flink SQL")
			.style(AttributedStyle.DEFAULT)
			.append("> ")
			.toAnsi();
	}

	public Terminal getTerminal() {
		return terminal;
	}

	public SessionContext getContext() {
		return context;
	}

	public void clearTerminal() {
		if (isPlainTerminal()) {
			for (int i = 0; i < 200; i++) { // large number of empty lines
				terminal.writer().println();
			}
		} else {
			terminal.puts(InfoCmp.Capability.clear_screen);
		}
	}

	public boolean isPlainTerminal() {
		// check if terminal width can be determined
		// e.g. IntelliJ IDEA terminal supports only a plain terminal
		return terminal.getWidth() == 0 && terminal.getHeight() == 0;
	}

	public int getWidth() {
		if (isPlainTerminal()) {
			return PLAIN_TERMINAL_WIDTH;
		}
		return terminal.getWidth();
	}

	public int getHeight() {
		if (isPlainTerminal()) {
			return PLAIN_TERMINAL_HEIGHT;
		}
		return terminal.getHeight();
	}

	public Executor getExecutor() {
		return executor;
	}

	/**
	 * Opens the interactive CLI shell.
	 */
	public void open() {
		isRunning = true;

		// print welcome
		terminal.writer().append(CliStrings.MESSAGE_WELCOME);

		// begin reading loop
		while (isRunning) {
			// make some space to previous command
			terminal.writer().append("\n");
			terminal.flush();

			final String line;
			try {
				line = lineReader.readLine(prompt, null, (MaskingCallback) null, null);
			} catch (UserInterruptException e) {
				// user cancelled line with Ctrl+C
				continue;
			} catch (EndOfFileException | IOError e) {
				// user cancelled application with Ctrl+D or kill
				break;
			} catch (Throwable t) {
				throw new SqlClientException("Could not read from command line.", t);
			}
			if (line == null) {
				continue;
			}
			final Optional<SqlCommandCall> cmdCall = parseCommand(line);
			cmdCall.ifPresent(this::callCommand);
		}
	}

	public void submitSQLFile(URL sqlFile) {
		isRunning = true;
		// load file
		final String statements;

		try {
			final Path path = Paths.get(sqlFile.toURI());
			byte[] encoded = Files.readAllBytes(path);
			statements = new String(encoded, Charset.defaultCharset());
		} catch (IOException | URISyntaxException e) {
			printException(e);
			return;
		}

		List<SqlInfo> sqls = SqlLists.getSQLList(statements);
		int jobCnt = 0;
		ExecutionContext<?> executionContext = executor.getOrCreateExecutionContext(this.context);
		// To ensure the line number matched with the original file.
		for (SqlInfo sqlInfo : sqls) {
			int startLine = sqlInfo.getLine();
			StringBuilder sqlBuilder = new StringBuilder();
			for (int i = 0; i < startLine - 1; i++) {
				sqlBuilder.append('\n');
			}
			String sql = sqlBuilder.append(sqlInfo.getSqlContent()).toString();
			sqlInfo.setSqlContent(sql);

			final Optional<SqlCommandCall> cmdCall = parseCommand(sql);

			// Count the jobs
			if (cmdCall.isPresent()) {
				if (executionContext.isNeedShareEnv()) {
					// Environment is shared
					if (cmdCall.get().command.isDML()) {
						jobCnt++;
					}
				} else {
					// Environment is not shared
					if (cmdCall.get().command.isJob()) {
						jobCnt++;
					}
				}
			}

			// If it is command or ddl, execution wil be done here.
			cmdCall.ifPresent(this::callCommand);
		}

		if (executionContext.isNeedShareEnv() && jobCnt > 0) {
			// If the Environment is shared, we will submit the job at the very last.
			ProgramTargetDescriptor programTarget = executor.submitJob(executionContext);
			terminal.writer().println(CliStrings.messageInfo(CliStrings.MESSAGE_SQL_FILE_SUBMITTED).toAnsi());
			terminal.writer().println(programTarget.toString());
			terminal.flush();
		}
	}

	/**
	 * Submits a SQL update statement and prints status information and/or errors on the terminal.
	 *
	 * @param statement SQL update statement
	 * @return flag to indicate if the submission was successful or not
	 */
	public boolean submitUpdate(String statement) {
		terminal.writer().println(CliStrings.messageInfo(CliStrings.MESSAGE_WILL_EXECUTE).toAnsi());
		terminal.writer().println(new AttributedString(statement).toString());
		terminal.flush();

		final Optional<SqlCommandCall> parsedStatement = parseCommand(statement);
		// only support INSERT INTO
		return parsedStatement.map(cmdCall -> {
			switch (cmdCall.command) {
				case INSERT_INTO:
					return callInsertInto(cmdCall);
				default:
					terminal.writer().println(CliStrings.messageError(CliStrings.MESSAGE_UNSUPPORTED_SQL).toAnsi());
					terminal.flush();
					return false;
			}
		}).orElse(false);
	}

	// --------------------------------------------------------------------------------------------

	private Optional<SqlCommandCall> parseCommand(String line) {
		final Optional<SqlCommandCall> parsedLine = SqlCommandParser.parse(line);
		if (!parsedLine.isPresent()) {
			terminal.writer().println(CliStrings.messageError(CliStrings.MESSAGE_UNKNOWN_SQL).toAnsi());
			terminal.flush();
		}
		return parsedLine;
	}

	private void callCommand(SqlCommandCall cmdCall) {
		switch (cmdCall.command) {
			case QUIT:
			case EXIT:
				callQuit(cmdCall);
				break;
			case CLEAR:
				callClear(cmdCall);
				break;
			case RESET:
				callReset(cmdCall);
				break;
			case SET:
				callSet(cmdCall);
				break;
			case HELP:
				callHelp(cmdCall);
				break;
			case SHOW_CATALOGS:
				callShowCatalogs(cmdCall);
				break;
			case SHOW_DATABASES:
				callShowDatabases(cmdCall);
				break;
			case SHOW_TABLES:
				callShowTables(cmdCall);
				break;
			case SHOW_FUNCTIONS:
				callShowFunctions(cmdCall);
				break;
			case USE:
				callUseDatabase(cmdCall);
				break;
			case DESCRIBE:
				callDescribe(cmdCall);
				break;
			case DESC:
				callDescribe(cmdCall);
				break;
			case EXPLAIN:
				callExplain(cmdCall);
				break;
			case CREATE_FUNCTION:
				callCreateFunction(cmdCall);
				break;
			case CREATE_TABLE:
				callCreateTable(cmdCall);
				break;
			case CREATE_VIEW:
				callCreateView();
				break;
			case ANALYZE:
				callAnalyze(cmdCall);
				break;
			case SELECT:
				callSelect(cmdCall);
				break;
			case INSERT_INTO:
				callInsertInto(cmdCall);
				break;
			case SOURCE:
				callSource(cmdCall);
				break;
			default:
				throw new SqlClientException("Unsupported command: " + cmdCall.command);
		}
	}

	private void callAnalyze(SqlCommandCall cmdCall) {
		executor.analyzeTable(context, cmdCall.operands[0]);
	}

	private void callCreateView() {
		// TODO Also save it to meta
	}

	private boolean callCreateFunction(SqlCommandCall cmdCall) {
		return executor.createFunction(context, cmdCall.operands[0]);
	}

	private void callQuit(SqlCommandCall cmdCall) {
		terminal.writer().println(CliStrings.MESSAGE_QUIT);
		terminal.flush();
		isRunning = false;
	}

	private void callClear(SqlCommandCall cmdCall) {
		clearTerminal();
	}

	private void callReset(SqlCommandCall cmdCall) {
		context.resetSessionProperties();
		terminal.writer().println(CliStrings.messageInfo(CliStrings.MESSAGE_RESET).toAnsi());
	}

	private void callSet(SqlCommandCall cmdCall) {
		// show all properties
		if (cmdCall.operands.length == 0) {
			final Map<String, String> properties;
			try {
				properties = executor.getSessionProperties(context);
			} catch (SqlExecutionException e) {
				printException(e);
				return;
			}
			if (properties.isEmpty()) {
				terminal.writer().println(CliStrings.messageInfo(CliStrings.MESSAGE_EMPTY).toAnsi());
			} else {
				properties
					.entrySet()
					.stream()
					.map((e) -> e.getKey() + "=" + e.getValue())
					.sorted()
					.forEach((p) -> terminal.writer().println(p));
			}
		}
		// set a property
		else {
			context.setSessionProperty(cmdCall.operands[0], cmdCall.operands[1]);
			terminal.writer().println(CliStrings.messageInfo(CliStrings.MESSAGE_SET).toAnsi());
		}
		terminal.flush();
	}

	private void callHelp(SqlCommandCall cmdCall) {
		terminal.writer().println(CliStrings.MESSAGE_HELP);
		terminal.flush();
	}

	private void callShowCatalogs(SqlCommandCall cmdCall) {
		final List<String> catalogs;
		try {
			catalogs = executor.listCatalogs(context);
		} catch (SqlExecutionException e) {
			printException(e);
			return;
		}
		if (catalogs.isEmpty()) {
			terminal.writer().println(CliStrings.messageInfo(CliStrings.MESSAGE_EMPTY).toAnsi());
		} else {
			catalogs.forEach((v) -> terminal.writer().println(v));
		}
		terminal.flush();
	}

	private void callShowDatabases(SqlCommandCall cmdCall) {
		final List<String> dbs;
		try {
			dbs = executor.listDatabases(context);
		} catch (SqlExecutionException e) {
			printException(e);
			return;
		}
		if (dbs.isEmpty()) {
			terminal.writer().println(CliStrings.messageInfo(CliStrings.MESSAGE_EMPTY).toAnsi());
		} else {
			dbs.forEach((v) -> terminal.writer().println(v));
		}
		terminal.flush();
	}

	private void callShowTables(SqlCommandCall cmdCall) {
		final List<String> tables;
		try {
			tables = executor.listTables(context);
		} catch (SqlExecutionException e) {
			printException(e);
			return;
		}
		if (tables.isEmpty()) {
			terminal.writer().println(CliStrings.messageInfo(CliStrings.MESSAGE_EMPTY).toAnsi());
		} else {
			tables.forEach((v) -> terminal.writer().println(v));
		}
		terminal.flush();
	}

	private void callShowFunctions(SqlCommandCall cmdCall) {
		final List<String> functions;
		try {
			functions = executor.listUserDefinedFunctions(context);
		} catch (SqlExecutionException e) {
			printException(e);
			return;
		}
		if (functions.isEmpty()) {
			terminal.writer().println(CliStrings.messageInfo(CliStrings.MESSAGE_EMPTY).toAnsi());
		} else {
			functions.forEach((v) -> terminal.writer().println(v));
		}
		terminal.flush();
	}

	private void callUseDatabase(SqlCommandCall cmdCall) {
		try {
			executor.setDefaultDatabase(context, cmdCall.operands[0]);
		} catch (SqlExecutionException e) {
			printException(e);
			return;
		}
		terminal.flush();
	}

	private void callDescribe(SqlCommandCall cmdCall) {
		final TableSchema schema;
		try {
			schema = executor.getTableSchema(context, cmdCall.operands[0]);
		} catch (SqlExecutionException e) {
			printException(e);
			return;
		}
		terminal.writer().println(schema.toString());
		terminal.flush();
	}

	private void callExplain(SqlCommandCall cmdCall) {
		final String explanation;
		try {
			explanation = executor.explainStatement(context, cmdCall.operands[0]);
		} catch (SqlExecutionException e) {
			printException(e);
			return;
		}
		terminal.writer().println(explanation);
		terminal.flush();
	}

	private void callSelect(SqlCommandCall cmdCall) {
		final ResultDescriptor resultDesc;
		try {
			resultDesc = executor.executeQuery(context, cmdCall.operands[0]);
		} catch (SqlExecutionException e) {
			printException(e);
			return;
		}
		final CliResultView view;
		if (resultDesc.isMaterialized()) {
			view = new CliTableResultView(this, resultDesc);
		} else {
			view = new CliChangelogResultView(this, resultDesc);
		}

		// enter view
		try {
			view.open();

			// view left
			terminal.writer().println(CliStrings.messageInfo(CliStrings.MESSAGE_RESULT_QUIT).toAnsi());
			terminal.flush();
		} catch (SqlExecutionException e) {
			printException(e);
		}
	}

	private boolean callInsertInto(SqlCommandCall cmdCall) {
		terminal.writer().println(CliStrings.messageInfo(CliStrings.MESSAGE_SUBMITTING_STATEMENT).toAnsi());
		terminal.flush();

		try {
			ProgramTargetDescriptor programTarget = executor.executeUpdate(context, cmdCall.operands[0]);
			if (null == programTarget && context.getCliMode().equals(CliMode.INTERACTIVE)) {
				// If it is an interactive mode
				ExecutionContext<?> executionContext = executor.getOrCreateExecutionContext(this.context);
				programTarget = executor.submitJob(executionContext);
			}
			if (null != programTarget) {
				terminal.writer().println(CliStrings.messageInfo(CliStrings.MESSAGE_STATEMENT_SUBMITTED).toAnsi());
				terminal.writer().println(programTarget.toString());
				terminal.flush();
			}
		} catch (SqlExecutionException e) {
			printException(e);
			return false;
		}
		return true;
	}

	private boolean callCreateTable(SqlCommandCall cmdCall) {
		return executor.createTable(context, cmdCall.operands[0]);
	}

	private void callSource(SqlCommandCall cmdCall) {
		final String pathString = cmdCall.operands[0];

		if (pathString.isEmpty()) {
			printError(CliStrings.MESSAGE_INVALID_PATH);
			return;
		}

		// load file
		final String stmt;
		try {
			final Path path = Paths.get(pathString);
			byte[] encoded = Files.readAllBytes(path);
			stmt = new String(encoded, Charset.defaultCharset());
		} catch (IOException e) {
			printException(e);
			return;
		}

		// limit the output a bit
		if (stmt.length() > SOURCE_MAX_SIZE) {
			printError(CliStrings.MESSAGE_MAX_SIZE_EXCEEDED);
			return;
		}

		terminal.writer().println(CliStrings.messageInfo(CliStrings.MESSAGE_WILL_EXECUTE).toAnsi());
		terminal.writer().println(new AttributedString(stmt).toString());
		terminal.flush();

		// try to run it
		final Optional<SqlCommandCall> call = parseCommand(stmt);
		call.ifPresent(this::callCommand);
	}

	// --------------------------------------------------------------------------------------------

	private void printException(Throwable t) {
		LOG.warn(CliStrings.MESSAGE_SQL_EXECUTION_ERROR, t);
		terminal.writer().println(CliStrings.messageError(CliStrings.MESSAGE_SQL_EXECUTION_ERROR, t).toAnsi());
		terminal.flush();
	}

	private void printError(String e) {
		terminal.writer().println(CliStrings.messageError(CliStrings.MESSAGE_SQL_EXECUTION_ERROR, e).toAnsi());
		terminal.flush();
	}
}
