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

package org.apache.flink.api.python;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.filecache.FileCache;
import org.apache.flink.util.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class PythonUtil {
	public static final String PYFLINK_CACHED_USR_LIB_IDS = "PYFLINK_SQL_CACHED_USR_LIB_IDS";

	private static final String PYFLINK_LIB_ZIP_FILENAME = "pyflink.zip";
	private static final String PYFLINK_PY4J_FILENAME = "py4j-0.10.8-src.zip";

	private static final String VIRTUALEVN_ZIP_FILENAME  = "venv.zip";
	// private static final String PYFLINK_SQL_WORKER = "pyflink.worker_server";

	/**
	 * Information for Java to start a Python process.
	 */
	public static class PythonEnvironment {
		public String workingDirectory;
		public String pythonExec = "python";  // by default
		public String pythonPath;
		public String envPath;
		public boolean isVirtualEnv = false; // has virtualenv or conda environment
		public Map<String, String> sysVariables = new HashMap<>();
	}

	/**
	 * Prepare a working directory for Python.
	 * @param usrFiles  User's python files (name -> fullPath)
	 * @return PythonEnvironment
	 * @throws IOException
	 */
	public static PythonEnvironment preparePythonEnvironment(Map<String, String> usrFiles) throws IOException {
		PythonEnvironment env = new PythonEnvironment();

		StringBuilder pythonPathEnv = new StringBuilder();

		// Add Python Flink libraries to PYTHONPATH
		String root = System.getenv().get("FLINK_ROOT_DIR");
		if (root != null) {
			String pyLibDir = root + File.separator + "lib" + File.separator + "python" +  File.separator;
			final String[] libs = {PYFLINK_LIB_ZIP_FILENAME, PYFLINK_PY4J_FILENAME};
			for (String lib : libs) {
				pythonPathEnv.append(pyLibDir + lib);
				pythonPathEnv.append(File.pathSeparator);
			}
		}

		// 1. setup temporary local directory for the user files
		String tmpFilesDir = System.getProperty("java.io.tmpdir") +
			File.separator + "pyflink_tmp_" + UUID.randomUUID();

		Path tmpFileDirPath = new Path(tmpFilesDir);
		FileSystem fs = tmpFileDirPath.getFileSystem();
		if (fs.exists(tmpFileDirPath)) {
			fs.delete(tmpFileDirPath, true);
		}
		tmpFileDirPath.getFileSystem().mkdirs(tmpFileDirPath);
		pythonPathEnv.append(tmpFileDirPath.toString());

		env.workingDirectory = tmpFileDirPath.toString();

		// copy all user's files from distributed cache to tmp folder
		// Map<String, File> usrFiles = getUserFilesFromDistributedCache(ctx);
		for (Map.Entry<String, String> entry : usrFiles.entrySet()) {
			if (entry.getKey().endsWith(VIRTUALEVN_ZIP_FILENAME)) {
				prepareVirtualEnvFiles(
					entry.getValue(),
					tmpFileDirPath.toString(),
					env);
				env.isVirtualEnv = true;
			}
			else {
				// use the original name (key)
				Path targetFilePath = new Path(tmpFileDirPath, entry.getKey());
				FileCache.copy(new Path(entry.getValue()), targetFilePath, false);

				// if user upload xxx.zip packages, add them to PYTHONPATH
				String usrFileName = targetFilePath.toString();
				if (usrFileName.endsWith(".zip")
					&& !usrFileName.endsWith(PYFLINK_LIB_ZIP_FILENAME)
					&& !usrFileName.endsWith(PYFLINK_PY4J_FILENAME)) {
					pythonPathEnv.append(File.pathSeparator);
					pythonPathEnv.append(usrFileName);
				}
			}
		}

		env.pythonPath = pythonPathEnv.toString();
		return env;
	}

	private static void prepareVirtualEnvFiles(String venvZipFilePath, String pythonDir, PythonEnvironment env) {
		try {
			// ZipInputStream won't keep the permission of the files
			// apache compress does. But, java OutputStream can't open hidden files.
			// here, use shell commands to unzip it.
			String[] unzipCmd = {
				"unzip",
				"-qq",
				"-o",
				venvZipFilePath,
				"-d",
				pythonDir
			};
			ProcessBuilder pb = new ProcessBuilder();
			pb.command(unzipCmd);
			Process p = pb.start();

			redirectStreamsToStderr(p.getInputStream(), p.getErrorStream());
			//Runtime.getRuntime().addShutdownHook(new ShutDownPythonHook(p, null));

			p.waitFor(1, TimeUnit.MINUTES);
			if (!p.isAlive()) {
				p.destroyForcibly();
			}

			File dir = new File(pythonDir);
			String pyExecPath = searchBinPython(dir);
			if (pyExecPath != null) {
				env.pythonExec = pyExecPath;
				int idx = pyExecPath.lastIndexOf(File.separator);
				env.envPath = pyExecPath.substring(0, idx);
				env.isVirtualEnv = true;
			}
			else {
				throw new RuntimeException("executable python is not found!\n");
			}
		}
		catch (Exception ex) {
			throw new RuntimeException("Can't prepare virtualenv for python, please check your venv.zip. " + ex.getMessage());
		}
	}

	private static String searchBinPython(File f) {
		if (f.isDirectory() && "bin".equals(f.getName())) {
			File pyExec = new File(f.getAbsolutePath() + File.separator + "python");
			if (pyExec.exists()) {
				return pyExec.getAbsolutePath();
			}
		}
		else {
			for (File sub : f.listFiles()) {
				if (sub.isDirectory()) {
					String p = searchBinPython(sub);
					if (p != null) {
						return p;
					}
				}
			}
		}
		return null;
	}

	public static synchronized Process startPythonProcess(
		PythonEnvironment pyEnv,
		String[] usrCommands,
		long waitFor) throws Exception {

		String[] commands = new String[usrCommands.length + 1];
		System.arraycopy(usrCommands, 0, commands, 1, usrCommands.length);
		commands[0] = pyEnv.pythonExec;
//		commands = new String[] {
//			pythonExecutable,
//			"-m",
//			pyWorker,
//			" " + javaPort
//		};
//
		ProcessBuilder pb = new ProcessBuilder();
		Map<String, String> env = pb.environment();
		if (pyEnv.isVirtualEnv) {
			StringBuilder pathVar = new StringBuilder();
			pathVar.append(pyEnv.envPath);
			pathVar.append(File.pathSeparator);
			pathVar.append(env.get("PATH"));
			env.put("PATH", pathVar.toString());
		}
		env.put("PYTHONPATH", pyEnv.pythonPath);

		pyEnv.sysVariables.forEach((key, value) -> {
			env.put(key, value);
		});

		pb.command(commands);
		pb.directory(new File(pyEnv.workingDirectory));
		Process p = pb.start();

		// Redirect python worker stdout and stderr
		PythonUtil.redirectStreamsToStderr(p.getInputStream(), p.getErrorStream());

		if (waitFor > 0) {
			p.waitFor(waitFor, TimeUnit.MILLISECONDS);
		}
		if (!p.isAlive()) {
			throw new RuntimeException("Failed to start Python process. ");
		}

		// Make sure that the python sub process will be killed when JVM exit
		Runtime.getRuntime().addShutdownHook(
			new PythonUtil.ShutDownPythonHook(p, pyEnv.workingDirectory));

		return p;
	}

	public static void redirectStreamsToStderr(InputStream stdout, InputStream stderr) {
		try {
			new PythonUtil.RedirectThread(stdout, System.err).start();
			new PythonUtil.RedirectThread(stderr, System.err).start();
		}
		catch (Exception ex) {
			//LOG.warn(ex.getMessage());
		}
	}

	static class RedirectThread extends Thread {
		InputStream in;
		OutputStream out;
		public RedirectThread(InputStream in, OutputStream out) {
			setDaemon(true);
			this.in = in;
			this.out = out;
		}

		@Override
		public void run() {
			try {
				byte[] buf = new byte[1024];
				int len = in.read(buf);
				while (len != -1) {
					out.write(buf, 0, len);
					out.flush();
					len = in.read(buf);
				}
			}
			catch (Exception ex) {
				// just ignore it
			}
		}
	}

	static class ShutDownPythonHook extends Thread {
		private Process p;
		private String pyFileDir;

		public ShutDownPythonHook(Process p, String pyFileDir) {
			this.p = p;
			this.pyFileDir = pyFileDir;
		}

		public void run() {

			p.destroyForcibly();

			if (pyFileDir != null) {
				File pyDir = new File(pyFileDir);
				FileUtils.deleteDirectoryQuietly(pyDir);
			}
		}
	}

	/**
	 *  types for python udf ser/des.
	 */
	enum PythonSerDesTypes {
		/**
		 * indicate java null, python None type.
		 */
		NONE,

		/**
		 * indicate utf-8 string.
		 */
		STRING,

		/**
		 * indicate boolean type.
		 */
		BOOLEAN,

		/**
		 * indicate short (2 bytes) integer.
		 */
		SHORT,

		/**
		 * indicate tiny (1 byte) integer.
		 */
		BYTE,

		/**
		 * indicate 4 bytes integer.
		 */
		INT,

		/**
		 * indicate 8 bytes integer.
		 */
		LONG,

		/**
		 * indicate float.
		 */
		FLOAT,

		/**
		 * indicate double.
		 */
		DOUBLE,

		/**
		 * indicate binary.
		 */
		BYTES,

		/**
		 * indicate date type. internally, it is epoch days.
		 */
		DATE,

		/**
		 * indicate time type. internally, it is epoch milliseconds.
		 */
		TIME,

		/**
		 * indicate timestamp type. internally, it is epoch milliseconds.
		 */
		TIMESTAMP,

		/**
		 * indicate decimal.
		 */
		DECIMAL
	}
}
