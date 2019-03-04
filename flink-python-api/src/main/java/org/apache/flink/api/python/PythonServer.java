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

import org.apache.flink.table.api.functions.FunctionContext;
import org.apache.flink.table.errorcode.TableErrors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;

/**
 * Handle the python process.
 */
public class PythonServer {

	private static final Logger LOG = LoggerFactory.getLogger(PythonWorker.class);

	private static Process pyProcess = null;

	private static PythonUtil.PythonEnvironment pyEnv;

	// socket info of the python server
	private static volatile int pySockPort = -1;
	private static String udsPath;  // if Unix Domain Socket is supported

	// key for distributed cached python files
	public static final String PYFLINK_CACHED_USR_LIB_IDS = "PYFLINK_SQL_CACHED_USR_LIB_IDS";
	private static final String PYFLINK_SQL_WORKER = "pyflink.worker_server";

	private FunctionContext ctx;

	public PythonServer(FunctionContext ctx) {
		this.ctx = ctx;
	}

	public int getPort() {
		return pySockPort;
	}

	public String getUdsPath() {
		return udsPath;
	}

	/**
	 *  Start the python process, shake hands with it so that we know the
	 *  port of the python server socket.  Only create s single Python process for
	 *  a JVM instance.
	 */
	public synchronized void startPythonServerIfNeeded () {
		if (pyProcess != null && pyProcess.isAlive()) {
			return;
		}

		ServerSocket serverSocket = null;
		try {
			// only once
			if (pyEnv == null) {
				pyEnv = PythonUtil.preparePythonEnvironment(getUserFilesFromDistributedCache(ctx));
			}

			// Use this server socket for shaking hands with python process
			serverSocket = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"));
			int localPort = serverSocket.getLocalPort();

			String[] pyArgs = new String[3];
			pyArgs[0] = "-m";
			pyArgs[1] = PYFLINK_SQL_WORKER;
			pyArgs[2] = ("" + localPort);
			pyProcess = PythonUtil.startPythonProcess(pyEnv, pyArgs, 1000);

			// assume that Python process will be ready in 10s
			// serverSocket.setSoTimeout(10000);
			Socket shakeHandSock = serverSocket.accept();

			DataInputStream in = new DataInputStream(
				new BufferedInputStream(shakeHandSock.getInputStream()));

			// [len] [port] [pathname]
			// int32  int32    utf-8
			int dataLen = in.readInt();
			int port = in.readInt();
			if (dataLen > 8) {
				udsPath = in.readUTF();
			}
			pySockPort = port;
		} catch (Exception e) {
			String err = TableErrors.INST.sqlPythonProcessError(e.getMessage());
			LOG.error(err);
			throw new RuntimeException(err);
		} finally {
			if (serverSocket != null) {
				try {
					serverSocket.close();
				} catch (IOException e) {
					LOG.warn(e.getMessage());
				}
			}
		}
	}

	private  HashMap<String, String> getUserFilesFromDistributedCache(FunctionContext ctx)
		throws FileNotFoundException {
		// key(fileName), distributed file
		HashMap<String, String> usrFiles = new HashMap<>();
		String usrLibIds = ctx.getJobParameter(PYFLINK_CACHED_USR_LIB_IDS, "");

		if (usrLibIds.length() <= 0) {
			return usrFiles;
		}

		for (String fileName : usrLibIds.split(",")) {
			File dcFile = ctx.getCachedFile(fileName);
			if (dcFile == null || !dcFile.exists()) {
				throw new FileNotFoundException("User's python lib file " + fileName + " does not exist.");
			}
			usrFiles.put(fileName, dcFile.getAbsolutePath());
		}

		return usrFiles;
	}
}
