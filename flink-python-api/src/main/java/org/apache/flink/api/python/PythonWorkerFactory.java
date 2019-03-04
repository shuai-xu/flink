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

import org.newsclub.net.unix.AFUNIXSocket;
import org.newsclub.net.unix.AFUNIXSocketAddress;

import java.io.File;
import java.net.Socket;

/***
 *
 */
public class PythonWorkerFactory {

	private static PythonServer server = null;
	private static FunctionContext context = null;

	// one socket connection per Operator(thread)
	// the PythonWorker holds on a TCP connection
	private static ThreadLocal<PythonWorker> workerLocal = new ThreadLocal<PythonWorker>(){

		protected PythonWorker initialValue() {
			return createWorker(context);
		}
	};

	public static synchronized PythonWorker getOrElseCreate(FunctionContext ctx) {
		if (context == null) {
			context = ctx;
		}
		return workerLocal.get();
	}

	public static synchronized PythonWorker createWorker(FunctionContext context) {

		final int retries = 1;
		Socket workerSocket;
		for (int p = 0; p < retries; p++) {
			if (server == null) {
				server = new PythonServer(context);
				server.startPythonServerIfNeeded();
			}

			for (int s = 0; s < retries; s++) {
				try {
					int pySockPort = server.getPort();
					if (pySockPort > 0) {
						workerSocket = new Socket("127.0.0.1", pySockPort);
						workerSocket.setKeepAlive(true);
						// workerSocket.setSoTimeout(1000);
					}
					else {
						// FileDescriptor
						final File socketFile = new File(server.getUdsPath());
						AFUNIXSocketAddress addr = new AFUNIXSocketAddress(socketFile);
						workerSocket = AFUNIXSocket.newInstance();
						workerSocket.connect(addr);
					}
					if (workerSocket.isConnected()) {
						return new PythonWorker(workerSocket);
					}
				} catch (Exception e) {
					String err = TableErrors.INST.sqlPythonCreateSocketError(e.getMessage());
//					LOG.error(err);
				}
			}

			// if can't connect to python which may be crashed
//			if (pyProcess != null) {
//				pyProcess.destroyForcibly();
//				pyProcess = null;
//				pySockPort = -1;
//			}
			if (p == retries) {
				throw new RuntimeException(TableErrors.INST.sqlPythonCreateSocketError(
					"Have tried twice, and can't connect to python. "));
			}
		}
		return null;
	}

}
