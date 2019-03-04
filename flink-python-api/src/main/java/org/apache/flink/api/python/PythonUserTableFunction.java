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
import org.apache.flink.table.api.functions.TableFunction;
import org.apache.flink.table.errorcode.TableErrors;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.DataTypes;
import org.apache.flink.table.types.InternalType;
import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * The Java wrapper for Python Table Function.
 */
public class PythonUserTableFunction extends TableFunction<Row> {
	private static final Logger LOG = LoggerFactory.getLogger(PythonUserTableFunction.class);

	private String funcName;
	private byte[] pickledPyUdf;
	private InternalType[] rowTypes;

	private transient PythonWorker worker;

	public PythonUserTableFunction(String funcName, byte[] pickledPyUdf, InternalType[] rowTypes) {
		this.funcName = funcName;
		this.pickledPyUdf = pickledPyUdf;
		this.rowTypes = rowTypes;
	}

	@Override
	public void open(FunctionContext context) throws Exception {
		// Create a dedicated socket for each TableFunction instance,
		// so that it won't block UDF's request.
		worker = PythonWorkerFactory.createWorker(context);
		worker.openPythonUDF(this.funcName, this.pickledPyUdf);
	}

	public void eval(Object... args) {
		try {
			worker.sendUdfEvalRequest(funcName, args);

			while (true) {
				Protocol.PythonResponse response = worker.getPythonResponse(getResultType(null, null));
				if (response.action == Protocol.ACTION_COLLECT) {
					Row r = (Row) response.result;
					super.collect(r);

					// The results will be buffered in socket channel.
					// It is OK to be asynchronous
					// No need to tell Python side a row has been collected successfully.
				}
				else if (response.action == Protocol.ACTION_SUCCESS) {
					// User's eval function has been finished.
					break;
				}
				else if (response.action == Protocol.ACTION_FAIL) {
					String err = response.result.toString();
					LOG.error(err);
					throw new RuntimeException(
						TableErrors.INST.sqlPythonUDFRunTimeError("eval", funcName, err));
				}
			}
		}
		catch (IOException ioe) {
			LOG.error(ioe.getMessage());
			throw new RuntimeException(
				TableErrors.INST.sqlPythonUDFSocketIOError(funcName, funcName, ioe.getMessage()));
		}
	}

	public void close() throws Exception {
		worker.close();
	}

	@Override
	public DataType getResultType(Object[] arguments, Class[] argTypes) {
		return DataTypes.createRowType(rowTypes);
	}

	public String toString() {
		return funcName;
	}
}
