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
import org.apache.flink.table.api.functions.ScalarFunction;
import org.apache.flink.table.errorcode.TableErrors;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.InternalType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * The Java wrapper for Python Scalar UDF.
 */
public class PythonScalarUDF extends ScalarFunction {
	private static final Logger LOG = LoggerFactory.getLogger(PythonScalarUDF.class);

	private String funcName;
	private byte[] pickledPyUdf;
	private InternalType returnType;

	private transient PythonWorker worker;

	public PythonScalarUDF(String funcName, byte[] pickledPyUdf, InternalType returnType) {
		this.funcName = funcName;
		this.pickledPyUdf = pickledPyUdf;
		this.returnType = returnType;
	}

	@Override
	public void open(FunctionContext context) throws Exception {
		worker = PythonWorkerFactory.getOrElseCreate(context);
		worker.openPythonUDF(this.funcName, this.pickledPyUdf);
	}

	public Object eval(Object... args) throws RuntimeException {
		try {
			worker.sendUdfEvalRequest(funcName, args);
			Protocol.PythonResponse response = worker.getPythonResponse(getResultType(null, null));

			if (response.action == Protocol.ACTION_SUCCESS) {
				return response.result;
			}
			else if (response.action == Protocol.ACTION_FAIL){
				String err = response.result.toString();
				LOG.error(err);
				throw new RuntimeException(
					TableErrors.INST.sqlPythonUDFRunTimeError("eval", funcName, err));
			}
		}
		catch (IOException ioe) {
			LOG.error(ioe.getMessage());
			throw new RuntimeException(
				TableErrors.INST.sqlPythonUDFSocketIOError(funcName, funcName, ioe.getMessage()));
		}

		return null;
	}

	public void close() throws Exception {
		worker.close();
	}

	@Override
	public DataType getResultType(Object[] arguments, Class[] argTypes) {
		// no overloading in Python UDF right now.
		// because python doesn't support overloading by default
		// in future, using python decorator for methods dispatching if necessary
		return returnType;
	}

	public String toString() {
		return funcName;
	}
}
