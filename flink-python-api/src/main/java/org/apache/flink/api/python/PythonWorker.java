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

import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.errorcode.TableErrors;
import org.apache.flink.table.runtime.functions.BuildInScalarFunctions;
import org.apache.flink.table.types.BooleanType;
import org.apache.flink.table.types.ByteArrayType;
import org.apache.flink.table.types.ByteType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.DataTypes;
import org.apache.flink.table.types.DateType;
import org.apache.flink.table.types.DecimalType;
import org.apache.flink.table.types.DoubleType;
import org.apache.flink.table.types.FloatType;
import org.apache.flink.table.types.LongType;
import org.apache.flink.table.types.RowType;
import org.apache.flink.table.types.ShortType;
import org.apache.flink.table.types.StringType;
import org.apache.flink.table.types.TimeType;
import org.apache.flink.table.types.TimestampType;
import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Communicating with Python, we hide the implementation(currently it is socket) in PythonWorker/PythonServer
 *
 * <p>
 * one PythonWorker per sub-task.
 *  Protocol:
 *  Data which is passed to Python Side:
 *  [Data] -> [RawDataLen][RawData]
 *  [RawData] -> [HEADER][Action][]
 * </p>
 */
public class PythonWorker {
	private static final Logger LOG = LoggerFactory.getLogger(PythonWorker.class);

	private static final int BUFFER_SIZE = 8192;
	private AtomicInteger useCount = new AtomicInteger();
	private Socket workerSock;

	public PythonWorker(Socket workerSock) {
		this.workerSock = workerSock;
	}

	/**
	 * Open funcName python UDF.
	 * @param funcName
	 */
	public void openPythonUDF(String funcName, byte[] pickledUdf) throws Exception {
		useCount.incrementAndGet();
		sendOpenRequest(funcName, pickledUdf);

		Protocol.PythonResponse response = getPythonResponse(null);

		if (response.action == Protocol.ACTION_FAIL){
			Object res = response.result;
			String err = res.toString();
			LOG.error(err);
			throw new RuntimeException(
				TableErrors.INST.sqlPythonUDFRunTimeError("open", funcName, err));
		}
	}

	public synchronized void close() throws Exception {
		int count = useCount.decrementAndGet();
		if (count == 0) {
			workerSock.close();
		}
	}

	public void sendOpenRequest(String funcName, byte[] pickledUdf) throws IOException {
		// side-stepping the Global Interpreter Lock by using subprocesses
		DataOutputStream out = new DataOutputStream(
			new BufferedOutputStream(workerSock.getOutputStream(), 8192));

		// Length: int, 4 bytes
		// version, 1 byte
		// action,  1 byte
		// func_name_len  2 byes
		// func_name      len byes
		// pickled_bytes

		byte[] funcBytes = funcName.getBytes("UTF-8");
		int dataLength = 4 + (1 + 1) + (2 + funcBytes.length) + (pickledUdf.length);

		out.writeInt(dataLength);
		out.writeByte(Protocol.VERSION);
		out.writeByte(Protocol.ACTION_OPEN);

		// writeUtf8
		out.writeShort(funcBytes.length);
		out.write(funcBytes);

		//out.writeInt(pickledUdf.length);
		out.write(pickledUdf);

		out.flush();
	}

	public void sendUdfEvalRequest(String pyFunctionName, Object... args) throws IOException {

		// Length: int, 4 bytes
		// version, 1 byte
		// action,  1 byte
		// func_name_len  2 byes
		// func_name      len byes
		// [num of args] short
		// [types of args]
		// [args]

		DataOutputStream out = new DataOutputStream(
			new BufferedOutputStream(workerSock.getOutputStream(), BUFFER_SIZE));

		ByteArrayOutputStream cmdBuff = new ByteArrayOutputStream();
		ByteArrayOutputStream argsTypesBuff = new ByteArrayOutputStream();
		ByteArrayOutputStream argsDataBuff = new ByteArrayOutputStream();

		DataOutputStream cmdOut = new DataOutputStream(cmdBuff);
		DataOutputStream argsTypesOut = new DataOutputStream(argsTypesBuff);
		DataOutputStream argsDataOut = new DataOutputStream(argsDataBuff);

		// Header & commands & args num
		cmdOut.writeByte(Protocol.VERSION);        // protocol version
		cmdOut.writeByte(Protocol.ACTION_SCALAR_EVAL);    // action
		cmdOut.writeUTF(pyFunctionName);          // with length

		// args data: [num][type][arg1][type][arg2]...
		cmdOut.writeShort(args.length);           // args num

		// types & data of arguments
		for (Object a : args) {
			if (a == null) {
				argsTypesOut.writeByte(PythonUtil.PythonSerDesTypes.NONE.ordinal());
			}
			if (a instanceof String) {
				argsTypesOut.writeByte(PythonUtil.PythonSerDesTypes.STRING.ordinal());
				argsDataOut.writeUTF((String) a);
			}
			else if (a instanceof Boolean) {
				argsTypesOut.writeByte(PythonUtil.PythonSerDesTypes.BOOLEAN.ordinal());
				argsDataOut.writeBoolean((Boolean) a);
			}
			else if (a instanceof Short) {
				argsTypesOut.writeByte(PythonUtil.PythonSerDesTypes.SHORT.ordinal());
				argsDataOut.writeShort((Short) a);
			}
			else if (a instanceof Byte) {
				argsTypesOut.writeByte(PythonUtil.PythonSerDesTypes.BYTE.ordinal());
				argsDataOut.writeByte((Byte) a);
			}
			else if (a instanceof Integer) {
				argsTypesOut.writeByte(PythonUtil.PythonSerDesTypes.INT.ordinal());
				argsDataOut.writeInt((Integer) a);
			}
			else if (a instanceof Long) {
				argsTypesOut.writeByte(PythonUtil.PythonSerDesTypes.LONG.ordinal());
				argsDataOut.writeLong((Long) a);
			}
			else if (a instanceof Float) {
				argsTypesOut.writeByte(PythonUtil.PythonSerDesTypes.FLOAT.ordinal());
				argsDataOut.writeFloat((Float) a);
			}
			else if (a instanceof Double) {
				argsTypesOut.writeByte(PythonUtil.PythonSerDesTypes.DOUBLE.ordinal());
				argsDataOut.writeDouble((Double) a);
			}
			else if (a instanceof byte[]) {
				argsTypesOut.writeByte(PythonUtil.PythonSerDesTypes.BYTES.ordinal());

				byte[] bytes = (byte[]) a;
				int len = bytes.length;
				argsDataOut.writeShort(len);
				argsDataOut.write(bytes);
			}
			else if (a instanceof java.sql.Date) {
				argsTypesOut.writeByte(PythonUtil.PythonSerDesTypes.DATE.ordinal());

				java.sql.Date date = (java.sql.Date) a;
				argsDataOut.writeInt(BuildInScalarFunctions.toInt(date));
			}
			else if (a instanceof java.sql.Time) {
				argsTypesOut.writeByte(PythonUtil.PythonSerDesTypes.TIME.ordinal());

				java.sql.Time time = (java.sql.Time) a;
				argsDataOut.writeInt(BuildInScalarFunctions.toInt(time));
			}
			else if (a instanceof java.sql.Timestamp) {
				argsTypesOut.writeByte(PythonUtil.PythonSerDesTypes.TIMESTAMP.ordinal());

				java.sql.Timestamp ts = (java.sql.Timestamp) a;
				argsDataOut.writeLong(ts.getTime());
			}
			else if (a instanceof java.math.BigDecimal) {
				argsTypesOut.writeByte(PythonUtil.PythonSerDesTypes.DECIMAL.ordinal());
				argsDataOut.writeUTF(a.toString());
			}
		}

		int dataLength = cmdBuff.size() + argsTypesBuff.size() + argsDataBuff.size();

		// write length (int type) into the stream,
		// so that python side know how to recv all the data.
		out.writeInt(dataLength);

		// write all the raw data into the stream
		out.write(cmdBuff.toByteArray());
		out.write(argsTypesBuff.toByteArray());
		out.write(argsDataBuff.toByteArray());
		out.flush();
	}

	public Protocol.PythonResponse getPythonResponse(DataType dataType) throws IOException, RuntimeException {
		// see: python implementation
		// def serialize_py_result(act_flag, data, return_types):
		//     Format:  [version] [act_flag]  [arity]  [null bits][data]
		//              byte      byte        short

		DataInputStream in = new DataInputStream(
			new BufferedInputStream(workerSock.getInputStream()));

		Protocol.PythonResponse response = new Protocol.PythonResponse();
		response.version = (int) in.readByte();
		assert(response.version == Protocol.VERSION);
		response.action = in.readByte();

		if (response.action == Protocol.ACTION_FAIL) {
			// TODO: read error msg
			String msg = "TODO: read error msg.";
			throw new RuntimeException(msg);
		}
		else {
			// in case python function return type is void.
			if (dataType == null) {
				return response;
			}

			int fieldsNum = (int) in.readShort();

			// read nullBitSet
			int nullBitsLength =  (fieldsNum + 7) / 8;
			byte[] nullBits = new byte[nullBitsLength];
			in.readFully(nullBits);

			if (dataType instanceof RowType) {
				DataType[] types = ((RowType) dataType).getFieldTypes();
				Row row = new Row(types.length);
				for (int i = 0; i < fieldsNum; i++) {
					if (isNullField(nullBits, i)) {
						row.setField(i, null);
					}
					row.setField(i, parseField(in, types[i]));
				}
				response.result = row;
			}
			else {
				if (!isNullField(nullBits, 0)) {
					response.result = parseField(in, dataType);
				}
			}

			return response;
		}
	}

	private Object parseField(DataInputStream in, DataType dataType) throws IOException, RuntimeException {
		// INT, STRING, LONG,
		if (dataType == DataTypes.INT) {
			return in.readInt();
		}
		else if (dataType instanceof StringType) {
			int len = in.readShort();
			byte[] utf8bytes = new byte[len];
			in.read(utf8bytes);
			return BinaryString.fromBytes(utf8bytes);
		}
		else if (dataType instanceof LongType) {
			in.readLong();
		}
		else if (dataType instanceof DateType || dataType instanceof TimeType) {
			return in.readInt();
		}
		else if (dataType instanceof TimestampType) {
			return in.readLong();
		}
		else if (dataType instanceof DecimalType) {
			// TODO: verify scale & precision of the decimal from python side.
			String s = in.readUTF();
			return new java.math.BigDecimal(s);
		}
		else if (dataType instanceof ByteType) {
			return in.readByte();
		}
		else if (dataType instanceof ShortType) {
			return in.readShort();
		}
		else if (dataType instanceof BooleanType) {
			return in.readBoolean();
		}
		else if (dataType instanceof ByteArrayType) {
			int len = in.readUnsignedShort();
			byte[] bytes = new byte[len];
			in.read(bytes);
			return bytes;
		}
		else if (dataType instanceof FloatType) {
			return in.readFloat();
		}
		else if (dataType instanceof DoubleType) {
			return in.readDouble();
		}
		else {
			throw new RuntimeException("Not Support type, or python return the wrong data");
		}

		return null;
	}

	private boolean isNullField(byte[] bitSet, int fieldIndex) {
		int byteIndex = (fieldIndex / 8);
		byte flag = (byte) (128 >> (fieldIndex % 8));
		return ((bitSet[byteIndex] & flag) & 0xFF) != 0;
	}
}
