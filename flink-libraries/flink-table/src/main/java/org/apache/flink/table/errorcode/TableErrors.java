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

package org.apache.flink.table.errorcode;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.text.MessageFormat;

/**
 * wrapper class for table error code instance.
 */
public class TableErrors {

	public static final TableErrorCode INST = (TableErrorCode) createProxy(TableErrorCode.class);

	/**
	 * create proxy instance for one module's error code interface.
	 *
	 * @param clazz interface that has error code definitions for one module.
	 * @return instance of the interface that can be used by developer for specifying error code
	 * (for now, it is for dumping error code and its cause&action message)
	 * when throwing exceptions
	 */
	public static Object createProxy(Class clazz) {

		return Proxy.newProxyInstance(
			clazz.getClassLoader(),
			clazz.isInterface() ? new Class[]{clazz} : clazz.getInterfaces(),
			(obj, method, args) -> {
				checkParam(method, args);
				return assemblyErrCodeString(method, args);
			});

	}

	/**
	 * Parameter check when invoking method.
	 *
	 * @param method
	 * @param args
	 */
	protected static void checkParam(Method method, Object[] args) {
		TableErrorCode.ErrCode errCode = method.getAnnotation(TableErrorCode.ErrCode.class);
		String errDetail = errCode.details();
		String errCause = errCode.cause();

		MessageFormat format1 = new MessageFormat(errDetail);
		MessageFormat format2 = new MessageFormat(errCause);

		if (args == null || args.length == 0) {
			if ((format1.getFormatsByArgumentIndex() != null && format1.getFormatsByArgumentIndex().length > 0)
				|| (format2.getFormatsByArgumentIndex() != null && format2.getFormatsByArgumentIndex().length > 0)) {
				throw new AssertionError("mismatched parameter length between "
					+ method.getName() + " and its annotation @ErrCode");
			}
		} else {
			if ((format1.getFormatsByArgumentIndex() != null && format1.getFormatsByArgumentIndex().length > args.length)
				|| format1.getFormatsByArgumentIndex() == null
				|| (format2.getFormatsByArgumentIndex() != null && format2.getFormatsByArgumentIndex().length > args.length)) {
				throw new AssertionError("mismatched parameter length between "
					+ method.getName() + " and its annotation @ErrCode");
			}
		}
	}

	/**
	 * assembly error code messages.
	 *
	 * @param method error code related function declared in error interface
	 * @param args   args passed to that related function
	 * @return error code messages containing code id, cause and action.
	 */
	protected static String assemblyErrCodeString(Method method, Object[] args) {
		TableErrorCode.ErrCode errCode = method.getAnnotation(TableErrorCode.ErrCode.class);
		String errId = errCode.codeId();
		String errCause = errCode.cause();
		String errDetail = errCode.details();
		String errAction = errCode.action();

		if (args != null && args.length != 0) {
			MessageFormat format1 = new MessageFormat(errDetail);
			errDetail = format1.format(args);

			MessageFormat format2 = new MessageFormat(errCause);
			errCause = format2.format(args);
		}

		errId = prettyPrint(errId);
		errCause = prettyPrint(errCause);
		errDetail = prettyPrint(errDetail);
		errAction = prettyPrint(errAction);

		String msg = "\n************\n"
			//"\n*******************************************************\n"
			+ "ERR_ID:\n"
			+ errId + "\n"
			+ "CAUSE:\n"
			+ errCause + "\n"
			+ "ACTION:\n"
			+ errAction + "\n"
			+ "DETAIL:\n"
			+ errDetail + "\n"
			//+ "*******************************************************";
			+ "************";
		return msg;
	}

	/**
	 * Print out error code in a pretty way.
	 *
	 * @param str
	 * @return
	 */
	public static String prettyPrint(String str) {
		if (str != null && str.length() != 0) {
			str = indent(5) + str.replaceAll("\n", "\n" + indent(5));
		}

		return str;
	}

	/**
	 * Get multiple indent.
	 *
	 * @param cnt
	 * @return
	 */
	public static String indent(int cnt) {
		if (cnt <= 0) {
			return null;
		}
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < cnt; i++) {
			sb.append(" ");
		}
		return sb.toString();
	}
}
