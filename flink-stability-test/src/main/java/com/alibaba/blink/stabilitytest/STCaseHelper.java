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

package com.alibaba.blink.stabilitytest;

import org.jboss.byteman.agent.Main;
import org.jboss.byteman.rule.Rule;
import org.jboss.byteman.rule.exception.EarlyReturnException;
import org.jboss.byteman.rule.exception.ThrowException;
import org.jboss.byteman.rule.helper.Helper;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;

public class STCaseHelper extends Helper {
	private static final String INITIALIZE_METHOD_NAME = "initialize";

	/** Mapping from key(ClassLoader + className + methodName) to ST(stability-test) method */
	private static ConcurrentHashMap<String, Method> methodMap = new ConcurrentHashMap<String, Method>();

	public STCaseHelper(Rule rule) {
		super(rule);
	}

	public Object invokeSTMethod(String stClassName, String stMethodName, Object... args)
		throws NoSuchMethodException, IllegalAccessException, ClassNotFoundException {
		if (stClassName == null || stMethodName == null) {
			throw new IllegalArgumentException("[stability test] the className or methodName of stability-test case can't be null.");
		}

		if (args.length < 1 || args[0] == null) {
			throw new IllegalArgumentException("[stability test] the target class object is missing or null.");
		}
		Object targetClassObj = args[0];

		// Call initialize() method,
		// all ST-Case class should implements the static method 'void initialize(Object arg0, Object arg1)'
		invokeMethod(stClassName, INITIALIZE_METHOD_NAME, targetClassObj, new Boolean[1], System.getProperties(), Main.BYTEMAN_PREFIX);

		// Call ST-Case method,
		// all ST-Case method should be static
		Boolean[] isEarlyReturn = new Boolean[1];
		Object result = invokeMethod(stClassName, stMethodName, targetClassObj, isEarlyReturn, args);
		if (isEarlyReturn[0] != null && isEarlyReturn[0]) {
			throw new EarlyReturnException("", result);
		}
		return result;
	}

	private Object invokeMethod(String className, String methodName, Object defineClassLoaderObj, Boolean[] isEarlyReturn, Object... args)
			throws ThrowException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException {
		// Disable recursive triggering of rules in the current thread,
		// and enable by the rule engine after rule execution has completed
		Rule.disableTriggersInternal();

		// Lookup the invokable method of the corresponding stability-test class by name
		final ClassLoader loader = defineClassLoaderObj.getClass().getClassLoader();

		String methodKey = getMethodKey(loader, className, methodName);
		Method method = methodMap.get(methodKey);
		if (method == null) {
			Class<?> cls = Class.forName(className, false, loader);

			int paramLen = args.length, startIndex = 0;
			if (!INITIALIZE_METHOD_NAME.equals(methodName)) {
				paramLen += 1;
				startIndex = 1;
			}
			Class<?>[] parameterTypes = new Class<?>[paramLen];
			if (paramLen > args.length) {
				parameterTypes[0] = Boolean[].class;
			}
			for (int i = startIndex; i < parameterTypes.length; i++) {
				parameterTypes[i] = Object.class;
			}

			method = cls.getDeclaredMethod(methodName, parameterTypes);
			Method prevMethod = methodMap.putIfAbsent(methodKey, method);
			if (prevMethod != null) {
				method = prevMethod;
			}
		}

		try {
			// invoke the static method
			if (INITIALIZE_METHOD_NAME.equals(methodName)) {
				return method.invoke(null, args);
			}

			Object[] newArgs = new Object[args.length + 1];
			newArgs[0] = isEarlyReturn;
			for (int i = 0; i < args.length; i++) {
				newArgs[i + 1] = args[i];
			}
			return method.invoke(null, newArgs);
		} catch (InvocationTargetException ite) {
			Throwable throwable = ite.getCause();
			if (throwable == null) {
				throwable = ite;
			}
			throw new ThrowException(throwable);
		} catch (Throwable t) {
			throw new ThrowException(t);
		}
	}

	private static String getMethodKey(ClassLoader loader, String className, String methodName) {
		return String.format("%s:%s:%s", loader, className , methodName);
	}
}
