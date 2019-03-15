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

package org.apache.flink.table.plan.util;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.plan.nodes.exec.ExecNode;
import org.apache.flink.table.plan.nodes.exec.ExecNodeWriter;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableList;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * Converts an ExecNode to string with only the information from the ExecNode itself
 * without the information from its inputs.
 */
public class ExecNodeInfoWriter implements ExecNodeWriter {

	public static final String STREAM_EXEC = "StreamExec";
	public static final String BATCH_EXEC = "BatchExec";

	private final PrintWriter pw;
	private final String execNodeTypeNamePrefix;
	private final boolean notOutputIfEmpty;

	private final List<Tuple2<String, Object>> values = new ArrayList<>();

	public ExecNodeInfoWriter(PrintWriter pw, String execNodeTypeNamePrefix, boolean notOutputIfEmpty) {
		this.pw = pw;
		this.execNodeTypeNamePrefix = execNodeTypeNamePrefix;
		this.notOutputIfEmpty = notOutputIfEmpty;
	}

	@Override
	public void explain(ExecNode<?, ?> node, List<Tuple2<String, Object>> valueList) {
		StringBuilder s = new StringBuilder();
		String execNodeTypeName = getExecNodeTypeName(node);
		if (execNodeTypeName.startsWith(execNodeTypeNamePrefix)) {
			s.append(execNodeTypeName.substring(execNodeTypeNamePrefix.length()));
		} else {
			throw new IllegalArgumentException(
				"Current ExecNode class name is not start with \"" + execNodeTypeName + "\"");
		}
		int j = 0;
		for (Tuple2<String, Object> value : valueList) {
			if (j++ == 0) {
				s.append("(");
			} else {
				s.append(", ");
			}
			s.append(value.f0).append("=[").append(value.f1).append("]");
		}
		if (j > 0) {
			s.append(")");
		}
		pw.print(s.toString());
	}

	@Override
	public ExecNodeWriter input(String term, ExecNode<?, ?> input) {
		// input nodes do not need to be displayed
		return this;
	}

	@Override
	public ExecNodeWriter item(String term, Object value) {
		values.add(Tuple2.of(term, value));
		return this;
	}

	@Override
	public ExecNodeWriter itemIf(String term, Object value, boolean condition) {
		if (condition) {
			item(term, value);
		}
		return this;
	}

	@Override
	public ExecNodeWriter done(ExecNode<?, ?> node) {
		final List<Tuple2<String, Object>> valuesCopy = ImmutableList.copyOf(values);
		values.clear();
		if (notOutputIfEmpty && !valuesCopy.isEmpty()) {
			explain(node, valuesCopy);
		}
		pw.flush();
		return this;
	}

	/**
	 * Returns the name of the node's class, sans package name.
	 */
	private String getExecNodeTypeName(ExecNode<?, ?> node) {
		String className = node.getClass().getName();
		int i = className.lastIndexOf("$");
		if (i >= 0) {
			return className.substring(i + 1);
		}
		i = className.lastIndexOf(".");
		if (i >= 0) {
			return className.substring(i + 1);
		}
		return className;
	}
}
