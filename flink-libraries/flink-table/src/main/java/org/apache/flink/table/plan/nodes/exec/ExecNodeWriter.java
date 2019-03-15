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

package org.apache.flink.table.plan.nodes.exec;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;

/**
 * Callback for an ExecNode to dump itself.
 *
 * <p>It is used for generating EXPLAIN PLAN output, and also for serializing
 * a tree of ExecNode to JSON.
 */
public interface ExecNodeWriter {

	/**
	 * Prints an explanation of a node, with a list of (term, value) pairs.
	 *
	 * <p>Each sub-class of {@link ExecNode} calls {@link #input(String, ExecNode)}
	 * and {@link #item(String, Object)} to declare term-value pairs.
	 *
	 * @param node      exec node
	 * @param valueList List of term-value pairs
	 */
	void explain(ExecNode<?, ?> node, List<Tuple2<String, Object>> valueList);

	/**
	 * Adds an input to the explanation of the current node.
	 *
	 * @param term  Term for input, e.g. "left" or "input #1".
	 * @param input Input exec node.
	 */
	ExecNodeWriter input(String term, ExecNode<?, ?> input);

	/**
	 * Adds an attribute to the explanation of the current node.
	 *
	 * @param term  Term for attribute, e.g. "joinType"
	 * @param value Attribute value
	 */
	ExecNodeWriter item(String term, Object value);

	/**
	 * Adds an input to the explanation of the current node, if a condition
	 * holds.
	 */
	ExecNodeWriter itemIf(String term, Object value, boolean condition);

	/**
	 * Writes the completed explanation.
	 */
	ExecNodeWriter done(ExecNode<?, ?> node);
}
