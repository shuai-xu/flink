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

package com.alibaba.blink.launcher.autoconfig;

import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;

import com.alibaba.blink.launcher.autoconfig.errorcode.AutoConfigErrors;

/**
 * StreamGraph Configurer which applies {@link StreamGraphProperty} to {@link StreamGraph}.
 */
public class StreamGraphConfigurer {
	public static void configure(StreamGraph graph, StreamGraphProperty property) {
		if (graph == null || property == null) {
			return;
		}
		for (StreamNodeProperty nodeProperty : property.getStreamNodeProperties()) {
			StreamNode node = graph.getStreamNode(nodeProperty.getId());
			if (node == null) {
				throw new UnexpectedConfigurationException(
						AutoConfigErrors.INST.cliAutoConfTransformationCfgSetError(
								"Fail to apply resource configuration file, node " + nodeProperty.toString() + " not found."));
			}
			nodeProperty.apple(node);
		}
		for (StreamEdgeProperty edgeProperty : property.getStreamEdgeProperties()) {
			StreamEdge edge = graph.getStreamEdges(edgeProperty.getSource(), edgeProperty.getTarget())
					.get(edgeProperty.getIndex());
			if (edge == null) {
				throw new UnexpectedConfigurationException(
						AutoConfigErrors.INST.cliAutoConfTransformationCfgSetError(
								"Fail to apply resource configuration file, edge " + edgeProperty.toString()) + " not found.");
			}
			edgeProperty.apply(edge);
		}
	}

}
