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
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;
import org.apache.flink.streaming.runtime.partitioner.RescalePartitioner;

import com.alibaba.blink.launcher.autoconfig.errorcode.AutoConfigErrors;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashSet;
import java.util.Set;

/**
 * StreamEdge Properties.
 */
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonIgnoreProperties(value = { "side"})
public class StreamEdgeProperty extends AbstractJsonSerializable implements Comparable<StreamEdgeProperty> {
	private int source;
	private int target;
	private int index = 0;
	@JsonProperty("ship_strategy")
	private String shipStrategy = "FORWARD";

	private static final Set<String> STRATEGY_UPDATABLE = new HashSet<>();
	static {
		STRATEGY_UPDATABLE.add("FORWARD");
		STRATEGY_UPDATABLE.add("REBALANCE");
		STRATEGY_UPDATABLE.add("RESCALE");
	}

	public StreamEdgeProperty() {
	}

	public StreamEdgeProperty(int source, int target, int index) {
		this.source = source;
		this.target = target;
		this.index = index;
	}

	public int getSource() {
		return source;
	}

	public void setSource(int source) {
		this.source = source;
	}

	public int getTarget() {
		return target;
	}

	public void setTarget(int target) {
		this.target = target;
	}

	public int getIndex() {
		return index;
	}

	public String getShipStrategy() {
		return shipStrategy;
	}

	public void setShipStrategy(String shipStrategy) {
		this.shipStrategy = shipStrategy;
	}

	@Override
	public int compareTo(StreamEdgeProperty streamEdgeProperty) {
		int r = this.source - streamEdgeProperty.source;
		if (r == 0) {
			r = this.target - streamEdgeProperty.target;
		}
		return r;
	}

	public void apply(StreamEdge edge) {
		if (shipStrategy.equalsIgnoreCase("FORWARD")) {
			edge.setPartitioner(new ForwardPartitioner<>());
		} else if (shipStrategy.equalsIgnoreCase("RESCALE")) {
			edge.setPartitioner(new RescalePartitioner<>());
		} else if (shipStrategy.equalsIgnoreCase("REBALANCE")) {
			edge.setPartitioner(new RebalancePartitioner<>());
		}
	}

	public void update(StreamEdgeProperty property) {
		if (STRATEGY_UPDATABLE.contains(this.shipStrategy.toUpperCase()) &&
				STRATEGY_UPDATABLE.contains(property.shipStrategy.toUpperCase())) {
			this.shipStrategy = property.getShipStrategy();
		} else if (!this.shipStrategy.equalsIgnoreCase(property.getShipStrategy())) {
			throw new UnexpectedConfigurationException(
					AutoConfigErrors.INST.cliAutoConfTransformationCfgSetError(
							"Fail to apply resource configuration file to edge " + this + "."));
		}
	}
}
