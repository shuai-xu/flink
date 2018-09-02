/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.partitioner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Partitioner that selects all the output channels.
 *
 * @param <T> Type of the elements in the Stream being broadcast
 */
@Internal
public class BroadcastPartitioner<T> extends StreamPartitioner<T> {
	private static final long serialVersionUID = 1L;

	int[] returnArray;
	boolean set;
	int setNumber;

	@Override
	public int[] selectChannels(StreamRecord<T> record,
			int numberOfOutputChannels) {
		if (set && setNumber == numberOfOutputChannels) {
			return returnArray;
		} else {
			this.returnArray = new int[numberOfOutputChannels];
			for (int i = 0; i < numberOfOutputChannels; i++) {
				returnArray[i] = i;
			}
			set = true;
			setNumber = numberOfOutputChannels;
			return returnArray;
		}
	}

	@Override
	public StreamPartitioner<T> copy() {
		return this;
	}

	@Override
	public String toString() {
		return "BROADCAST";
	}
}
