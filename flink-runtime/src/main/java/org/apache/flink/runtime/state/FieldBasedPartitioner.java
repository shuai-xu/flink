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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

/**
 * An implementation of {@link Partitioner} which partitions rows by the hash
 * code of one of its fields.
 */
public class FieldBasedPartitioner implements Partitioner<Row> {

	private static final long serialVersionUID = 1L;

	/**
	 * The field on whose hash code the partitioning depends.
	 */
	private final int field;

	/**
	 * The partitioner on the objects of the field.
	 */
	private final Partitioner<Object> partitioner;

	public FieldBasedPartitioner(int field, Partitioner<Object> partitioner) {
		Preconditions.checkNotNull(partitioner);

		this.field = field;
		this.partitioner = partitioner;
	}

	@Override
	public int partition(Row key, int numPartitions) {
		Preconditions.checkNotNull(key);
		Preconditions.checkArgument(key.getArity() >= field);

		return partitioner.partition(key.getField(field), numPartitions);
	}

	public int getField() {
		return field;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		FieldBasedPartitioner that = (FieldBasedPartitioner) o;
		return field == that.field && partitioner.equals(that.partitioner);
	}

	@Override
	public int hashCode() {
		return field;
	}

	@Override
	public String toString() {
		return "FieldBasedHashPartitioner{" +
			"field=" + field +
			", partitioner=" + partitioner +
			"}";
	}
}
