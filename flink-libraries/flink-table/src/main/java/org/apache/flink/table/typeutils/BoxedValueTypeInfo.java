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

package org.apache.flink.table.typeutils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.dataformat.BoxedValue;

/**
 * TypeInfo for BoxedValue.
 */
public class BoxedValueTypeInfo extends TypeInformation<BoxedValue<?>> {

	private final TypeInformation<?> valueTypeInfo;

	public BoxedValueTypeInfo(TypeInformation<?> valueTypeInfo) {
		this.valueTypeInfo = valueTypeInfo;
	}

	@Override
	public boolean isBasicType() {
		return false;
	}

	@Override
	public boolean isTupleType() {
		return false;
	}

	@Override
	public int getArity() {
		return 1;
	}

	@Override
	public int getTotalFields() {
		return 1;
	}

	@Override
	public Class<BoxedValue<?>> getTypeClass() {
		Class<?> clazz = BoxedValue.class;
		//noinspection unchecked
		return (Class<BoxedValue<?>>) clazz;
	}

	@Override
	public boolean isKeyType() {
		return true;
	}

	@Override
	public TypeSerializer<BoxedValue<?>> createSerializer(ExecutionConfig config) {
		//noinspection unchecked
		return new BoxedValueSerializer(valueTypeInfo.createSerializer(config));
	}

	@Override
	public String toString() {
		return "BoxedValue<" + valueTypeInfo.toString() + ">";
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof BoxedValueTypeInfo) {
			BoxedValueTypeInfo other = (BoxedValueTypeInfo) obj;
			return this.valueTypeInfo.equals(other.valueTypeInfo);
		}
		return false;
	}

	@Override
	public int hashCode() {
		return valueTypeInfo.hashCode();
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof BoxedValueTypeInfo;
	}
}
