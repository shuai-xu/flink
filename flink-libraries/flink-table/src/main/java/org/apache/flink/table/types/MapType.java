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

package org.apache.flink.table.types;

/**
 * Map type.
 */
public class MapType implements InternalType {

	private final InternalType keyType;
	private final InternalType valueType;

	public MapType(InternalType keyType, InternalType valueType) {
		this.keyType = keyType;
		this.valueType = valueType;
	}

	public InternalType getKeyType() {
		return keyType;
	}

	public InternalType getValueType() {
		return valueType;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		MapType mapType = (MapType) o;

		return keyType.equals(mapType.keyType) &&
				valueType.equals(mapType.valueType);
	}

	@Override
	public int hashCode() {
		int result = keyType.hashCode();
		result = 31 * result + valueType.hashCode();
		return result;
	}
}
