/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copysecond ownership.
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

package org.apache.flink.table.runtime.operator.join.stream.state.match;

import org.apache.flink.runtime.state2.keyed.KeyedMapState;
import org.apache.flink.table.codegen.Projection;
import org.apache.flink.table.dataformat.BaseRow;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * The implementation for {@link JoinMatchStateHandler}.
 */
public class JoinKeyNotContainPrimaryKeyMatchStateHandler implements JoinMatchStateHandler {

	private final KeyedMapState<BaseRow, BaseRow, Long> keyedMapState;

	//pk projection
	private final Projection<BaseRow, BaseRow> pkProjection;

	private transient BaseRow currentJoinKey;

	private transient BaseRow pk;

	private transient long currentRowMatchJoinCont;

	public JoinKeyNotContainPrimaryKeyMatchStateHandler(KeyedMapState<BaseRow, BaseRow, Long> keyedMapState,
			Projection<BaseRow, BaseRow> pkProjection) {
		this.keyedMapState = keyedMapState;
		this.pkProjection = pkProjection;
	}

	@Override
	public void extractCurrentRowMatchJoinCount(BaseRow joinKey, BaseRow row, long possibleJoinCnt) {
		this.currentJoinKey = joinKey;
		this.pk = pkProjection.apply(row);

		Long count = keyedMapState.get(joinKey, pk);
		if (count == null) {
			this.currentRowMatchJoinCont = 0;
		} else {
			this.currentRowMatchJoinCont = count;
		}
	}

	@Override
	public long getCurrentRowMatchJoinCnt() {
		return currentRowMatchJoinCont;
	}

	@Override
	public void resetCurrentRowMatchJoinCnt(long joinCnt) {
		keyedMapState.add(currentJoinKey, pk, joinCnt);
		this.currentRowMatchJoinCont = joinCnt;
	}

	@Override
	public void updateRowMatchJoinCnt(BaseRow joinKey, BaseRow baseRow, long joinCnt) {
		keyedMapState.add(joinKey, pkProjection.apply(baseRow), joinCnt);
	}

	@Override
	public void addRowMatchJoinCnt(BaseRow joinKey, BaseRow baseRow, long joinCnt) {
		BaseRow mapKey = pkProjection.apply(baseRow);
		long count = keyedMapState.get(joinKey, mapKey);
		keyedMapState.add(joinKey, mapKey, joinCnt + count);
	}

	@Override
	public void remove(BaseRow joinKey, BaseRow baseRow) {
		keyedMapState.remove(joinKey, pkProjection.apply(baseRow));
	}

	@Override
	public void remove(BaseRow joinKey) {
		keyedMapState.remove(joinKey);
	}

	@Override
	public void removeAll(BaseRow joinKey, Set<BaseRow> keys) {
		Set<BaseRow> pks = new HashSet<>();
		for (BaseRow baseRow: keys) {
			pks.add(pkProjection.apply(baseRow));
		}
		keyedMapState.removeAll(pks);
	}

	@Override
	public void addAll(BaseRow joinKey, Map<BaseRow, Long> kvs) {
		Map<BaseRow, Long> pkMap = new HashMap<>();
		for (Map.Entry<BaseRow, Long> entry: kvs.entrySet()) {
			pkMap.put(pkProjection.apply(entry.getKey()), entry.getValue());
		}
		keyedMapState.addAll(joinKey, pkMap);
	}
}
