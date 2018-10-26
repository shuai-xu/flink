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

package com.alibaba.blink.launcher.util;

import com.alibaba.blink.launcher.autoconfig.AbstractJsonSerializable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * CollectionUtil.
 */
public class CollectionUtil {

	public static <T, K, V> Collector<T, ?, LinkedHashMap<K, V>> toLinkedHashMap(
		java.util.function.Function<? super T, ? extends K> keyMapper,
		java.util.function.Function<? super T, ? extends V> valueMapper) {

		return Collector.of(
			LinkedHashMap::new,
			(LinkedHashMap<K, V> map, T x) -> map.put(keyMapper.apply(x), valueMapper.apply(x)),
			(map1, map2) -> {
				throw new UnsupportedOperationException("parallel stream is not supported");
			});
	}

	public static <K, V extends AbstractJsonSerializable> LinkedHashMap<K, V> copy(Map<K, V> map) {
		return map == null
			? new LinkedHashMap<>()
			: map.entrySet().stream().collect(toLinkedHashMap(e -> e.getKey(), e -> e.getValue().copy()));
	}

	public static <E extends AbstractJsonSerializable> ArrayList<E> copy(List<E> l) {
		return l == null
			? new ArrayList<>()
			: l.stream().map(e -> e.<E>copy()).collect(Collectors.toCollection(ArrayList::new));
	}

}
