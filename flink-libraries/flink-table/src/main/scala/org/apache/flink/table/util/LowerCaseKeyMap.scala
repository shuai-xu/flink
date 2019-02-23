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

package org.apache.flink.table.util

import java.util.Locale

/**
  * Builds a map in which keys are case insensitive. This map will make keys lowercase before
  * matching . The primary constructor is marked private to avoid nested case-insensitive map
  * creation.
  */
class LowerCaseKeyMap[T] private (val originalMap: Map[String, T]) extends Map[String, T]
  with Serializable {

  val keyLowerCasedMap = originalMap.map(kv => kv.copy(_1 = kv._1.toLowerCase(Locale.ROOT)))

  override def get(k: String): Option[T] = keyLowerCasedMap.get(k.toLowerCase(Locale.ROOT))

  override def contains(k: String): Boolean =
    keyLowerCasedMap.contains(k.toLowerCase(Locale.ROOT))

  override def +[B1 >: T](kv: (String, B1)): Map[String, B1] = {
    new LowerCaseKeyMap(originalMap + kv)
  }

  override def iterator: Iterator[(String, T)] = keyLowerCasedMap.iterator

  override def -(key: String): Map[String, T] = {
    new LowerCaseKeyMap(originalMap.filter(!_._1.equalsIgnoreCase(key)))
  }
}

object LowerCaseKeyMap {
  def apply[T](params: Map[String, T]): LowerCaseKeyMap[T] = params match {
    case caseSensitiveMap: LowerCaseKeyMap[T] => caseSensitiveMap
    case _ => new LowerCaseKeyMap(params)
  }
}

