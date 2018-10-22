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

import org.apache.flink.util.InstantiationUtil

import org.apache.commons.codec.binary.Base64

/**
 * Utils for serializing object to a base64 encoded string and de-serializing.
 */
object SerializationUtil {

  def serializeObject(ob: Object): String = {
    val byteArray = InstantiationUtil.serializeObject(ob)
    Base64.encodeBase64URLSafeString(byteArray)
  }

  def deSerializeObject(data: String): Object =
    deSerializeObject(data, Thread.currentThread.getContextClassLoader)

  def deSerializeObject(data: String, classLoader: ClassLoader): Object = {
    val byteData = Base64.decodeBase64(data)
    InstantiationUtil
    .deserializeObject[Object](byteData, classLoader)
  }

  def string2HexString(str: String): String = {
    str.map(_.toHexString).mkString("")
  }

  def hexString2String(str: String): String = {
    str.sliding(2, 2).map(s => Integer.parseInt(s, 16).asInstanceOf[Char].toString).mkString("")
  }
}
