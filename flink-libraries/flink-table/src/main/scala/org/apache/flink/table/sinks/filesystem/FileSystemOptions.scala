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

package org.apache.flink.table.sinks.filesystem

import org.apache.flink.configuration.ConfigOption
import org.apache.flink.configuration.ConfigOptions.key

import java.util

/** Config options for file system sink. */
object FileSystemOptions {
  // --------------- required conf -----------------------------------------------------
  // write path for filesystem.
  val PATH: ConfigOption[String] = key("path").noDefaultValue()
  // file format.
  val FORMAT: ConfigOption[String] = key("format".toLowerCase).noDefaultValue()

  // --------------- optional conf -----------------------------------------------------
  val TIME_ZONE: ConfigOption[String] = key("timeZone".toLowerCase)
    .defaultValue("UTC")

  val SUPPORTED_KEYS: util.List[String] = util.Arrays.asList(
    TIME_ZONE.key(),
    PATH.key(),
    FORMAT.key())
}
