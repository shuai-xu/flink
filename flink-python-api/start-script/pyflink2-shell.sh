#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

FLINK_CLASSPATH=`constructFlinkClassPath`

SHELL_DRIVER="org.apache.flink.api.PythonShellGatewayServer"

PYFLINK_INTERPRETER="${PYFLINK_INTERPRETER:-"python"}"

# So that python can find out Flink's Jars
export FLINK_BIN_DIR=$FLINK_BIN_DIR

# Add pyflink & py4j to PYTHONPATH
export PYTHONPATH="${FLINK_ROOT_DIR}/lib/python/pyflink.zip:$PYTHONPATH"
export PYTHONPATH="${FLINK_ROOT_DIR}/lib/python/py4j-0.10.8-src.zip:$PYTHONPATH"

# -i: interactive
# -m: execute shell.py in the zip package
${PYFLINK_INTERPRETER} -i -m pyflink.shell
