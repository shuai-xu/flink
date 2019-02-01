# ###############################################################################
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
import os
import os.path

from pyflink.sql.data_type import *
from pyflink.util.type_util import TypesUtil
from pyflink.table.sinks import JavaTableSink

import pytest


# TODO: integrate py.test with maven
# ./start-cluster.sh
# set FLINK_HOME


@pytest.fixture(scope="module", autouse=True)
def init_pyflink_env():
    os.environ['FLINK_BIN_DIR'] = os.environ['FLINK_HOME'] + '/bin'
    print (os.environ['FLINK_BIN_DIR'])


def test_sink_conf():
    java_sink_name = 'org.apache.flink.table.sinks.csv.UpsertCsvTableSink'
    sink_clazz = TypesUtil.class_for_name(java_sink_name)
    sink = JavaTableSink(sink_clazz('tmp.csv', ','))
    sink.configure(['a', 'b'],  [StringType(), IntegerType()])
