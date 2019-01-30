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
import tempfile

from pyflink import StreamExecutionEnvironment
from pyflink.table import TableEnvironment, Table
from pyflink.table.window import Tumble
from pyflink.sql.data_type import *
from pyflink.stream.enums import TimeCharacteristic
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


def test_stream():
    job_name = 'pyflink Table API Streaming example'
    tmp_dir = tempfile.gettempdir()
    tmp_csv = tmp_dir + '/streaming.csv'
    if os.path.isfile(tmp_csv):
        os.remove(tmp_csv)

    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = TableEnvironment.get_table_environment(env)

    ds = env.from_collection(
        [(1L, "aa"),
         (2L, "bb"),
         (3L, "cc"),
         (4L, "dd"),
         (5L, "cc")])
    t = t_env.from_data_stream(ds, "id, name, c.proctime")

    s = t.window(
        Tumble.over("2.rows").on("c").as_("w")) \
        .group_by("w").select("sum(id), max(name)")

    # wrap an existing java sink
    java_sink_name = 'org.apache.flink.table.sinks.csv.UpsertCsvTableSink'
    sink_clazz = TypesUtil.class_for_name(java_sink_name)
    sink = JavaTableSink(sink_clazz(tmp_csv, ','))
    t_env.register_table_sink('mysink', ['c1', 'c2'], [IntegerType(), StringType()], sink)
    s.write_to_sink(sink)

    # in case that you want configure the stream graph
    tc = TimeCharacteristic.ProcessingTime
    stream_graph = t_env.generate_stream_graph()
    stream_graph.set_job_name(job_name)
    stream_graph.set_time_characteristic(tc)
    # p = cpu_count()
    # stream_graph.set_parallelism(1, p)

    # print(stream_graph.get_streaming_plan_as_json())
    env.execute(stream_graph)

    # t_env.execute(job_name)

    expected = 'Add,3,bb\n' + 'Add,7,dd\n'
    print('Expected result:\n')
    with open(tmp_csv, 'r') as f:
        lines = f.read()
        assert lines == expected
        print (lines)
    os.remove(tmp_csv)


def test_batch():
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = TableEnvironment.get_batch_table_environment(env)
    t = t_env.from_collection([1, 2, 3, 4, 5], 'id')
    t.select("id").filter("id > 3").collect()

    print("done")

