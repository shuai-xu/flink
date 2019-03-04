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

import os.path

import os
import os.path
import tempfile

from pyflink import StreamExecutionEnvironment
from pyflink.table import TableEnvironment
from pyflink.util.type_util import TypesUtil
from pyflink.table.sinks import JavaTableSink
from pyflink.table.functions import ScalarFunction, TableFunction
from pyflink.sql.udf import udf, udtf
from pyflink.sql.data_type import RowType, IntegerType, StringType

import pytest


# TODO: integrate py.test with maven
# ./start-cluster.sh
# set FLINK_HOME


@pytest.fixture(scope="module", autouse=True)
def init_pyflink_env():
    # os.environ['JVM_ARGS'] = ' -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 '
    os.environ['FLINK_BIN_DIR'] = os.environ['FLINK_HOME'] + '/bin'
    print (os.environ['FLINK_BIN_DIR'])


class MyUdf(object):
    def open(self, ctx=None):
        pass

    def eval(self, s):
        s.upper()

    def close(self):
        pass

    @classmethod
    def is_deterministic(cls):
        return True


class PyScalarFunction1(ScalarFunction):
    first_name = None

    def open(self, ctx=None):
        self.first_name = 'KE '

    @udf(data_type=StringType())
    def eval(self, name):
        return self.first_name + name.upper()

    def close(self):
        pass

    @classmethod
    def is_deterministi(cls):
        return True


def test_udf_basic():
    try:
        job_name = 'pyflink Table API Streaming udf testing'
        tmp_dir = tempfile.gettempdir()
        tmp_csv = tmp_dir + '/streaming.csv'
        if os.path.isfile(tmp_csv):
            os.remove(tmp_csv)

        env = StreamExecutionEnvironment.get_execution_environment()
        t_env = TableEnvironment.get_table_environment(env)

        my_udf = PyScalarFunction1()
        t_env.register_function('py_udf', my_udf)
        env.register_python_file('./pyflink/tests/test_udf.py', 'test_udf.py')
        # env.register_python_file('mypyprj.zip')

        ds = env.from_collection(
            [(1L, "ruopu"),
             (2L, "ruochong")])
        t = t_env.from_data_stream(ds, "id, name")

        s = t.select("id, py_udf(name) as fullname")

        # wrap an existing java sink
        java_sink_name = 'org.apache.flink.table.sinks.csv.UpsertCsvTableSink'
        sink_clazz = TypesUtil.class_for_name(java_sink_name)
        sink = JavaTableSink(sink_clazz(tmp_csv, ','))
        s.write_to_sink(sink)

        t_env.execute(job_name)

        expected = 'Add,1,KE RUOPU\n' + 'Add,2,KE RUOCHONG\n'
        print('Expected result:\n')
        with open(tmp_csv, 'r') as f:
            lines = f.read()
            assert lines == expected
            print (lines)
        os.remove(tmp_csv)
    except Exception as err:
        raise err


# def _dump_for_debugging(data, file):
#     try:
#         import os
#         if os.path.isfile(file):
#             os.remove(file)
#         with open(file, 'w') as f:
#             f.write(data)
#     except OSError as e:
#         print ("Error: %s - %s." % (e.filename, e.strerror))
#

# def _load_for_debugging(file):
#     try:
#         import os
#         with open(file, 'r') as f:
#             return f.read()
#     except Exception as e:
#         print ("Error: %s - %s." % (e.filename, e.strerror))
#

# def test_fun1():
#     pickle_file = '/Users/xianda/picklefile'
#     sock_file = '/Users/xianda/tmp/socket_tmp'
#
#     pick = _load_for_debugging(pickle_file)
#     sockfile = _load_for_debugging(sock_file)
#     pv = memoryview(pick)
#     sv = memoryview(sockfile)
#
#     from pyflink.worker_server import do_open_action
#     do_open_action(sv, 6)


def test_row_types():
    types = [StringType(), IntegerType()]
    names = ['name', 'age']
    py_RowType = RowType(types, names)
    java_RowType = TypesUtil.to_java_sql_type(py_RowType)
    dir(java_RowType)