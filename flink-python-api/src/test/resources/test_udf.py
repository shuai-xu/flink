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

import sys
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
        tmp_csv = tmp_dir + '/udf.csv'
        if os.path.isfile(tmp_csv):
            os.remove(tmp_csv)
        if os.path.isdir(tmp_csv):
            import shutil
            shutil.rmtree(tmp_csv)

        env = StreamExecutionEnvironment.get_execution_environment()
        t_env = TableEnvironment.get_table_environment(env)
        env.set_parallelism(1)

        my_udf = PyScalarFunction1()
        t_env.register_function('py_udf', my_udf)
        env.register_python_file('./test_udf.py', 'test_udf.py')

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
            print(lines)
        os.remove(tmp_csv)
    except Exception as err:
        raise err


if __name__ == '__main__':
    try:
        test_udf_basic()
    except Exception as err:
        import traceback
        traceback.print_exc()
        sys.exit(1)
