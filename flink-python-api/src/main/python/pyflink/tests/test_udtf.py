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

from pyflink.table.functions import ScalarFunction, TableFunction
from pyflink.sql.udf import udf, udtf
from pyflink import cloudpickle
from pyflink.sql.data_type import StringType


class PyTableFunction(TableFunction):
    @udtf(row_types=[StringType()])
    def eval(self, names):
        for name in names.split('#'):
            super(PyTableFunction, self).collect(name)


def test_f1():
    with open('/Users/xianda/picklefile_udtf', 'w') as f:
        udtf = PyTableFunction()
        udtf_bytes = cloudpickle.dumps(udtf)
        f.write(udtf_bytes)
