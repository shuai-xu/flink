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
from abc import ABCMeta, abstractmethod
from pyflink.worker_server import do_table_func_collect

__all__ = [
    'JavaScalarFunction',
    'JavaAggregateFunction',
    'JavaTableFunction',
    'ScalarFunction',
    'TableFunction'
]


class JavaScalarFunction(object):
    """
    A wrapper of existing java scalar function
    """
    def __init__(self, j_scalar_func):
        self.j_func = j_scalar_func


class JavaAggregateFunction(object):
    """
     A wrapper of existing java AggregateFunction
    """
    def __init__(self, j_agg_func):
        self.j_func = j_agg_func


class JavaTableFunction(object):
    """
    A wrapper of existing java TableFunction
    """

    def __init__(self, j_table_func):
        self.j_func = j_table_func


class ScalarFunction(object):
    """
    An interface for writing python scalar function
    # TODO (WIP)
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def eval(self, *args):
        """Please define your implementation"""

    def open(self, *args):
        pass

    def close(self, *args):
        pass

    @classmethod
    def is_deterministic(cls):
        return True


class TableFunction(object):
    """
    An interface for writing python Table function
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def eval(self, *args):
        """Please define your implementation"""

    def open(self, *args):
        pass

    def close(self, *args):
        pass

    def collect(self, *args):
        """
        Note that this is a system interface, no need to implement this interface
        At runtime, it will be override by Flink.
        :param args:
        :return:
        """
        do_table_func_collect(self, *args)

    @classmethod
    def is_deterministic(cls):
        return True
