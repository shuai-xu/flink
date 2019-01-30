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
from pyflink.java_gateway import get_gateway

__all__ = ['TableConfig']


class TableConfig(object):
    """
    Wrapper for org.apache.flink.table.api.TableConfig
    """

    # TODO:

    def __init__(self, j_conf):
        self._j_table_config = j_conf

    def set_timezone(self, tz):
        j_tz = get_gateway().jvm.java.util.TimeZone.getTimeZone(tz)
        self._j_table_config.setTimeZone(j_tz)

    def get_timezone(self):
        return self._j_table_config.getTimeZone()

    def get_null_check(self):
        return self._j_table_config.get_null_check()

    def set_null_check(self, null_check):
        self._j_table_config.setNullCheck(null_check)

    def set_null_check(self, null_check):
        self._j_table_config.setNullCheck(null_check)

    def get_conf(self):
        """

        :rtype: org.apache.flink.configuration.Configuration
        :return:
        """
        return self._j_table_config.getConf()

    def set_conf(self, conf):
        """
        :type conf: org.apache.flink.configuration.Configuration
        :param conf:
        """
        self._j_table_config.setConf(conf)

    def get_calcite_config(self):
        """
        :rtype: org.apache.flink.table.calcite.CalciteConfig
        :return:
        """
        return self._j_table_config.getCalciteConfig()

    def set_calcite_config(self, calcite_config):
        """
        :type conf: org.apache.flink.configuration.Configuration
        :param conf:
        """
        self._j_table_config.setCalciteConfig(calcite_config)
