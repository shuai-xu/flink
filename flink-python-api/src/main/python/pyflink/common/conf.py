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

from py4j.java_collections import MapConverter
from pyflink.java_gateway import get_gateway
from pyflink.util.type_util import TypesUtil


__all__ = [
    'ExecutionConfig',
    'Configuration',
    'ParameterTool'
]


class ExecutionConfig(object):
    """
    A wrapper for org.apache.flink.api.common.ExecutionConfig
    """
    def __init__(self, j_exe_conf):
        self._java_conf = j_exe_conf

    def enable_closure_cleaner(self):
        self._java_conf.enableClosureCleaner()
        return self

    def disable_closure_cleaner(self):
        self._java_conf.disableClosureCleaner()
        return self

    def is_closure_cleaner_enabled(self):
        return self._java_conf.isClosureCleanerEnabled()

    def set_auto_watermark_interval(self, interval):
        self._java_conf.setAutoWatermarkInterval(interval)
        return self

    def get_auto_watermark_interval(self):
        return self._java_conf.getAutoWatermarkInterval()

    def set_latency_tracking_interval(self, interval):
        self._java_conf.setLatencyTrackingInterval(interval)
        return self

    def get_latency_tracking_interval(self):
        return self._java_conf.getLatencyTrackingInterval()

    def is_latency_tracking_enabled(self):
        return self._java_conf.isLatencyTrackingEnabled()

    def get_parallelism(self):
        return self._java_conf.getParallelism()

    def set_parallelism(self, parallelism):
        self._java_conf.setParallelism(parallelism)
        return self

    def get_max_parallelism(self):
        return self._java_conf.getMaxParallelism()

    def set_max_parallelism(self, max_parallelism):
        self._java_conf.setMaxParallelism(max_parallelism)
        return self

    def get_task_cancellation_interval(self):
        return self._java_conf.getTaskCancellationInterval()

    def set_task_cancellation_interval(self, interval):
        self._java_conf.setTaskCancellationInterval(interval)
        return self

    def get_task_cancellation_timeout(self):
        return self._java_conf.getTaskCancellationTimeout()

    def set_task_cancellation_timeout(self, interval):
        self._java_conf.setTaskCancellationTimeout(interval)
        return self

    # TODO
    # public void setRestartStrategy(RestartStrategies.RestartStrategyConfiguration restartStrategyConfiguration)
    # public void setExecutionMode(ExecutionMode executionMode)

    def enable_object_reuse(self):
        self._java_conf.enableObjectReuse()
        return self

    def disable_object_reuse(self):
        self._java_conf.disableObjectReuse()
        return self

    def is_object_reuse_enabled(self):
        return self._java_conf.isObjectReuseEnabled()

    def enable_sysout_logging(self):
        self._java_conf.enableSysoutLogging()
        return self

    def disable_sysout_logging(self):
        self._java_conf.disableSysoutLogging()
        return self

    def is_sysout_logging_enabled(self):
        return self._java_conf.isSysoutLoggingEnabled()

    def get_global_job_parameters(self):
        return self._java_conf.getGlobalJobParameters()

    # TODO: registerPojoType


class Configuration(object):
    """
    Lightweight configuration object which stores key/value pairs.

    A wrapper for org.apache.flink.configuration.Configuration
    """

    def __init__(self, j_configuration):
        self._j_configuration = j_configuration

    def get_string(self, key, default_value=None):
        """
        Returns the value associated with the given key as a string.

        :param key: the key pointing to the associated value
        :param default_value: the default value which is returned in case
                there is no value associated with the given key
        :return: the (default) value associated with the given key
        """
        return self._j_configuration.getString(key, default_value)

    def set_string(self, key, value):
        self._j_configuration.setString(key, value)

    def get_integer(self, key, default_value=None):
        return self._j_configuration.getInteger(key, default_value)

    def set_integer(self, key, value):
        self._j_configuration.setInteger(key, value)

    def get_long(self, key, default_value=None):
        return self._j_configuration.getLong(key, default_value)

    def set_long(self, key, value):
        self._j_configuration.setLong(key, value)

    def get_boolean(self, key, default_value=None):
        return self._j_configuration.getBoolean(key, default_value)

    def set_boolean(self, key, value):
        self._j_configuration.setBoolean(key, value)

    def get_float(self, key, default_value=None):
        return self._j_configuration.getFloat(key, default_value)

    def set_float(self, key, value):
        self._j_configuration.setFloat(key, value)

    def get_double(self, key, default_value=None):
        return self._j_configuration.getDouble(key, default_value)

    def set_double(self, key, value):
        self._j_configuration.setDouble(key, value)

    def get_bytes(self, key, default_value=None):
        return self._j_configuration.getBytes(key, default_value)

    def set_bytes(self, key, value):
        """

        :param key:
        :param value: bytearray (2.x) or bytes (3.x)
        :return:
        """
        self._j_configuration.setBytes(key, value)

    def key_set(self):
        return self._j_configuration.keySet()


class ParameterTool(object):

    def __init__(self, j_parameter_tool):
        self._j_parameter_tool = j_parameter_tool

    @classmethod
    def from_args(cls, args):
        j_arr = TypesUtil._convert_py_list_to_java_array(
            'java.lang.String',
            args)
        j_pt = cls._get_java_cls().fromArgs(j_arr)
        return ParameterTool(j_pt)

    @classmethod
    def from_properties_file(cls, path):
        j_pt = cls._get_java_cls().fromPropertiesFile(path)
        return ParameterTool(j_pt)

    @classmethod
    def from_dict(cls, py_dict):
        j_params = MapConverter().convert(py_dict, get_gateway()._gateway_client)
        j_pt = cls._get_java_cls().fromMap(j_params)
        return ParameterTool(j_pt)

    @classmethod
    def from_map(cls, j_map):
        j_pt = cls._get_java_cls().fromMap(j_map)
        return ParameterTool(j_pt)

    @classmethod
    def from_system_properties(cls):
        j_pt = cls._get_java_cls().fromSystemProperties()
        return ParameterTool(j_pt)

    def get_number_of_parameters(self):
        return self._j_parameter_tool.getNumberOfParameters()

    def get(self, key):
        return self._j_parameter_tool.get(key)

    def get_required(self, key):
        return self._j_parameter_tool.getRequired(key)

    def has(self, key):
        return self._j_parameter_tool.has(key)

    def create_properties_file(self, path_to_file, overwrite):
        self._j_parameter_tool.createPropertiesFile(path_to_file, overwrite)

    def merge_with(self, other):
        j_pt = self._j_parameter_tool.mergeWith(other._j_parameter_tool)
        return ParameterTool(j_pt)

    def to_map(self):
        return self._j_parameter_tool.toMap()

    # @classmethod
    # def get_properties(cls):
    #     return cls._get_java_parameter_tool().getProperties()

    @classmethod
    def _get_java_cls(cls):
        return get_gateway().jvm.org.apache.flink.api.java.utils.ParameterTool

