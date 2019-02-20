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
from pyflink.stream.datastream import DataStream, DataStreamSource
from pyflink.stream.functions.source import JavaSourceFunction
from pyflink.common import ExecutionConfig
from pyflink.common.cache import DistributedCache
from pyflink.common.job_result import JobExecutionResult, JobSubmissionResult
from pyflink.util.type_util import TypesUtil

__all__ = [
    'StreamExecutionEnvironment',
    'LocalStreamEnvironment',
    'RemoteStreamEnvironment',
    ]

# TODO: org.apache.flink.streaming.api.environment
# * CheckpointConfig


# TODO: type_limit ...
class StreamExecutionEnvironment(object):

    """
    org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
    """

    _j_env = None

    def __init__(self, j_env):
        """
        :param j_env:
        """
        self._j_env = j_env

    def get_config(self):
        """

        :return:
        """
        return ExecutionConfig(self._j_env.getConfig())

    def get_cached_files(self):
        j_files = self._j_env.getCachedFiles()
        py_list = []
        types = [unicode, DistributedCache.DistributedCacheEntry]
        for i in range(j_files):
            py_tuple = TypesUtil._java_tuple_to_tuple(j_files[i], types)
            py_list.append(py_tuple)
        return py_list

    def get_parallelism(self):
        return self._j_env.getParallelism()

    def get_max_parallelism(self):
        return self._j_env.getMaxParallelism()

    def add_job_listener(self, job_listener):
        # not supported now
        pass

    # @PublicEvolving
    def disable_operator_chaining(self):
        self._j_env.disableOperatorChaining()
        return self

    def set_multi_head_chain_mode(self):
        self._j_env.setMultiHeadChainMode()
        return self

    def is_multi_head_chain_mode(self):
        return self._j_env.isMultiHeadChainMode()

    def disable_checkpointing(self):
        self._j_env.disableCheckpointing()
        return self

    def enable_slot_sharing(self):
        self._j_env.enableSlotSharing()
        return self

    def disable_slot_sharing(self):
        self._j_env.disableSlotSharing()
        return self

    def is_slot_sharing_enabled(self):
        return self._j_env.isSlotSharingEnabled()

    def get_default_resources(self):
        # not supported now
        pass

    def set_default_resources(self):
        # not supported now
        pass

    def get_checkpoint_config(self):
        # TODO: wrap CheckpointConfig ?
        return self._j_env.getCheckpointConfig()

    def enable_checkpointing(self, interval=500, mode=None, force=None):
        if mode is None:
            self._j_env.setCheckpointingMode(interval)
        elif force is None:
            self._j_env.setCheckpointingMode(interval, mode)
        else:
            self._j_env.enableCheckpointing(interval, mode, force)
        return self

    def disable_checkpointing(self):
        self._j_env.disableCheckpointing()
        return self

    def get_checkpoint_interval(self):
        return self._j_env.getCheckpointInterval()

    def is_force_checkpointing(self):
        return self._j_env.isForceCheckpointing()

    def get_checkpointing_mode(self):
        # TODO: wrap CheckpointingMode
        return self._j_env.getCheckpointingMode()

    # def setStateBackend(backend: StateBackend):
    # def getStateBackend
    # Not supported now

    # TODO:  define RestartStrategies classes
    # def setRestartStrategy
    # getRestartStrategy

    def set_number_of_execution_retries(self, num_retries):
        self._j_env.setNumberOfExecutionRetries(num_retries)
        return self

    def get_number_of_execution_retries(self):
        self._j_env.getNumberOfExecutionRetries()

    def get_custom_configuration(self):
        self._j_env.getCustomConfiguration()

    def set_parallelism(self, parallelism):
        self._j_env.setParallelism(parallelism)
        return self

    def set_max_parallelism(self, parallelism):
        self._j_env.setMaxParallelism(parallelism)
        return self

    # TODO:
    # def addDefaultKryoSerializer(self, serializer):
    # def registerTypeWithKryoSerializer
    # Not supported now

    # def registerType
    # not supported now

    def generate_sequence(self, from_num, to_num):
        j_ds = self._j_env.generateSequence(from_num, to_num)
        return DataStream(j_ds)

    def from_collection(self, data):
        if type(data[0]) is tuple:
            java_list = TypesUtil._convert_tuple_list(data)
        else:
            java_list =TypesUtil._convert_pylist_to_java_list(data)
        j_ds_source = self._j_env.fromCollection(java_list)
        return DataStreamSource(j_ds_source)

    def from_elements(self, data):
        j_ds = self._j_env.fromElements(data)
        return DataStream(j_ds)

    def from_elements2(self, data):
        j_ds = self._j_env.fromElements2(data)
        return DataStream(j_ds)

    # TODO: fromParallelCollection

    def read_text_file(self, file_path, charset_name=None):
        if charset_name is None:
            j_ds = self._j_env.readTextFile(file_path)
        else:
            j_ds = self._j_env.readTextFile(file_path, charset_name)
        return DataStream(j_ds)

    def read_file(self, input_format, file_path, watch_type=None, interval=None):
        if watch_type is None and interval is None:
            j_ds = self._j_env.readFile(input_format._j_input_format,  \
                                        file_path,  \
                                        watch_type.convert_to_java_obj(),  \
                                        interval)
        else:
            j_ds = self._j_env.readFile(input_format._j_input_format, file_path)
        return DataStream(j_ds)

    def socket_text_stream(self, hostname, port, delimiter='\n', max_retry=0):
        j_ds = self._j_env.socketTextStream(hostname, port, delimiter, max_retry)
        return DataStream(j_ds)

    def create_input(self, input_format):
        j_ds = self._j_env.createInput(input_format._j_input_format)
        return DataStream(j_ds)

    def add_source(self, source_func):
        # (JavaSourceFunction) -> DataStreamSource

        if isinstance(source_func, JavaSourceFunction):
            j_ds = self._j_env.addSource(source_func._j_source_function)
        else:
            # TODO: support pure python SourceFunction (DataStream API)
            j_ds = None
        return DataStreamSource(j_ds)

    # TODO: SourceFunction,  SourceContext, addSourceV2

    def execute(self, stream_graph=None, job_name=None):
        if stream_graph is not None:
            j_job_execution_result = self._j_env.execute(stream_graph._j_stream_graph)
        elif job_name is None:
            j_job_execution_result = self._j_env.execute()
        else:
            j_job_execution_result = self._j_env.execute(job_name)
        return JobExecutionResult(j_job_execution_result)

    def submit(self, job_name=None):
        if job_name is None:
            j_job_submission_result = self._j_env.submit()
        else:
            j_job_submission_result = self._j_env.submit(job_name)
        return JobSubmissionResult(j_job_submission_result)

    def cancel(self, job_id):
        self._j_env.cancel(job_id)

    def cancel_with_savepoint(self, job_id, path):
        return self._j_env.cancelWithSavepoint(job_id, path)

    def trigger_savepoint(self, job_id, path=None):
        if path is None:
            return self._j_env.triggerSavepoint(job_id)
        else:
            return self._j_env.triggerSavepoint(job_id, path)

    def get_execution_plan(self):
        return self._j_env.getExecutionPlan()

    def stop_job(self, job_id):
        self._j_env.stopJob(job_id)

    @classmethod
    def get_execution_environment(cls):
        """
        :return: StreamExecutionEnvironment

        >>> from pyflink import StreamExecutionEnvironment
        >>> s_env = StreamExecutionEnvironment.get_execution_environment()

        """
        _cls = get_gateway().jvm.org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
        j_env = _cls.getExecutionEnvironment()
        return StreamExecutionEnvironment(j_env)

    @classmethod
    def set_default_local_parallelism(cls, parallelism):
        _flink = get_gateway().jvm.org.apache.flink
        _flink.streaming.api.environment.StreamExecutionEnvironment.setDefaultLocalParallelism(parallelism)

    @classmethod
    def create_local_environment(cls, parallelism=None, conf=None):
        # TODO, Configuration wrapper
        _cls = get_gateway().jvm.org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
        if parallelism is None:
            j_local_env = _cls.createLocalEnvironment()
        elif conf is None:
            j_local_env = _cls.createLocalEnvironment(parallelism)
        else:
            j_local_env = _cls.createLocalEnvironment(parallelism, conf)

        return LocalStreamEnvironment(j_local_env)

    @classmethod
    def create_local_environment_with_web_UI(cls, conf):
        _cls = get_gateway().jvm.org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
        j_local_env = _cls.createLocalEnvironmentWithWebUI(conf)
        return LocalStreamEnvironment(j_local_env)

    @classmethod
    def create_remote_environment(cls, host, port, parallelism=None, *jar_files):
        _cls = get_gateway().jvm.org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

        j_remote_env = _cls.createRemoteEnvironment(host, port, *jar_files)
        if parallelism is not None:
            j_remote_env.setParallelism(parallelism)
        return RemoteStreamEnvironment(j_remote_env)


class LocalStreamEnvironment(StreamExecutionEnvironment):
    """

    """
    def __init__(self, j_env):
        self._j_env = j_env

    # TODO: other functions


class RemoteStreamEnvironment(StreamExecutionEnvironment):
    """

    """

    def __init__(self, j_env):
        self._j_env = j_env

    # TODO: other functions
    # TODO: org.apache.flink.configuration.Configuration
