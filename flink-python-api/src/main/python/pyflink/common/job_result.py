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


class JobID(object):
    """
    Wrapper for org.apache.flink.api.common.JobID
    """
    def __init__(self, j_job_id):
        self._j_job_id = j_job_id
    # TODO: add other functions...


class JobSubmissionResult(object):
    """
    Wrapper for org.apache.flink.api.common.JobSubmissionResult
    """

    def __init__(self, j_jsr):
        self._j_jsr = j_jsr

    def get_job_id(self):
        j_job_id = self._j_jsr.getJobID()
        return JobID(j_job_id)

    def is_job_execution_result(self):
        return isinstance(self, JobExecutionResult)

    def get_job_execution_result(self):
        j_res = self._j_jsr.getJobExecutionResult()
        return JobExecutionResult(j_res)


class JobExecutionResult(JobSubmissionResult):
    """

    """
    def __init__(self, j_job_execution_result):
        self._j_job_execution_result = j_job_execution_result

    def get_net_runtime(self):
        return self._j_job_execution_result.getNetRuntime()

    def get_accumulator_result(self):
        return self._j_job_execution_result.getNetRuntime()

