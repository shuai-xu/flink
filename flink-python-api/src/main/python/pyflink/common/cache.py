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


# Wrapper for org.apache.flink.api.common.cache.DistributedCache

__all__ = ['DistributedCache']


class DistributedCache(object):
    """

    """

    def __init__(self, j_dc):
        self._j_dc = j_dc

    class DistributedCacheEntry(object):

        """

        """
        file_path = None
        is_executable = False
        blob_key = None
        is_zipped = False

        def __init__(self, j_dc_entry):
            self.file_path = j_dc_entry.filePath
            self.is_executable = j_dc_entry.isExecutable
            self.blob_key = j_dc_entry.blobKey
            self.is_zipped = j_dc_entry.isZipped
