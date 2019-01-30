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

# TODO: to be remove

from subprocess import check_output, CalledProcessError
import re


def get_java_server_port(app_name):
    """
    A util function to find out the port of java gateway. Only for Unix platform
    :param app_name:
    :return:
    """
    output = bytes.decode(check_output(['jps', '-l'])).split('\n')
    pids = [int(l.split(' ')[0]) for l in output if l.endswith(app_name)]

    # if multiple java instance, use the first one
    if len(pids) > 0:
        pid = pids[0]
    else:
        return None

    output = bytes.decode(check_output(['lsof', "-a", '-p%d' % pid])).split('\n')
    lines = ([l for l in output if l.endswith('(LISTEN)')])
    if len(lines):
        l = lines[0]
    else:
        return None

    # parse the port from the string like:
    # 'java 14454 ... 0t0  TCP localhost:59085 (LISTEN)'
    port = int(re.findall(r".*:(\d+) \(LISTEN\)", l)[0])
    return port


def _test():
    try:
        get_java_server_port('org.apache.flink.api.python.PythonShellGatewayServer')
    except Exception as err:
        print(err)


if __name__ == "__main__":
    _test()
