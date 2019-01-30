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

import os
import signal
import struct
import socket
from subprocess import Popen, PIPE
from threading import RLock

from py4j.java_gateway import java_import, JavaGateway, GatewayClient

_gateway = None
_lock = RLock()


def get_gateway():
    global _gateway
    global _lock
    with _lock:
        if _gateway is None:
            _gateway = launch_java_gateway()
    return _gateway


def receive_all(sock, data_len):
    chunks = []
    bytes_recd = 0
    while bytes_recd < data_len:
        chunk = sock.recv(min(data_len - bytes_recd, 2048))
        if chunk == b'':
            raise RuntimeError("socket connection broken")
        chunks.append(chunk)
        bytes_recd = bytes_recd + len(chunk)
    return b''.join(chunks)


def launch_java_gateway(conf = None):
    # if java gateway is alive, try to get its listening port
    # java_port = get_java_server_port('org.apache.flink.api.python.PythonShellGatewayServer')
    java_port = None
    # if not started yet, start it.
    if java_port is None:
        # start a ServerSocket and tell Java the port, so that Java can pass its Gateway port
        # through this socket connection.
        handshake_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        handshake_socket.bind(('127.0.0.1', 0))
        handshake_socket.listen(1)
        host, port = handshake_socket.getsockname()

        # TODO: investigate that settimeout() seems doesn't work
        # timeout = 2.0
        # handshake_socket.settimeout(timeout)

        launch_java_process(port)

        try:
            connection, _ = handshake_socket.accept()
            data = receive_all(connection, 4)
            java_port = struct.unpack("!I", data)[0]
        except Exception as err:
            print(err)   # TODO: add logger
            raise err
        finally:
            connection.close()
            handshake_socket.close()

    # Connect to the java gateway
    gateway = JavaGateway(GatewayClient(port=java_port), auto_convert=True)

    import_java_modular(gateway)

    return gateway


def launch_java_process(port):
    env = dict(os.environ)
    bin_dir = env.get('FLINK_BIN_DIR')
    if bin_dir is None:
        raise Exception("FLINK_BIN_DIR has not been set!")

    shell_gateway = 'org.apache.flink.api.python.PythonShellGatewayServer'
    p = None
    try:
        command = [bin_dir+'/pyflink2.sh', '-c', shell_gateway, str(port)]

        def preexec_func():
            # ignore SIGINT
            signal.signal(signal.SIGINT, signal.SIG_IGN)

        p = Popen(command, stdin=PIPE, stdout=PIPE, stderr=PIPE, preexec_fn=preexec_func, env=dict(os.environ))
    except Exception as err:
        raise err

    # if it has exited
    if p.poll() is not None:
        raise Exception("Launching Java failed!")
    return p


def import_java_modular(gateway):
    java_import(gateway.jvm, "org.apache.flink.api.python.PythonShellGateway")
    java_import(gateway.jvm, "org.apache.flink.streaming.api.environment.ExecutionEnvironment")
    java_import(gateway.jvm, "org.apache.flink.table.api.java.StreamTableEnvironment")
    java_import(gateway.jvm, "org.apache.flink.table.api.TableEnvironment")


def _test():
    try:
        launch_java_gateway()
    except Exception as err:
        print(err)


if __name__ == "__main__":
    _test()
