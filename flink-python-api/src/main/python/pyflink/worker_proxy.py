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

# Use a process for each Python UDF
# Side-stepping the Global Interpreter Lock by using subprocesses instead of threads

import sys
import struct
import multiprocessing

from socket import socket, AF_INET, SOCK_STREAM, SOMAXCONN

CPU_NUM = multiprocessing.cpu_count()


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


def recv_udf_call_request(sock):
    chunks = []
    bytes_recd = 0

    # [LEN 8 bytes][Data]
    # make sure we received the LEN block
    data_len = 8  # LEN block
    while bytes_recd < data_len:
        chunk = sock.recv(min(data_len - bytes_recd, 8192))
        if chunk == b'':
            raise RuntimeError("socket connection broken")
        chunks.append(chunk)
        bytes_recd = bytes_recd + len(chunk)

    data = b''.join(chunks)
    v = memoryview(data)
    data_size = struct.unpack('!q', v[0:8])[0]
    data_len = data_size + 8

    # if any data left, continue to receive
    chunks = []
    while bytes_recd < data_len:
        chunk = sock.recv(min(data_len - bytes_recd, 8192))
        if chunk == b'':
            raise RuntimeError("socket connection broken")
        chunks.append(chunk)
        bytes_recd = bytes_recd + len(chunk)

    return data + b''.join(chunks)


# def recv_all_data(sock):
#     """
#
#     :param sock:
#     :return:
#     """
#
#     chunks = []
#
#     chunk = sock.recv(8192)
#     if not chunk:
#         return None
#
#     data_len = struct.unpack('!q', chunk[0:8])[0]
#     chunk += chunk[8:]
#     left_len = data_len - (len(chunk) - 8)
#
#     while left_len > 0:
#         packet = sock.recv(8192)
#         if not packet:
#             break
#         data += packet
#         left_len -= len(packet)
#
#     return b''.join(chunks)


def long_connect_handler(conn):
    try:
        # loop for stream mode
        while True:
            data = recv_udf_call_request(conn)
            if not data:
                break

            # header, idx, error = parse_header(data, 0)
            # if error is not None:
            #     conn.sendall(error)
            #     break
            #
            # # if JVM tell python to exit.
            # if header[1] == ControlFlags.EXIT_STREAM:
            #     # sys.exit(0)
            #     break
            #
            # fun, idx, error = parse_udf(data, idx)
            # if error is not None:
            #     conn.sendall(error)
            #     break
            #
            # args, idx, error = parse_args(data, idx)
            # if error is not None:
            #     conn.sendall(error)
            #     break
            #
            # schema, packed_res, error = call_udf(fun, args)
            # if error is not None:
            #     conn.sendall(error)
            #     break
            #
            # response = build_response(schema, packed_res)
            # conn.sendall(response)

    except socket.error as err:
        sys.stderr.write(str(err) + "\n")
        # build_error_response("Python process can't connect to Java process. ", err)
        # conn.sendall(response)
    finally:
        conn.close()


def tell_jvm_python_port(py_port, jvm_port):
    try:
        # Tell jvm the port of python server
        sock = socket.socket(AF_INET, SOCK_STREAM)
        sock.connect(("127.0.0.1", jvm_port))
        stream = struct.pack("!1I", py_port)
        sock.sendall(stream)

    except Exception as err:
        sys.stderr.write("Can't connect back to JVM!" + str(err))
        sys.exit(1)
    finally:
        sock.close()


def shake_hands_with_jvm():
    try:
        jvm_port = int(sys.argv[1])
        server_sock = socket.socket(AF_INET, SOCK_STREAM)
        server_sock.bind(('127.0.0.1', 0))
        server_sock.listen(max(CPU_NUM * 64, SOMAXCONN))
        host, py_port = server_sock.getsockname()

        tell_jvm_python_port(py_port, jvm_port)
    except Exception as err:
        sys.stderr.write("[Python Error]: Can't raise python process correctly! " + str(err))
        server_sock.close()

    return server_sock


def run_worker_server(server_sock):
    # running in server mode now
    try:
        # loop for multi-threads request
        while True:
            conn, addr = server_sock.accept()
            # t = threading.Thread(target=long_connect_handler, args=(conn,))
            # t.start()

    except socket.error as err:
        sys.stderr.write(str(err)+"\n")
    finally:
        server_sock.close()


def main():
    server_sock = shake_hands_with_jvm()
    run_worker_server(server_sock)


if __name__ == '__main__':
    main()
