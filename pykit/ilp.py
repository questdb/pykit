#
#     ___                  _   ____  ____
#    / _ \ _   _  ___  ___| |_|  _ \| __ )
#   | | | | | | |/ _ \/ __| __| | | |  _ \
#   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
#    \__\_\\__,_|\___||___/\__|____/|____/
#
#  Copyright (c) 2014-2019 Appsicle
#  Copyright (c) 2019-2020 QuestDB
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

import socket
import typing


def create_message(table_name: str,
                   symbols: typing.Dict[str, typing.Any] = None,
                   fields: typing.Dict[str, typing.Any] = None,
                   ts: int = -1) -> str:
    message = [table_name]
    if symbols:
        for (name, value) in symbols.items():
            message.extend((',', name, '=', value))
    if fields:
        message.append(' ')
        for idx, (name, value) in enumerate(fields.items()):
            if isinstance(value, str):
                value = f'"{value}"'
            elif isinstance(value, int):
                value = f'{value}i'
            else:
                value = str(value)
            if idx == 0:
                message.extend((name, '=', value))
            else:
                message.extend((',', name, '=', value))
    if ts > -1:
        message.extend((' ', str(ts)))
    message.append('\n')
    return ''.join(message)


def send_tcp_messages(*messages: typing.List[str]) -> bool:
    host, port = '127.0.0.1', 9009
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((host, port))
            for message in messages:
                sock.sendall(message.encode('utf-8'))
                print(f'Sent tcp: {message}', end='')
            return True
    except Exception as err:
        print(f'Failed to send tcp messages: {err}')
        return False


def send_udp_messages(*messages: typing.List[str]) -> bool:
    host, port = '232.1.2.3', 9009
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            for message in messages:
                sock.sendto(message.encode('utf-8'), (host, port))
                print(f'Sent udp: {message}', end='')
            return True
    except Exception as err:
        print(f'Failed to send udp messages: {err}')
        return False
