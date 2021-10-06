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

import ctypes
import struct
import platform

from pykit import (
    QDB_CLONE_FOLDER
)

__ext_libs_root_ = QDB_CLONE_FOLDER / 'core' / 'src' / 'main' / 'resources' / 'io' / 'questdb' / 'bin'


def is_64b():
    return 64 != struct.calcsize('P') * 8


def is_arm_arch():
    return platform.machine().startswith('arm')


def is_linux():
    return platform.system() == 'Linux'


def is_macos():
    return platform.system() == 'Darwin'


def is_windows():
    return platform.system() == 'Windows'


def is_freebsd():
    return platform.system() == 'FreeBSD'


def _load_os_dependent_questdb_lib():
    if 64 != struct.calcsize('P') * 8:
        raise Exception('QuestDB requires 64-bit Python')
    sys_name = platform.system()
    sys_arch = platform.machine()
    if sys_name == 'Linux':
        file_ext = 'so'
        sys_folder = 'armlinux' if sys_arch == 'arm64' else 'linux'
    elif sys_name == 'Darwin':  # MacOS
        file_ext = 'dylib'
        sys_folder = 'armosx' if sys_arch == 'arm64' else 'osx'
    elif sys_name == 'Windows':
        file_ext = 'dll'
        sys_folder = 'windows'
    elif sys_name == 'FreeBSD':
        file_ext = 'so'
        sys_folder = 'freebsd'
    else:
        raise Exception(f'unsupported OS: {sys_name}')
    lib_path = __ext_libs_root_ / sys_folder / f'libquestdb.{file_ext}'
    if sys_name == 'Windows':
        return ctypes.windll.LoadLibrary(lib_path)
    return ctypes.cdll.LoadLibrary(lib_path)
