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

import os
import psutil
from datetime import datetime


class MemSnapshot:
    def __init__(self):
        self.timestamp = datetime.utcnow()
        self.total_physical = None
        self.total_virtual = None
        self.used = None
        self.zeroed = None
        self.available = None
        self.proc_resident_set = None

    def __str__(self):
        result_str = f'[{self.timestamp}]' + os.linesep
        result_str += f'proc. resident set: {_2human(self.proc_resident_set)}' + os.linesep
        result_str += f'total phy.: {_2human(self.total_physical)}' + os.linesep
        result_str += f'total vir.: {_2human(self.total_virtual)}' + os.linesep
        result_str += f'used: {_2human(self.used)}' + os.linesep
        result_str += f'available: {_2human(self.available)}' + os.linesep
        result_str += f'zeroed: {_2human(self.zeroed)}'
        return result_str


def mem_snapshot() -> MemSnapshot:
    # https://psutil.readthedocs.io/en/latest/index.html?highlight=memory_info#psutil.virtual_memory
    # total: total physical memory (exclusive swap).
    # used: memory used. total - free does not necessarily match used.
    # free: memory not being used at all (zeroed) that is readily available.
    # available: memory that can be given instantly to processes without the system
    #     going into swap. This is calculated by summing different memory values
    #     depending on the platform and it is supposed to be used to monitor actual
    #     memory usage in a cross platform fashion.
    # https://psutil.readthedocs.io/en/latest/index.html?highlight=memory_info#psutil.Process.memory_info
    # rss: resident set size, the non-swapped physical memory a process has used.
    # vms: total program size, the total amount of virtual memory used by the process.
    snapshot = MemSnapshot()
    virtual_memory = psutil.virtual_memory()
    snapshot.total_physical = virtual_memory.total
    snapshot.used = virtual_memory.used
    snapshot.zeroed = virtual_memory.free
    snapshot.available = virtual_memory.available
    process_memory = psutil.Process(os.getpid()).memory_info()
    snapshot.total_virtual = process_memory.vms
    snapshot.proc_resident_set = process_memory.rss
    return snapshot


def mem_snapshot_diff(earlier_snapshot: MemSnapshot, later_snapshot: MemSnapshot) -> MemSnapshot:
    snapshot = MemSnapshot()
    snapshot.total_physical = later_snapshot.total_physical - earlier_snapshot.total_physical
    snapshot.used = later_snapshot.used - earlier_snapshot.used
    snapshot.zeroed = later_snapshot.zeroed - earlier_snapshot.zeroed
    snapshot.available = later_snapshot.available - earlier_snapshot.available
    snapshot.total_virtual = later_snapshot.total_virtual - earlier_snapshot.total_virtual
    snapshot.proc_resident_set = later_snapshot.proc_resident_set - earlier_snapshot.proc_resident_set
    return snapshot


__bytes_scale__ = {
    'K': 1024,
    'M': 1024 ** 2,
    'G': 1024 ** 3,
    'T': 1024 ** 4,
    'P': 1024 ** 5,
    'E': 1024 ** 6,
    'Z': 1024 ** 7,
    'Y': 1024 ** 8
}
__bytes_scale_symbols__ = ('Y', 'Z', 'E', 'P', 'T', 'G', 'M', 'K')


def _2human(total_bytes: int) -> str:
    sign = ''
    if total_bytes < 0:
        total_bytes = -total_bytes
        sign = '-'
    for symbol in __bytes_scale_symbols__:
        symbol_size = __bytes_scale__[symbol]
        if total_bytes >= symbol_size:
            return f'{sign}{float(total_bytes) / symbol_size:.2f}{symbol}'
    return f'{total_bytes}B'


if __name__ == '__main__':
    print(mem_snapshot())
