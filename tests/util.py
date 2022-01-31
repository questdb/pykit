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

import unittest
import os
import psycopg2
import numpy as np
import mmap
from pathlib import Path
import pandas as pd
from pandas.core.internals import (BlockManager, make_block)
from pandas.core.indexes.base import Index

from pykit import (
    select_all,
    with_cursor,
    Cursor
)
from pykit.internal import (
    MemSnapshot,
    mem_snapshot,
    mem_snapshot_diff
)


class BaseTestTest(unittest.TestCase):
    def assert_table_content(self, table_name: str, expected: str) -> None:
        results = ''
        for row in select_all(table_name):
            results += str(row) + os.linesep
        self.assertEqual(expected, results)

    def take_mem_snapshot(self):
        return mem_snapshot()

    def report_mem_snapshot_diff(self, snapshot_start: MemSnapshot, heading: str = None) -> MemSnapshot:
        snapshot_now = mem_snapshot()
        if heading is not None:
            print(heading)
        print(mem_snapshot_diff(snapshot_start, snapshot_now))
        return snapshot_now

    def create_rnd_table(self, table_name: str, num_rows: int = 10):
        def _create_rnd_table(stmt_cursor: Cursor) -> None:
            statement = f'CREATE TABLE {table_name} AS('
            statement += 'SELECT'
            statement += '  rnd_long(0, 9223372036854775807, 1) long, '
            statement += '  rnd_int(0, 2147483647, 1) int, '
            statement += '  rnd_boolean() boolean, '
            statement += "  rnd_date(to_date('1978', 'yyyy'),  to_date('2021', 'yyyy'), 1) date, "
            statement += '  rnd_double(1) double, '
            statement += "  rnd_timestamp(to_timestamp('1978', 'yyyy'), to_timestamp('2021', 'yyyy'), 0) ts "
            statement += 'FROM'
            statement += f'  long_sequence({num_rows})'
            statement += ') timestamp(ts) partition by YEAR;'
            stmt_cursor.execute(statement)

        try:
            with_cursor(_create_rnd_table)
        except (Exception, psycopg2.Error) as create_error:
            print(f'Error while creating rnd table [{table_name}]: {create_error}')


def dataframe(file_path: Path,
              col_name: str,
              row_count: int,
              dtype: np.dtype,
              storage: int,
              na_value: int,
              cls=None):
    return pd.DataFrame(
        data=BlockManagerUnconsolidated(
            blocks=(make_block(
                values=mmap_column(file_path, row_count, dtype, storage, na_value, cls),
                placement=(0,)
            ),),
            axes=[
                Index(data=[col_name]),
                pd.RangeIndex(name='Idx', start=0, stop=row_count, step=1)
            ],
            verify_integrity=False),
        copy=False)


def mmap_column(file_path: Path, nrows: int, dtype: np.dtype, storage: int, na_value: int, cls=None):
    with open(file_path, mode='rb') as col_file:
        col_mmap = mmap.mmap(
            col_file.fileno(),
            length=nrows * storage,
            flags=mmap.MAP_SHARED,
            access=mmap.ACCESS_READ,
            offset=0)
    column_array = np.ndarray(shape=(nrows,), dtype=dtype, buffer=col_mmap, offset=0, order='C')
    column_array.flags['WRITEABLE'] = False
    column_array.flags['ALIGNED'] = True
    mask_array = np.zeros((nrows,), dtype=bool, order='C')
    for null_idx in np.where(column_array == na_value):
        mask_array[null_idx] = True
    np.save(Path('resources') / 'null_mask.npy', mask_array, allow_pickle=False)
    constructor = pd.arrays.IntegerArray if cls is None else cls
    return constructor(column_array, mask_array)


class BlockManagerUnconsolidated(BlockManager):
    def __init__(self, *args, **kwargs):
        BlockManager.__init__(self, *args, **kwargs)
        self._is_consolidated = False
        self._known_consolidated = True

    def _consolidate_inplace(self):
        pass

    def _consolidate(self):
        return self.blocks
