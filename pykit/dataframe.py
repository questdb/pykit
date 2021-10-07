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

import numpy as np
import mmap
import typing
import pandas as pd
from pandas.core.internals import (BlockManager, make_block)
from pandas.core.internals.blocks import Block
from pandas.core.indexes.base import Index
from pykit.core import TableInfo
from pykit.types import COLUMN_TYPES

NOT_STORED_ANONYMOUS_MEMORY = -1


def df_from_table(table_name: str, columns: typing.Tuple[typing.Tuple[str, str]]) -> pd.DataFrame:
    table_info = TableInfo(table_name)
    column_names = []
    column_np_arrays = []
    for col_idx in range(table_info.column_count):
        col_name = table_info.column_name(col_idx)
        if _validate_column(col_name, *columns):
            col_mmap = mmap.mmap(
                NOT_STORED_ANONYMOUS_MEMORY,
                length=table_info.row_count * table_info.column_storage_size(col_idx),
                flags=mmap.MAP_SHARED,
                access=mmap.ACCESS_WRITE,
                offset=0)
            wr_offset = 0
            for p_id in range(table_info.partitions_count):
                p_folder, p_row_count = table_info.partition_info(p_id)
                with open(p_folder / f'{col_name}.d', 'rb') as p_file:
                    p_storage_size = p_row_count * table_info.column_storage_size(col_idx)
                    p_mmap = mmap.mmap(
                        p_file.fileno(),
                        length=p_storage_size,
                        flags=mmap.MAP_SHARED,
                        access=mmap.ACCESS_READ,
                        offset=0)
                    write_end = wr_offset + p_storage_size
                    col_mmap[wr_offset:write_end] = p_mmap
                    wr_offset = write_end
            col_np_array = NDArray.__new__(
                NDArray,
                p_file.name,
                table_info.row_count,
                table_info.column_dtype(col_idx),
                col_mmap
            )
            if table_info.ts_idx == col_idx:
                index = Index(
                    data=col_np_array,
                    name=col_name,
                    tupleize_cols=False,
                    copy=False)
            else:
                column_names.append(col_name)
                column_np_arrays.append(col_np_array)
    if table_info.ts_idx is None:
        index = pd.RangeIndex(
            name='Idx',
            start=0,
            stop=table_info.row_count,
            step=1)

    def _block_gen() -> typing.Sequence[Block]:
        for position, column in enumerate(column_np_arrays):
            yield make_block(
                values=column.reshape((1, len(column))),
                placement=(position,))

    return pd.DataFrame(
        data=BlockManagerUnconsolidated(
            blocks=tuple(_block_gen()),
            axes=[Index(data=column_names), index],
            verify_integrity=False),
        copy=False)


class BlockManagerUnconsolidated(BlockManager):
    def __init__(self, *args, **kwargs):
        BlockManager.__init__(self, *args, **kwargs)
        self._is_consolidated = False
        self._known_consolidated = True

    def _consolidate_inplace(self):
        pass

    def _consolidate(self):
        return self.blocks


class NDArray(np.ndarray):
    def __new__(subtype,
                col_file: str,
                row_count: int,
                col_dtype: np.dtype,
                col_mmap: mmap.mmap):
        col_np_array = np.ndarray.__new__(
            NDArray,
            shape=(row_count,),
            dtype=col_dtype,
            buffer=col_mmap,
            offset=0,
            order='C')
        col_np_array._mmap = col_mmap
        col_np_array.mode = 'rb'
        col_np_array.filename = col_file
        col_np_array.flags['WRITEABLE'] = False
        col_np_array.flags['ALIGNED'] = True
        return col_np_array

    def __getitem__(self, item):
        value = super().__getitem__(item)
        return value


def _validate_column(target_col_name: str, *columns: typing.Tuple[str, str]) -> bool:
    for col_name, col_type in columns:
        if col_name == target_col_name and COLUMN_TYPES.resolve(col_type).supports_dataframe:
            return True
    return False


def _np_array_getitem(*args, **kwargs):
    print(f'args:{args}')
    print(f'kwargs:{kwargs}')
