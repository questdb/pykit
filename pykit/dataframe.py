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
from pandas.core.internals import BlockManager
from pandas.core.internals.blocks import Block
from pandas.core.indexes.base import Index

from pykit.core import TableInfo
from pykit.types import COLUMN_TYPES


def df_from_table(table_name: str, columns: typing.Tuple[typing.Tuple[str, str]]) -> pd.DataFrame:
    table_info = TableInfo(table_name)
    dataframes = []
    for p_id in range(table_info.partitions_count):
        p_folder, row_count = table_info.partition_info(p_id)
        column_names = []
        mapped_columns = []
        if table_info.ts_idx is None:
            index = pd.RangeIndex(name='Idx', start=0, stop=row_count, step=1)
        for i in range(table_info.column_count):
            col_name = table_info.column_name(i)
            if _validate_column(col_name, *columns):
                with (file_name := open(p_folder / f'{col_name}.d', 'rb')) as fid:
                    size_to_map = row_count * table_info.column_storage_size(i)
                    mm = mmap.mmap(fid.fileno(), size_to_map, access=mmap.ACCESS_READ, offset=0)
                mm_column = np.ndarray.__new__(
                    np.memmap,
                    shape=(row_count,),
                    dtype=table_info.column_dtype(i),
                    buffer=mm,
                    offset=0,
                    order='C')
                mm_column._mmap = mm
                mm_column.mode = 'rb'
                mm_column.filename = file_name
                mm_column.flags['WRITEABLE'] = False
                mm_column.flags['ALIGNED'] = True
                if table_info.ts_idx == i:
                    index = Index(data=mm_column, name=col_name, tupleize_cols=False, copy=False)
                else:
                    column_names.append(col_name)
                    mapped_columns.append(mm_column)

        from pandas.core.internals import make_block

        def block_gen() -> typing.Sequence[Block]:
            for position, column in enumerate(mapped_columns):
                yield make_block(
                    values=column.reshape((1, len(column))),
                    placement=(position,))

        dataframes.append(WrapperDataFrame(pd.DataFrame(
            data=BlockManagerUnconsolidated(
                blocks=tuple(block_gen()),
                axes=[Index(data=column_names), index],
                verify_integrity=False),
            copy=False)))
    return dataframes


class WrapperDataFrame(pd.DataFrame):
    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        pass


class BlockManagerUnconsolidated(BlockManager):
    def __init__(self, *args, **kwargs):
        BlockManager.__init__(self, *args, **kwargs)
        self._is_consolidated = False
        self._known_consolidated = True

    def _consolidate_inplace(self):
        pass

    def _consolidate(self):
        return self.blocks


def _validate_column(target_col_name: str, *columns: typing.Tuple[str, str]) -> bool:
    for col_name, col_type in columns:
        if col_name == target_col_name and COLUMN_TYPES.resolve(col_type).supports_dataframe:
            return True
    return False
