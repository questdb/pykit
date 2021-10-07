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
from pathlib import Path

from pykit.ts import (
    from_timestamp
)
from pykit.types import (
    COLUMN_TYPES,
    PARTITION_BY_NONE,
    PARTITION_BY_DAY,
    PARTITION_BY_MONTH,
    PARTITION_BY_YEAR
)
from pykit.core import (
    Metadata,
    Transaction,
    _table_data_root
)


def df_from_table(table_name: str, columns: typing.Tuple[typing.Tuple[str, str]]) -> pd.DataFrame:
    metadata = Metadata(table_name)
    transaction = Transaction(table_name)
    ts_idx = metadata.timestamp_idx
    dataframes = []
    partitions_root_path = _table_data_root(table_name)
    for p_id in range(transaction.partitions_count):
        partition = transaction.partitions[p_id]
        p_folder = _partition_folder(partitions_root_path,
                                     partition.p_timestamp,
                                     metadata.partition_by)
        if metadata.partition_by == PARTITION_BY_NONE:
            row_count = transaction.row_count
        elif p_id + 1 < transaction.partitions_count:
            row_count = partition.p_size
        else:
            row_count = transaction.transient_row_count
        column_names = []
        mapped_columns = []
        if ts_idx is None:
            index = pd.RangeIndex(name='Idx', start=0, stop=row_count, step=1)
        for i in range(metadata.column_count):
            col_name = metadata.column_names[i]
            if _validate_column(col_name, *columns):
                col_type_meta = metadata.column_types[i]
                with (file_name := open(p_folder / f'{col_name}.d', 'rb')) as fid:
                    size_to_map = row_count * col_type_meta.storage_size
                    mm = mmap.mmap(fid.fileno(), size_to_map, access=mmap.ACCESS_READ, offset=0)
                mm_column = np.ndarray.__new__(
                    np.memmap,
                    shape=(row_count,),
                    dtype=metadata.column_types[i].col_dtype,
                    buffer=mm,
                    offset=0,
                    order='C')
                mm_column._mmap = mm
                mm_column.mode = 'rb'
                mm_column.filename = file_name
                mm_column.flags['WRITEABLE'] = False
                mm_column.flags['ALIGNED'] = True
                if ts_idx == i:
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

        dataframes.append(pd.DataFrame(
            data=BlockManagerUnconsolidated(
                blocks=tuple(block_gen()),
                axes=[Index(data=column_names), index],
                verify_integrity=False),
            copy=False))
    return dataframes


class WrapperDataFrame(pd.DataFrame):
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


def _partition_folder(partitions_root_path: Path, date_micros: int, partition_by: int) -> Path:
    if partition_by == PARTITION_BY_DAY:
        folder_name = from_timestamp(date_micros, '%Y-%m-%d')
    elif partition_by == PARTITION_BY_MONTH:
        folder_name = from_timestamp(date_micros, '%Y-%m')
    elif partition_by == PARTITION_BY_YEAR:
        folder_name = from_timestamp(date_micros, '%Y')
    elif partition_by == PARTITION_BY_NONE:
        folder_name = 'default'
    return partitions_root_path / folder_name
