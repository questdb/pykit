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

from enum import Enum
import os
from pathlib import Path
import sys
import typing

from pykit.types import (
    ColumnTypes,
    ColumnType
)
from pykit.ts import (
    from_date,
    from_timestamp
)

VERSION = '1.0.0'

# User local QuestDB installation folder
QDB_HOME = Path.home() / '.questdb'

# QuestBD's storage root folder contains db, conf
QDB_DB_ROOT = QDB_HOME / 'ROOT'

# QuestBD's storage data folder
QDB_DB_DATA = QDB_DB_ROOT / 'db'

# QuestBD's configuration folder, if not exists, create a default one
QDB_DB_CONF = QDB_DB_ROOT / 'conf'

# Git clone, automatically checked out on server start, or on module command 'update'
QDB_CLONE_FOLDER = QDB_HOME / 'clone'


class PartitionBy(Enum):
    DAY = 0
    MONTH = 1
    YEAR = 2
    NONE = 3

    @staticmethod
    def resolve(partition_id: int):
        for partition_by in PartitionBy:
            if partition_id == partition_by.value:
                return partition_by
        return PartitionBy.NONE


class TypeMetadata(ColumnType):
    def __new__(cls, type_id: int, type_flags: int, type_idx_block_size: int):
        return ColumnTypes.resolve(type_id)

    def __init__(self, _type_id: int, type_flags: int, type_idx_block_size: int):
        self.type_flags = type_flags
        self.type_idx_block_size = type_idx_block_size

    def __str__(self):
        result_str = str(self)
        result_str += f' [flags: {self.type_flags}, '
        result_str += f'index block size: {self.type_idx_block_size}]'
        return result_str


class Metadata:
    def __init__(self, table_name: str):
        self.table_name = table_name
        self.meta_path = None
        self.column_count = None
        self.partition_by = None
        self.timestamp_idx = None
        self.version = None
        self.table_id = None
        self.max_uncommitted_rows = None
        self.commit_lag = None
        self.column_names = []
        self.column_types = []
        self.reload()

    def reload(self):
        self.column_names.clear()
        self.column_types.clear()
        if table_root_path := _table_data_root(self.table_name):
            self.meta_path = table_root_path / '_meta'
            with open(self.meta_path, mode='rb') as meta_file:
                self.column_count = _read_int32(meta_file, offset=0)
                self.partition_by = PartitionBy.resolve(_read_int32(meta_file, offset=4))
                timestamp_idx = _read_int32(meta_file, offset=8)
                if 0 <= timestamp_idx < self.column_count:
                    self.timestamp_idx = timestamp_idx
                self.version = _read_int32(meta_file, offset=12)
                self.table_id = _read_int32(meta_file, offset=16)
                self.max_uncommitted_rows = _read_int32(meta_file, offset=20)
                self.commit_lag = _read_int64(meta_file, offset=24)
                name_offset = 128 + self.column_count * 16
                for i in range(self.column_count):
                    type_offset = 128 + i * 16
                    self.column_types.append(
                        TypeMetadata(
                            type_id=_read_int32(meta_file, offset=type_offset),
                            type_flags=_read_int64(meta_file, offset=type_offset + 4),
                            type_idx_block_size=_read_int32(meta_file, offset=type_offset + 4 + 8)))
                    name_len = _read_int32(meta_file, name_offset)
                    col_name = meta_file.read(name_len * 2).decode('utf-16')
                    self.column_names.append(col_name)
                    name_offset += 4 + name_len * 2

    def __str__(self):
        if self.meta_path:
            result_str = f'meta_path: {self.meta_path}{os.linesep}'
            result_str += f'table_name: {self.table_name}{os.linesep}'
            result_str += f'table_id: {self.table_id}{os.linesep}'
            result_str += f'version: {self.version}{os.linesep}'
            result_str += f'column_count: {self.column_count}{os.linesep}'
            for i in range(self.column_count):
                result_str += f'- {i}: {self.column_types[i]}{os.linesep}'
            if 0 <= self.timestamp_idx < self.column_count:
                ts_value = self.column_names[self.timestamp_idx]
            else:
                ts_value = 'NONE'
            result_str += f'timestamp_idx: {ts_value}{os.linesep}'
            result_str += f'partition_by: {self.partition_by}{os.linesep}'
            result_str += f'max_uncommitted_rows: {self.max_uncommitted_rows}{os.linesep}'
            result_str += f'commit_lag: {self.commit_lag}'
            return result_str
        return None


class Partition:
    def __init__(self, p_id: int, p_timestamp: int, p_size: int, p_name_tx: int, p_data_tx: int):
        self.p_id = p_id
        self.p_timestamp = p_timestamp
        self.p_size = p_size
        self.p_name_tx = p_name_tx if p_name_tx != 0xFFFFFFFFFFFFFFFF else -1
        self.p_data_tx = p_data_tx if p_data_tx != 0xFFFFFFFFFFFFFFFF else -1

    def __str__(self):
        result_str = f'id:{self.p_id}, '
        result_str += f'ts:{from_date(self.p_timestamp)}, '
        result_str += f'ts epoch:{self.p_timestamp}, '
        result_str += f'size:{self.p_size}, '
        result_str += f'name_tx:{self.p_name_tx}, '
        result_str += f'data_tx:{self.p_data_tx})'
        return result_str


class Transaction:
    def __init__(self, table_name: str):
        self.table_name = table_name
        self.root_path = None
        self.txn_path = None
        self.txn_id = None
        self.transient_row_count = None
        self.fixed_row_count = None
        self.min_timestamp = None
        self.max_timestamp = None
        self.struct_version = None
        self.data_version = None
        self.partition_table_version = None
        self.txn_check = None
        self.symbols_count = None
        self.partitions_count = None
        self.partition_table_size = None
        self.partitions = []
        self.reload()

    def reload(self):
        if table_root_path := _table_data_root(self.table_name):
            self.root_path = table_root_path
            self.txn_path = self.root_path / '_txn'
            with open(self.txn_path, mode='rb') as txn_file:
                self.txn_id = _read_int64(txn_file, offset=0)
                self.transient_row_count = _read_int64(txn_file, offset=8)
                self.fixed_row_count = _read_int64(txn_file, offset=16)
                self.min_timestamp = _read_int64(txn_file, offset=24)
                self.max_timestamp = _read_int64(txn_file, offset=32)
                self.struct_version = _read_int64(txn_file, offset=40)
                self.data_version = _read_int64(txn_file, offset=48)
                self.partition_table_version = _read_int64(txn_file, offset=56)
                self.txn_check = _read_int64(txn_file, offset=64)
                self.symbols_count = _read_int32(txn_file, offset=72)
                partition_table_offset = 72 + 4 + self.symbols_count * 8
                self.partition_table_size = int(_read_int32(txn_file, offset=partition_table_offset) / 8)
                self.partitions_count = int(self.partition_table_size / 4)
                self.partitions.clear()
                for partition_id in range(self.partitions_count):
                    partition_offset = partition_table_offset + 4 + partition_id * 32
                    self.partitions.append(Partition(
                        partition_id,
                        _read_int64(txn_file, offset=partition_offset),
                        _read_int64(txn_file, offset=partition_offset + 8),
                        _read_int64(txn_file, offset=partition_offset + 16),
                        _read_int64(txn_file, offset=partition_offset + 24)))

    def partition_size(self, partition_index: int) -> int:
        return self.partitions[partition_index].p_size

    @property
    def row_count(self):
        return self.transient_row_count + self.fixed_row_count

    def __str__(self):
        if self.txn_path:
            result_str = f'root_path: {self.root_path}{os.linesep}'
            result_str += f'txn_path: {self.txn_path}{os.linesep}'
            result_str += f'table_name: {self.table_name}{os.linesep}'
            result_str += f'txn_id: {self.txn_id}{os.linesep}'
            result_str += f'transient_row_count: {self.transient_row_count}{os.linesep}'
            result_str += f'fixed_row_count: {self.fixed_row_count}{os.linesep}'
            result_str += f'min_timestamp: {self.min_timestamp}{os.linesep}'
            result_str += f'max_timestamp: {self.max_timestamp}{os.linesep}'
            result_str += f'struct_version: {self.struct_version}{os.linesep}'
            result_str += f'data_version: {self.data_version}{os.linesep}'
            result_str += f'partition_table_version: {self.partition_table_version}{os.linesep}'
            result_str += f'txn_check: {self.txn_check}{os.linesep}'
            result_str += f'symbols_count: {self.symbols_count}{os.linesep}'
            result_str += f'partitions_count: {self.partitions_count}'
            if self.partitions_count:
                result_str += os.linesep
                for i in range(self.partitions_count):
                    result_str += f'partition[{i}]: {self.partitions[i]}'
                    if i + 1 < self.partitions_count:
                        result_str += os.linesep
            return result_str
        return None


class TableInfo:
    def __init__(self, table_name: str):
        self.metadata = Metadata(table_name)
        self.transaction = Transaction(table_name)

    @property
    def row_count(self):
        return self.transaction.row_count

    @property
    def column_count(self):
        return self.metadata.column_count

    def column_name(self, col_idx: int) -> str:
        if 0 <= col_idx < self.column_count:
            return self.metadata.column_names[col_idx]
        return None

    def column_type(self, col_idx: int) -> str:
        if 0 <= col_idx < self.column_count:
            return self.metadata.column_types[col_idx]
        return None

    @property
    def ts_idx(self) -> int:
        return self.metadata.timestamp_idx

    def is_partitioned(self) -> bool:
        return self.metadata.partition_by != PartitionBy.NONE

    @property
    def partition_by(self) -> int:
        return self.metadata.partition_by

    @property
    def partitions_count(self) -> int:
        return self.transaction.partitions_count

    def partition_info(self, p_id: int) -> typing.Tuple[Path, int]:
        if 0 <= p_id < self.partitions_count:
            partition = self.transaction.partitions[p_id]
            p_folder = self._partition_folder(partition.p_timestamp, partition.p_name_tx)
            if self.partition_by == PartitionBy.NONE:
                row_count = self.transaction.row_count
            elif p_id + 1 < self.partitions_count:
                row_count = partition.p_size
            else:
                row_count = self.transaction.transient_row_count
            return p_folder, row_count
        return None, None

    def _partition_folder(self, date_micros: int, tx_name: int) -> Path:
        if self.partition_by == PartitionBy.DAY:
            folder_name = from_timestamp(date_micros, '%Y-%m-%d')
        elif self.partition_by == PartitionBy.MONTH:
            folder_name = from_timestamp(date_micros, '%Y-%m')
        elif self.partition_by == PartitionBy.YEAR:
            folder_name = from_timestamp(date_micros, '%Y')
        elif self.partition_by == PartitionBy.NONE:
            folder_name = 'default'
        if self.partition_by != PartitionBy.NONE and tx_name != -1:
            folder_name += f'.{tx_name}'
        return self.transaction.root_path / folder_name


def _table_data_root(table_name: str) -> Path:
    if QDB_DB_DATA.exists():
        candidate = QDB_DB_DATA / str(table_name)
        if candidate.is_dir():
            return candidate
    return None


def _read_int32(meta_file: typing.BinaryIO, offset: int) -> int:
    return _read_int(meta_file, offset, 4)


def _read_int64(meta_file: typing.BinaryIO, offset: int) -> int:
    return _read_int(meta_file, offset, 8)


def _read_int(meta_file: typing.BinaryIO, offset: int, num_bytes: int) -> int:
    meta_file.seek(offset)
    return int.from_bytes(meta_file.read(num_bytes), byteorder=sys.byteorder)
