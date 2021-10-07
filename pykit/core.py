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
from pathlib import Path
import sys
import typing

from pykit.ts import (
    from_date,
)
from pykit.types import (
    COLUMN_TYPES,
    PARTITION_BY
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


class TypeMetadata:
    def __init__(self, col_id: int, col_flags: int, col_idx_block_size: int):
        self.col_id = col_id
        self.col_flags = col_flags
        self.col_idx_block_size = col_idx_block_size

    @property
    def col_name(self):
        return COLUMN_TYPES[self.col_id].type_name

    @property
    def col_dtype(self):
        return COLUMN_TYPES[self.col_id].type_dtype

    @property
    def storage_size(self):
        return COLUMN_TYPES[self.col_id].type_storage_size

    def __repr__(self):
        result_str = f'({self.col_name}, '
        result_str += f'{self.col_id}, '
        result_str += f'{self.col_flags}, '
        result_str += f'{self.col_idx_block_size}, '
        result_str += f'{str(self.col_dtype)})'
        return result_str

    def __str__(self):
        result_str = self.col_name
        result_str += f'[flags: {self.col_flags}, '
        result_str += f'index block size: {self.col_idx_block_size}, '
        result_str += f'dtype: {self.col_dtype}]'
        return result_str


class Metadata:
    def __init__(self, table_name: str):
        self.table_name = table_name
        self.meta_path = table_name
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
                self.column_count = _read_int32(meta_file)
                self.partition_by = _read_int32(meta_file)
                timestamp_idx = _read_int32(meta_file)
                if 0 < timestamp_idx < self.column_count:
                    self.timestamp_idx = timestamp_idx
                self.version = _read_int32(meta_file)
                self.table_id = _read_int32(meta_file)
                self.max_uncommitted_rows = _read_int32(meta_file)
                self.commit_lag = _read_int64(meta_file)
                for i in range(self.column_count):
                    meta_file.seek(128 + i * 16)
                    self.column_types.append(
                        TypeMetadata(
                            col_id=_read_int32(meta_file),
                            col_flags=_read_int32(meta_file),
                            col_idx_block_size=_read_int32(meta_file)))
                name_offset = 128 + self.column_count * 16
                meta_file.seek(name_offset)
                for i in range(self.column_count):
                    name_len = _read_int32(meta_file)
                    col_name = bytes(b for b in meta_file.read(name_len * 2) if b).decode('utf-8')
                    self.column_names.append(col_name)

    def __str__(self):
        if self.meta_path:
            result_str = f'meta_path: {self.meta_path}{os.linesep}'
            result_str += f'table_name: {self.table_name}{os.linesep}'
            result_str += f'table_id: {self.table_id}{os.linesep}'
            result_str += f'version: {self.version}{os.linesep}'
            result_str += f'column_count: {self.column_count}{os.linesep}'
            for i in range(self.column_count):
                result_str += f'- {i}: {self.column_types[i]}{os.linesep}'
            if 0 <= self.timestamp_idx < len(self.column_names):
                ts_value = self.column_names[self.timestamp_idx]
            else:
                ts_value = 'NONE'
            result_str += f'timestamp_idx: {ts_value}{os.linesep}'
            result_str += f'partition_by: {PARTITION_BY[self.partition_by]}{os.linesep}'
            result_str += f'max_uncommitted_rows: {self.max_uncommitted_rows}{os.linesep}'
            result_str += f'commit_lag: {self.commit_lag}'
            return result_str
        return None


class Partition:
    def __init__(self, p_id: int, p_timestamp: int, p_size: int, p_name_tx: int, p_data_tx: int):
        self.p_id = p_id
        self.p_timestamp = p_timestamp
        self.p_size = p_size
        self.p_name_tx = p_name_tx
        self.p_data_tx = p_data_tx

    def __str__(self):
        result_str = f'Partition(id:{self.p_id}, '
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
        self.partition_table_size = None
        self.partitions = []
        self.reload()

    def reload(self):
        if table_root_path := _table_data_root(self.table_name):
            self.root_path = table_root_path
            self.txn_path = self.root_path / '_txn'
            with open(self.txn_path, mode='rb') as txn_file:
                self.txn_id = _read_int64(txn_file)
                self.transient_row_count = _read_int64(txn_file)
                self.fixed_row_count = _read_int64(txn_file)
                self.min_timestamp = _read_int64(txn_file)
                self.max_timestamp = _read_int64(txn_file)
                self.struct_version = _read_int64(txn_file)
                self.data_version = _read_int64(txn_file)
                self.partition_table_version = _read_int64(txn_file)
                self.txn_check = _read_int64(txn_file)
                self.symbols_count = _read_int32(txn_file)
                txn_file.seek(76 + self.symbols_count * 8)
                self.partition_table_size = int(_read_int32(txn_file) / 8)
                self.partitions.clear()
                for i in range(self.partitions_count):
                    self.partitions.append(Partition(
                        i,
                        _read_int64(txn_file),
                        _read_int64(txn_file),
                        _read_int64(txn_file),
                        _read_int64(txn_file)))

    @property
    def partitions_count(self):
        return int(self.partition_table_size / 4)

    @property
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


def _table_data_root(table_name: str) -> Path:
    if QDB_DB_DATA.exists():
        candidate = QDB_DB_DATA / str(table_name)
        if candidate.is_dir():
            return candidate
    return None


def _read_int32(meta_file: typing.BinaryIO):
    return int.from_bytes(meta_file.read(4), byteorder=sys.byteorder)


def _read_int64(meta_file: typing.BinaryIO):
    return int.from_bytes(meta_file.read(8), byteorder=sys.byteorder)
