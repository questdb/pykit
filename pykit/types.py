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

import mmap
import numpy as np
import pandas as pd
import typing

from pandas.core.dtypes.base import ExtensionDtype


@pd.api.extensions.register_extension_dtype
class ColumnType(ExtensionDtype):
    _metadata = ('type_id',)

    def __init__(self, dtype: type[np.generic], metadata: typing.Dict[str, typing.Any]):
        self._dtype = np.dtype(dtype, metadata=metadata)

    @property
    def name(self) -> str:
        return self.type_name()

    @property
    def type(self) -> type[np.generic]:
        return self._dtype.type

    @property
    def dtype(self) -> np.dtype:
        return self._dtype

    @property
    def kind(self) -> str:
        return self._dtype.metadata['kind']

    @property
    def _is_numeric(self) -> bool:
        return self.kind in ('i', 'f')

    @property
    def _is_boolean(self) -> bool:
        return self.kind == 'b'

    @classmethod
    def construct_array_type(cls):
        return NPArray

    @classmethod
    def construct_from_string(cls, string: str) -> ExtensionDtype:
        return super().construct_from_string(string)

    @property
    def type_id(self) -> int:
        return self._dtype.metadata['id']

    @property
    def type_name(self) -> str:
        return self._dtype.metadata['name']

    @property
    def type_storage_size(self) -> int:
        value = self._dtype.metadata['storage']
        # TODO: variable storage size
        return value if value > 0 else 1

    def __str__(self):
        return f'{self.type_name}({self.type_id}, metadata:{self.metadata})'


class ColumnTypes:
    UNDEFINED = ColumnType(np.void, {'id': 0, 'kind': 'V', 'name': 'UNDEFINED', 'storage': 0, 'null': None})
    NULL = ColumnType(np.void, {'id': 24, 'kind': 'V', 'name': 'NULL', 'storage': 0, 'null': None})
    BOOLEAN = ColumnType(bool, {'id': 1, 'kind': 'b', 'name': 'BOOLEAN', 'storage': 1, 'null': False})

    # Numeric
    BYTE = ColumnType(np.uint8, {'id': 2, 'kind': 'B', 'name': 'BYTE', 'storage': 1, 'null': None})

    SHORT = ColumnType(np.int16, {'id': 3, 'kind': 'i', 'name': 'SHORT', 'storage': 2, 'null': None})
    INT = ColumnType(np.int32, {'id': 5, 'kind': 'i', 'name': 'INT', 'storage': 4, 'null': 0x80000000})
    LONG = ColumnType(np.int64, {'id': 6, 'kind': 'i', 'name': 'LONG', 'storage': 8, 'null': 0x8000000000000000})
    DATE = ColumnType(np.int64, {'id': 7, 'kind': 'i', 'name': 'DATE', 'storage': 8, 'null': 0x8000000000000000})
    TIMESTAMP = ColumnType(np.int64,
                           {'id': 8, 'kind': 'i', 'name': 'TIMESTAMP', 'storage': 8, 'null': 0x8000000000000000})
    FLOAT = ColumnType(np.float32, {'id': 9, 'kind': 'f', 'name': 'FLOAT', 'storage': 4, 'null': None})
    DOUBLE = ColumnType(np.float64, {'id': 10, 'kind': 'f', 'name': 'DOUBLE', 'storage': 8, 'null': None})
    GEOBYTE = ColumnType(np.int8, {'id': 14, 'kind': 'i', 'name': 'GEOBYTE', 'storage': 1, 'null': -1})
    GEOSHORT = ColumnType(np.int16, {'id': 15, 'kind': 'i', 'name': 'GEOSHORT', 'storage': 2, 'null': -1})
    GEOINT = ColumnType(np.int32, {'id': 16, 'kind': 'i', 'name': 'GEOINT', 'storage': 4, 'null': -1})
    GEOLONG = ColumnType(np.int64, {'id': 17, 'kind': 'i', 'name': 'GEOLONG', 'storage': 8, 'null': -1})

    # UTF-6 based, len prefix no \0 terminated
    CHAR = ColumnType(np.int16, {'id': 4, 'kind': 'i', 'name': 'CHAR', 'storage': 1, 'null': None})
    STRING = ColumnType(np.unicode_, {'id': 11, 'kind': 'U', 'name': 'STRING', 'storage': 2, 'null': None})
    SYMBOL = ColumnType(np.unicode_, {'id': 12, 'kind': 'U', 'name': 'SYMBOL', 'storage': 2, 'null': None})
    LONG256 = ColumnType(np.unicode_, {'id': 13, 'kind': 'U', 'name': 'LONG256', 'storage': 2, 'null': None})

    # not used by pykit
    BINARY = ColumnType(np.ubyte, {'id': 18, 'kind': 'V', 'name': 'BINARY', 'storage': 1, 'null': None})
    PARAMETER = ColumnType(np.void, {'id': 19, 'kind': 'V', 'name': 'PARAMETER', 'storage': 0, 'null': None})
    CURSOR = ColumnType(np.void, {'id': 20, 'kind': 'V', 'name': 'CURSOR', 'storage': 0, 'null': None})
    VAR_ARG = ColumnType(np.void, {'id': 21, 'kind': 'V', 'name': 'VAR_ARG', 'storage': 0, 'null': None})
    RECORD = ColumnType(np.void, {'id': 22, 'kind': 'V', 'name': 'RECORD', 'storage': 0, 'null': None})
    GEOHASH = ColumnType(np.int64, {'id': 23, 'kind': 'i', 'name': 'GEOHASH', 'storage': 8, 'null': None})

    __values = (  # leave them in this order
        UNDEFINED, BOOLEAN, BYTE, SHORT, CHAR, INT,
        LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, STRING,
        SYMBOL, LONG256, GEOBYTE, GEOSHORT, GEOINT, GEOLONG,
        BINARY, PARAMETER, CURSOR, VAR_ARG, RECORD, GEOHASH,
        NULL
    )

    @staticmethod
    def resolve(type_id: int) -> ColumnType:
        for col_type in ColumnTypes.__values:
            if type_id == col_type.type_id:
                return col_type
        return ColumnType.UNDEFINED


class NPArray(np.ndarray):
    def __new__(cls,
                col_file: str,
                row_count: int,
                col_type: ColumnType,
                col_mmap: mmap.mmap):

        if isinstance(col_mmap, mmap.mmap):
            col_np_array = np.ndarray.__new__(
                NPArray,
                shape=(row_count,),
                dtype=col_type,
                buffer=col_mmap,
                offset=0,
                order='C')
            col_np_array.filename = col_file
            col_np_array.mode = 'rb'
            col_np_array.flags['WRITEABLE'] = False
            col_np_array.flags['ALIGNED'] = True
            col_np_array._mmap = col_mmap
        else:
            col_np_array = np.array(
                col_mmap,
                dtype=col_type,
                copy=False)
        return col_np_array

    @classmethod
    def _from_sequence(cls, scalars: typing.List[typing.Any], dtype: ColumnType, copy: bool = False):
        return NPArray(None, None, dtype, scalars)
