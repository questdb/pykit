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


class ColumnType:
    def __init__(self,
                 type_id: int,
                 type_name: str,
                 type_storage_size: int,
                 type_dtype: str = None):
        self.type_id = type_id
        self.type_name = type_name
        self.type_storage_size = type_storage_size
        self.type_dtype = type_dtype

    @property
    def supports_dataframe(self):
        return self.type_dtype is not None

    def __str__(self):
        result_str = f'{self.type_name}('
        result_str += f'id:{self.type_id}, '
        result_str += f'storage size:{self.type_storage_size}, '
        result_str += f'dtype:{self.type_dtype}, '
        result_str += f'dataframe?:{self.supports_dataframe})'
        return result_str


class ColumnTypes:
    def __init__(self):
        self.types = (
            ColumnType(0, 'UNDEFINED', None, None),
            ColumnType(1, 'BOOLEAN', 1, np.dtype(bool)),
            ColumnType(2, 'BYTE', 1, None),
            ColumnType(3, 'SHORT', 2, None),
            ColumnType(4, 'CHAR', 2, None),
            ColumnType(5, 'INT', 4, np.int32),
            ColumnType(6, 'LONG', 8, np.int64),
            ColumnType(7, 'DATE', 8, np.int64),
            ColumnType(8, 'TIMESTAMP', 8, np.int64),
            ColumnType(9, 'FLOAT', 4, None),
            ColumnType(10, 'DOUBLE', 8, np.float64),
            ColumnType(11, 'STRING', 2, None),
            ColumnType(12, 'SYMBOL', 2, None),
            ColumnType(13, 'LONG256', 2, None),
            ColumnType(14, 'GEOBYTE', 1, None),
            ColumnType(15, 'GEOSHORT', 2, None),
            ColumnType(16, 'GEOINT', 4, None),
            ColumnType(17, 'GEOLONG', 8, None),
            ColumnType(18, 'BINARY', 1, None),
            ColumnType(19, 'PARAMETER', None, None),
            ColumnType(20, 'CURSOR', None, None),
            ColumnType(21, 'VAR_ARG', None, None),
            ColumnType(22, 'RECORD', None, None),
            ColumnType(23, 'GEOHASH', None, None),
            ColumnType(24, 'NULL', None, None)
        )

    def __getitem__(self, item: int):
        return self.types[item]

    def resolve(self, type_name: str) -> ColumnType:
        for column_type in self.types:
            if column_type.type_name == type_name:
                return column_type
        return ColumnType(-1, type_name, None)


class PartitionBy:
    def __init__(self, type_id: int, type_name: str):
        self.type_id = type_id
        self.type_name = type_name

    def __str__(self):
        return f'{self.type_name}'


COLUMN_TYPES = ColumnTypes()

PARTITION_BY_DAY = 0
PARTITION_BY_MONTH = 1
PARTITION_BY_YEAR = 2
PARTITION_BY_NONE = 3

PARTITION_BY = (
    PartitionBy(PARTITION_BY_DAY, 'DAY'),
    PartitionBy(PARTITION_BY_MONTH, 'MONTH'),
    PartitionBy(PARTITION_BY_YEAR, 'YEAR'),
    PartitionBy(PARTITION_BY_NONE, 'NONE')
)
