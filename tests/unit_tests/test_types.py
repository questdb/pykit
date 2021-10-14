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

import typing

import numpy as np
import pandas as pd

from pykit import (
    ColumnTypes,
    ColumnType
)

from tests.unit_tests.util import BaseTestTest


class TypesTest(BaseTestTest):
    def test_types(self):
        print()
        self._test_type(ColumnTypes.BOOLEAN, [True, np.nan, False, None, True, True]) # bool, cannot use pd.NA
        # self._test_type(ColumnTypes.BYTE, [255, np.nan, 0, None, 128, pd.NA]) # Int8Dtype
        # self._test_type(ColumnTypes.SHORT, [10000, np.nan, 0, None, -128, pd.NA]) # Int16Dtype
        # self._test_type(ColumnTypes.INT, [314159, pd.NA, 0, None, 271828, pd.NA]) # Int32Dtype
        # self._test_type(ColumnTypes.LONG, [314159, pd.NA, 0, None, 271828, pd.NA]) # Int64Dtype
        # self._test_type(ColumnTypes.DATE, [314159, pd.NA, 0, None, 271828, pd.NA])
        # self._test_type(ColumnTypes.TIMESTAMP, [314159, pd.NA, 0, None, 271828, pd.NA])
        # self._test_type(ColumnTypes.FLOAT, [3.14159, np.NaN, 0.0, None, 2.71828, pd.NA])
        # self._test_type(ColumnTypes.DOUBLE, [3.14159, np.NaN, 0.0, None, 2.71828, pd.NA])

    def _test_type(self, series_type: ColumnType, values: typing.List[typing.Any]):
        series = pd.Series(data=values, dtype=series_type)

        series_bytes = series.values.tobytes('C')
        total_bytes = len(series_bytes)
        num_values = len(series)
        # self.assertEqual(6, num_values)
        self.assertEqual(total_bytes, num_values * series_type.type_storage_size)
        print(f'{series_type.type_name}({series_type.type_storage_size}b)*{num_values}: {series_bytes}')
        for i in range(0, total_bytes, series_type.type_storage_size):
            item_bytes = series_bytes[i:i + series_type.type_storage_size]
            item_idx = int(i / series_type.type_storage_size)
            item = series[item_idx]
            if pd.isna(item):
                print(f'{item_idx}: {item}, offset:[{i}:{i + len(item_bytes)}], bytes: {item_bytes}')
