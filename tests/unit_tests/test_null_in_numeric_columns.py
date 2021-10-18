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

import numpy as np
import pandas as pd
from pathlib import Path
import struct
from tests.unit_tests.util import (BaseTestTest, dataframe)


# https://github.com/numpy/numpy/issues/14821


class NullInNumericColumnsTest(BaseTestTest):
    def test_masked_byte_array(self):
        self._test_masked_numeric_array(
            Path('resources') / 'masked_byte_array.npy',
            col_name='UInt8',
            pack_type='B',
            row_count=10,
            dtype=np.uint8,
            storage=1,
            na_value=0,
            expected='     UInt8' + os.linesep +
                     'Idx       ' + os.linesep +
                     '0     <NA>' + os.linesep +
                     '1        1' + os.linesep +
                     '2     <NA>' + os.linesep +
                     '3        3' + os.linesep +
                     '4        4' + os.linesep +
                     '5     <NA>' + os.linesep +
                     '6        6' + os.linesep +
                     '7        7' + os.linesep +
                     '8        8' + os.linesep +
                     '9        9')

    def test_masked_short_array(self):
        self._test_masked_numeric_array(
            Path('resources') / 'masked_short_array.npy',
            col_name='Int16',
            pack_type='h',
            row_count=10,
            dtype=np.int16,
            storage=2,
            na_value=0,
            expected='     Int16' + os.linesep +
                     'Idx       ' + os.linesep +
                     '0     <NA>' + os.linesep +
                     '1        1' + os.linesep +
                     '2     <NA>' + os.linesep +
                     '3        3' + os.linesep +
                     '4        4' + os.linesep +
                     '5     <NA>' + os.linesep +
                     '6        6' + os.linesep +
                     '7        7' + os.linesep +
                     '8        8' + os.linesep +
                     '9        9')

    def test_masked_integer_array(self):
        self._test_masked_numeric_array(
            Path('resources') / 'masked_integer_array.npy',
            col_name='Int32',
            pack_type='i',
            row_count=10,
            dtype=np.int32,
            storage=4,
            na_value=-2147483648,
            expected='     Int32' + os.linesep +
                     'Idx       ' + os.linesep +
                     '0        0' + os.linesep +
                     '1        1' + os.linesep +
                     '2     <NA>' + os.linesep +
                     '3        3' + os.linesep +
                     '4        4' + os.linesep +
                     '5     <NA>' + os.linesep +
                     '6        6' + os.linesep +
                     '7        7' + os.linesep +
                     '8        8' + os.linesep +
                     '9        9')

    def test_masked_long_array(self):
        self._test_masked_numeric_array(
            Path('resources') / 'masked_long_array.npy',
            col_name='Int64',
            pack_type='l',
            row_count=10,
            dtype=np.int64,
            storage=8,
            na_value=-9223372036854775808,
            expected='     Int64' + os.linesep +
                     'Idx       ' + os.linesep +
                     '0        0' + os.linesep +
                     '1        1' + os.linesep +
                     '2     <NA>' + os.linesep +
                     '3        3' + os.linesep +
                     '4        4' + os.linesep +
                     '5     <NA>' + os.linesep +
                     '6        6' + os.linesep +
                     '7        7' + os.linesep +
                     '8        8' + os.linesep +
                     '9        9')

    def test_masked_float_array(self):
        self._test_masked_numeric_array(
            Path('resources') / 'masked_float_array.npy',
            col_name='Float32',
            pack_type='f',
            row_count=10,
            dtype=np.float32,
            storage=4,
            na_value=np.nan,
            expected='     Float32' + os.linesep +
                     'Idx         ' + os.linesep +
                     '0        0.0' + os.linesep +
                     '1        1.0' + os.linesep +
                     '2        NaN' + os.linesep +
                     '3        3.0' + os.linesep +
                     '4        4.0' + os.linesep +
                     '5        NaN' + os.linesep +
                     '6        6.0' + os.linesep +
                     '7        7.0' + os.linesep +
                     '8        8.0' + os.linesep +
                     '9        9.0',
            cls=pd.arrays.FloatingArray)

    def test_masked_double_array(self):
        self._test_masked_numeric_array(
            Path('resources') / 'masked_double_array.npy',
            col_name='Float64',
            pack_type='d',
            row_count=10,
            dtype=np.float64,
            storage=8,
            na_value=np.nan,
            expected='     Float64' + os.linesep +
                     'Idx         ' + os.linesep +
                     '0        0.0' + os.linesep +
                     '1        1.0' + os.linesep +
                     '2        NaN' + os.linesep +
                     '3        3.0' + os.linesep +
                     '4        4.0' + os.linesep +
                     '5        NaN' + os.linesep +
                     '6        6.0' + os.linesep +
                     '7        7.0' + os.linesep +
                     '8        8.0' + os.linesep +
                     '9        9.0',
            cls=pd.arrays.FloatingArray)

    def _test_masked_numeric_array(self,
                                   file_path: Path,
                                   col_name: str,
                                   pack_type: str,
                                   row_count: int,
                                   dtype: np.dtype,
                                   storage: int,
                                   na_value: int,
                                   expected: str,
                                   cls=None):
        with open(file_path, mode='wb') as col_file:
            for i in range(row_count):
                col_file.write(struct.pack(pack_type, na_value if i in (2, 5) else dtype(i)))
        df = dataframe(file_path, col_name, row_count, dtype, storage, na_value, cls)
        self.assertEqual(expected, str(df))
        self.assertEqual(col_name, str(df.dtypes[col_name]))
