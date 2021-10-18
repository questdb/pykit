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
import mmap
import pandas as pd
from pathlib import Path
from pandas.core.internals import (BlockManager, make_block)
from pandas.core.indexes.base import Index
import struct
import unittest
from tests.unit_tests.util import (BaseTestTest, dataframe)

UTF16_STRING = 'Miguel investigating 控网站漏洞风 and комитета'


class ResearchQuestionsTest(BaseTestTest):

    def test_save_numpy_array_to_file(self):
        print()
        file_path = Path('resources') / 'single_string_array.npy'
        single_string_array = np.array([UTF16_STRING])
        np.save(file_path, single_string_array, allow_pickle=False)
        self.assertEqual(UTF16_STRING, np.load(file_path)[0])

    def test_string_binary_representation(self):
        file_path = Path('resources') / 'single_string_array.npy'
        # file_bytes = bytearray(string, 'utf-16')
        # with open(file_path, mode='wb') as file:
        #     file.write(file_bytes)
        with open(file_path, mode='rb') as col_file:
            col_mmap = mmap.mmap(
                col_file.fileno(),
                length=288,
                flags=mmap.MAP_SHARED,
                access=mmap.ACCESS_READ,
                offset=0)
        column = np.ndarray(shape=(1,), dtype=str, buffer=col_mmap, offset=128, order='C')
        column.flags['WRITEABLE'] = False
        column.flags['ALIGNED'] = True
        df = dataframe(file_path, col_name='string', row_count=1, dtype=str, storage=2, na_value=None)
        print(f'\n{df}')
