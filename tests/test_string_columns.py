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
from pathlib import Path

import pandas as pd
from tests.util import (BaseTestTest)

from pykit import (
    to_timestamp,
)

OUR_STRING = 'Miguel investigating 控网站漏洞风 and комитета'


class ResearchQuestionsTest(BaseTestTest):

    def test_string_type(self):
        file_path = Path('resources') / 'string_type.feather'
        df = pd.DataFrame(
            {
                'string': ['QuestDB', 'pykit', OUR_STRING]
            },
            index=np.asarray([
                to_timestamp('2021-10-01 02:00:00.123456'),
                to_timestamp('2021-10-02 02:02:00.123456'),
                to_timestamp('2021-10-03 02:04:00.123456')], dtype=np.int64))
        print(df.dtypes)




