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

import unittest

from pykit import (
    to_timestamp,
    from_timestamp,
    to_date,
    from_date
)


class TimestampTest(unittest.TestCase):
    def test_timestamp(self):
        timestamp_value = to_timestamp('2021-10-01 09:38:42.123456')
        self.assertEqual('2021-10-01 09:38:42.123456', from_timestamp(timestamp_value))

    def test_date(self):
        date_value = to_date('2021-10-01')
        self.assertEqual('2021-10-01', from_date(date_value))
