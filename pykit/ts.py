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

from datetime import datetime as dt

import pytz

TIMESTAMP_FORMAT = '%Y-%m-%d %H:%M:%S.%f'
DATE_FORMAT_DAY = '%Y-%m-%d'


def to_date(date_value: str) -> int:
    return to_timestamp(date_value, DATE_FORMAT_DAY)


def from_date(date_micros: int) -> str:
    return from_timestamp(date_micros, DATE_FORMAT_DAY)


def now_utc() -> int:
    return int(dt.utcnow().timestamp() * 1e6)


def to_timestamp(timestamp_value: str, timestamp_format: str = TIMESTAMP_FORMAT) -> int:
    timestamp = dt.strptime(timestamp_value, timestamp_format)
    timestamp = pytz.utc.localize(timestamp, is_dst=None).astimezone(pytz.utc)
    return int(timestamp.timestamp() * 1e6)


def from_timestamp(timestamp_micros: int, timestamp_format: str = TIMESTAMP_FORMAT) -> str:
    return dt.fromtimestamp(timestamp_micros / 1e6, pytz.utc).strftime(timestamp_format)
