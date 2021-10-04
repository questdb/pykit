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

import datetime
import pandas as pd
import typing


def create_time_series(values: typing.List[typing.Any], start_utc_milli_ts: int, period_milli: int) -> pd.Series:
    data_no_index = pd.DataFrame(values)
    size = data_no_index[data_no_index.columns[0]].count()
    freq = pd.tseries.offsets.DateOffset(microseconds=period_milli * 1e3)
    starts = datetime.datetime.utcfromtimestamp(start_utc_milli_ts / 1e3).strftime('%c')
    index = pd.date_range(start=starts, periods=size, freq=freq)
    return data_no_index.set_index(index).iloc[:, 0]


if __name__ == "__main__":
    start_utc_milli_ts = 1631607757000
    starts = datetime.datetime.utcfromtimestamp(start_utc_milli_ts / 1e3).strftime('%c')

    df = pd.DataFrame(
        {
            'col1': [1.0, 2.0, 3.14, None],
            'col2': [100, 200, 314, 218],
            'col3': [1.0, 4.0, 6.28, 4.36]
        },
        index=pd.date_range(start=starts, periods=4, freq='S'))  # 1 sec

    print(df)
    print(df.dtypes)
    print(f'\nindex: {df.index}')
    print(f'columns: {df.columns}')

    data = create_time_series((v for v in range(100, 120, 2)), 1631607757000, 1000)  # 1 sec
    print(data)
