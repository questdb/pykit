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

import pandas as pd

from pykit import (
    create_table,
    insert_values,
    drop_table,
    to_timestamp,
    to_date,
    df_from_table
)

if __name__ == '__main__':
    table_name = 'dataframe'
    columns = (
        ('long', 'LONG'),
        ('int', 'INT'),
        ('bool', 'BOOLEAN'),
        ('date', 'DATE'),
        ('double', 'DOUBLE'),
        ('ts', 'TIMESTAMP'))
    drop_table(table_name)
    create_table(table_name, columns, designated='ts', partition_by='NONE')
    insert_values(
        table_name,
        columns,
        (0, 1, True, to_date('2021-10-01'), 13.2745337223234, to_timestamp('2021-10-01 02:00:20.123456')),
        (1, 1, False, to_date('2021-10-01'), 23.2333334, to_timestamp('2021-10-01 02:00:22.123456')),
        (2, 1, True, to_date('2021-10-01'), None, to_timestamp('2021-10-01 02:01:07.123456')),
        (3, 1, None, to_date('2021-10-01'), 313.3223234, to_timestamp('2021-10-01 02:10:42.123456')),
        (4, None, False, to_date('2021-10-02'), 14.233443234, to_timestamp('2021-10-02 22:00:40.123456')),
        (5, 10, False, to_date('2021-10-02'), 144.233443234, to_timestamp('2021-10-02 22:10:14.123456')),
        (6, 100, False, to_date('2021-10-03'), 15.233883234, to_timestamp('2021-10-03 12:00:59.123456')),
        (None, 100, False, to_date('2021-10-03'), 215.233883234, to_timestamp('2021-10-03 12:00:59.123459')),
        (8, 100, False, None, 1015.233883234, to_timestamp('2021-10-03 13:00:00.000020')),
        (9, 2, True, to_date('2021-11-01'), 13.2745337223234, to_timestamp('2021-11-01 02:00:20.123456')),
        (10, 2, False, to_date('2021-11-01'), 23.2333334, to_timestamp('2021-11-01 02:00:22.123456')),
        (11, 2, True, to_date('2021-12-01'), None, to_timestamp('2021-12-01 02:01:07.123456')),
        (12, 2, None, to_date('2021-12-01'), 313.3223234, to_timestamp('2021-12-01 02:10:42.123456')),
        (13, None, False, to_date('2021-12-31'), 14.233443234, to_timestamp('2021-12-31 22:00:40.123456')),
        (14, 12, False, to_date('2021-12-31'), 144.233443234, to_timestamp('2021-12-31 23:59:59.999999')),
        (15, 101, True, to_date('2022-01-01'), 15.233883234, to_timestamp('2022-01-01 00:00:00.000000')),
        (None, 101, True, to_date('2022-01-03'), 215.233883234, to_timestamp('2022-01-03 12:00:59.123459')),
        (17, 101, False, None, 1015.233883234, to_timestamp('2022-11-03 13:00:00.000020')),
    )
    pd.set_option('display.width', 800)
    pd.set_option('max_columns', len(columns))
    for df in df_from_table(table_name, columns):
        print(df.loc[to_timestamp('2022-11-03 13:00:00.000020')]['long'])
        print(df)
    drop_table(table_name)
