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

from pykit import (
    create_table,
    insert_values,
    select_all,
    drop_table,
    to_timestamp
)

if __name__ == "__main__":
    table_name = 'test_table_pgwire'
    columns = (('id', 'INT'), ('value', 'DOUBLE'), ('ts', 'TIMESTAMP'))
    drop_table(table_name)
    create_table(table_name, columns, designated='ts', partition_by='DAY')
    insert_values(
        table_name,
        columns,
        (0, 1.000001, to_timestamp('2021-10-01 02:00:00.123456')),
        (1, 2.002002, to_timestamp('2021-10-01 02:01:00.123456')),
        (2, 4.404404, to_timestamp('2021-10-02 02:02:00.123456')),
        (3, 22 / 7, to_timestamp('2021-10-02 02:03:00.123456')),
        (4, None, to_timestamp('2021-10-01 02:04:00.123456')))
    for row in select_all(table_name):
        print(row)
    drop_table(table_name)
