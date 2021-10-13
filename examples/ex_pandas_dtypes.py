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
import numpy as np
import pandas as pd

from pykit import (
    create_table,
    insert_values,
    drop_table,
    df_from_table
)

if __name__ == "__main__":
    pd.set_option('display.width', 800)
    pd.set_option('max_columns', 4)

    starts = datetime.datetime.utcfromtimestamp(1631607757).strftime('%c')
    df_index = pd.date_range(start=starts, periods=4, freq='S', name='Idx')
    df = pd.DataFrame({
        'col1': [1.0, 2.0, 3.14, 2.68],
        'col2': [100, 200, 314, 218],
        'col3': [1.0, 4.0, 6.28, 4.36]
    }, index=df_index).astype(dtype={
        'col1': np.float64,
        'col2': np.int32,
        'col3': np.float64
    }, copy=False)
    print('Standard:')
    print(df)

    table_name = 'dtypes'
    columns = (('col1', 'DOUBLE'), ('col2', 'INT'), ('col3', 'DOUBLE'))
    drop_table(table_name)
    create_table(table_name, columns)
    values = []
    for _index, row in df.iterrows():
        values.append((row['col1'], int(row['col2']), row['col3']))
    insert_values(table_name, columns, *values)
    df_pk = df_from_table(table_name, columns, usr_index=df_index)
    print('From table bin files:')
    print(df_pk)
    print(df_pk.dtypes)
    drop_table(table_name)
