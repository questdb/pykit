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

import math
import os

from pykit import (
    create_table,
    insert_values,
    drop_table,
    to_timestamp,
    df_from_table
)

from tests.unit_tests.util import BaseTestTest


class DataFrameFromTables(BaseTestTest):
    def test_read_only(self):
        def assert_read_only(iloc):
            try:
                iloc[0] = 999  # Should be non-editable
                self.fail("MUST BE READ ONLY (1)")
            except ValueError as e:
                self.assertEqual('assignment destination is read-only', str(e))

        table_name = 'test_read_only'
        columns = (('int', 'INT'), ('double', 'DOUBLE'), ('ts', 'TIMESTAMP'))
        drop_table(table_name)
        create_table(table_name, columns, designated='ts')
        try:
            insert_values(
                table_name,
                columns,
                (0, 1.000001, to_timestamp('2021-10-01 02:00:00.123456')),
                (1, 2.002002, to_timestamp('2021-10-01 02:01:00.123456')),
                (2, 4.404404, to_timestamp('2021-10-02 02:02:00.123456')))
            dataframes = df_from_table(table_name, columns)
            self.assertEqual(1, len(dataframes))
            df = dataframes[0]
            assert_read_only(df["int"].iloc)
            assert_read_only(df["double"].iloc)
            assert_read_only(df.index.array)
            try:
                df["ts"].iloc
            except KeyError as ke:
                self.assertEqual("'ts'", str(ke))
        finally:
            drop_table(table_name)

    def test_no_partitions(self):
        table_name = 'test_no_partitions'
        columns = (
            ('int', 'INT'),
            ('double', 'DOUBLE'),
            ('ts', 'TIMESTAMP'))
        drop_table(table_name)
        create_table(table_name, columns, designated='ts')
        try:
            insert_values(
                table_name,
                columns,
                (0, 1.000001, to_timestamp('2021-10-01 02:00:00.123456')),
                (1, 2.002002, to_timestamp('2021-10-01 02:01:00.123456')),
                (2, 4.404404, to_timestamp('2021-10-02 02:02:00.123456')),
                (3, 22 / 7, to_timestamp('2021-10-02 02:03:00.123456')),
                (4, 0.798117, to_timestamp('2021-10-03 02:04:00.123456')),
                (5, math.sqrt(2), to_timestamp('2021-10-03 02:05:00.123456')),
                (6, math.sin(math.radians(45.0)), to_timestamp('2021-10-03 02:06:00.123456'))
            )
            self.assert_table_content(
                table_name,
                '(0, 1.000001, datetime.datetime(2021, 10, 1, 2, 0, 0, 123456))' + os.linesep +
                '(1, 2.002002, datetime.datetime(2021, 10, 1, 2, 1, 0, 123456))' + os.linesep +
                '(2, 4.4044039999999995, datetime.datetime(2021, 10, 2, 2, 2, 0, 123456))' + os.linesep +
                '(3, 3.1428571428571432, datetime.datetime(2021, 10, 2, 2, 3, 0, 123456))' + os.linesep +
                '(4, 0.798117, datetime.datetime(2021, 10, 3, 2, 4, 0, 123456))' + os.linesep +
                '(5, 1.4142135623730951, datetime.datetime(2021, 10, 3, 2, 5, 0, 123456))' + os.linesep +
                '(6, 0.7071067811865475, datetime.datetime(2021, 10, 3, 2, 6, 0, 123456))' + os.linesep)
            dataframes = df_from_table(table_name, columns)
            self.assertEqual(1, len(dataframes))
            df = dataframes[0]
            self.assertEqual(
                '                  int    double' + os.linesep +
                '1633053600123456    0  1.000001' + os.linesep +
                '1633053660123456    1  2.002002' + os.linesep +
                '1633140120123456    2  4.404404' + os.linesep +
                '1633140180123456    3  3.142857' + os.linesep +
                '1633226640123456    4  0.798117' + os.linesep +
                '1633226700123456    5  1.414214' + os.linesep +
                '1633226760123456    6  0.707107',
                str(df))
            self.assertEqual("Index(['int', 'double'], dtype='object')", str(df.columns))
            self.assertEqual((7, 2), df.shape)
            self.assertEqual(7, len(df))
        finally:
            drop_table(table_name)

    def test_with_partitions(self):
        table_name = 'test_with_partitions'
        columns = (
            ('int', 'INT'),
            ('double', 'DOUBLE'),
            ('ts', 'TIMESTAMP'))
        drop_table(table_name)
        create_table(table_name, columns, designated='ts', partition_by='DAY')
        try:
            insert_values(
                table_name,
                columns,
                (0, 1.000001, to_timestamp('2021-10-01 02:00:00.123456')),
                (1, 2.002002, to_timestamp('2021-10-01 02:01:00.123456')),
                (2, 4.404404, to_timestamp('2021-10-02 02:02:00.123456')),
                (3, 22 / 7, to_timestamp('2021-10-01 02:03:00.123456')),
                (4, 0.798117, to_timestamp('2021-10-02 02:04:00.123456')),
                (5, math.sqrt(2), to_timestamp('2021-10-03 02:05:00.123456')),
                (6, math.sin(math.radians(45.0)), to_timestamp('2021-10-01 02:06:00.123456'))
            )
            self.assert_table_content(
                table_name,
                '(0, 1.000001, datetime.datetime(2021, 10, 1, 2, 0, 0, 123456))' + os.linesep +
                '(1, 2.002002, datetime.datetime(2021, 10, 1, 2, 1, 0, 123456))' + os.linesep +
                '(3, 3.1428571428571432, datetime.datetime(2021, 10, 1, 2, 3, 0, 123456))' + os.linesep +
                '(6, 0.7071067811865475, datetime.datetime(2021, 10, 1, 2, 6, 0, 123456))' + os.linesep +
                '(2, 4.4044039999999995, datetime.datetime(2021, 10, 2, 2, 2, 0, 123456))' + os.linesep +
                '(4, 0.798117, datetime.datetime(2021, 10, 2, 2, 4, 0, 123456))' + os.linesep +
                '(5, 1.4142135623730951, datetime.datetime(2021, 10, 3, 2, 5, 0, 123456))' + os.linesep)
            dataframes = df_from_table(table_name, columns)
            self.assertEqual(3, len(dataframes))
            df = dataframes[0]
            self.assertEqual(
                '                  int    double' + os.linesep +
                '1633053600123456    0  1.000001' + os.linesep +
                '1633053660123456    1  2.002002' + os.linesep +
                '1633053780123456    3  3.142857' + os.linesep +
                '1633053960123456    6  0.707107',
                str(df))
            self.assertEqual("Index(['int', 'double'], dtype='object')", str(df.columns))
            self.assertEqual((4, 2), df.shape)
            self.assertEqual(4, len(df))

            df = dataframes[1]
            self.assertEqual(
                '                  int    double' + os.linesep +
                '1633140120123456    2  4.404404' + os.linesep +
                '1633140240123456    4  0.798117',
                str(df))
            self.assertEqual("Index(['int', 'double'], dtype='object')", str(df.columns))
            self.assertEqual((2, 2), df.shape)
            self.assertEqual(2, len(df))

            df = dataframes[2]
            self.assertEqual(
                '                  int    double' + os.linesep +
                '1633226700123456    5  1.414214',
                str(df))
            self.assertEqual("Index(['int', 'double'], dtype='object')", str(df.columns))
            self.assertEqual((1, 2), df.shape)
            self.assertEqual(1, len(df))
        finally:
            drop_table(table_name)