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

import psycopg2
import typing

CONN_ATTRS = {
    'user': 'admin',
    'password': 'quest',
    'host': '127.0.0.1',
    'port': '8812',
    'database': 'qdb'
}

Cursor = typing.NewType('Cursor', psycopg2.extensions.cursor)
CursorConsumer = typing.NewType('CursorConsumer', typing.Callable[[Cursor], typing.Any])


def with_cursor(consumer: CursorConsumer) -> typing.Any:
    with psycopg2.connect(**CONN_ATTRS) as conn:
        conn.autocommit = False
        with conn.cursor() as stmt_cursor:
            result = consumer(stmt_cursor)
            conn.commit()
    return result


def select_all(table_name: str, limit: int = 10) -> typing.List[typing.Tuple[typing.Any, ...]]:
    def _select_all(stmt_cursor: Cursor) -> typing.List[typing.Tuple[typing.Any, ...]]:
        try:
            stmt_cursor.execute(f'{table_name} LIMIT {limit};')
            return stmt_cursor.fetchall()
        except (Exception, psycopg2.Error) as error:
            print(f'Notice table [{table_name}]: {error}')
            return None

    return with_cursor(_select_all)


def insert_values(table_name: str,
                  columns: typing.Tuple[typing.Tuple[str, str], ...],
                  *values: typing.Tuple[typing.Any, ...]) -> None:
    num_columns = len(columns)
    for value in values:
        _len = len(value)
        if _len != num_columns:
            raise ValueError(f'all tuples must be of len {num_columns}')

    def _insert_table(stmt_cursor: Cursor) -> None:
        insert_stmt = f'insert into {table_name} values({", ".join("%s" for _ in range(num_columns))})'
        stmt_cursor.executemany(insert_stmt, values)

    with_cursor(_insert_table)


def create_table(table_name: str,
                 columns: typing.List[typing.Tuple[str, str]],
                 designated: str = None,
                 partition_by: str = 'NONE') -> bool:
    def _create_table(stmt_cursor: Cursor) -> None:
        statement = f'CREATE TABLE IF NOT EXISTS {table_name} ('
        statement += ', '.join(f'{col_name} {col_type}' for col_name, col_type in columns)
        statement += f')'
        if designated:
            statement += f' TIMESTAMP({designated}) PARTITION BY {partition_by}'
        stmt_cursor.execute(statement)

    try:
        with_cursor(_create_table)
        return True
    except (Exception, psycopg2.Error) as create_error:
        print(f'Error while creating table [{table_name}]: {create_error}')
        return False


def drop_table(table_name: str) -> bool:
    def _drop_table(stmt_cursor: Cursor) -> bool:
        try:
            stmt_cursor.execute(f'DROP table {table_name};')
            return True
        except (Exception, psycopg2.Error) as error:
            return False

    return with_cursor(_drop_table)


def drop_tables(name_prefix: str, total: int) -> None:
    def _drop_tables(stmt_cursor: Cursor) -> None:
        for idx in range(total):
            try:
                table_name = f'{name_prefix}{idx}'
                stmt_cursor.execute(f'DROP table {table_name};')
            except (Exception, psycopg2.Error) as error:
                print(f'Notice table [{table_name}]: {error}')

    with_cursor(_drop_tables)


def report_version():
    def _report_version(stmt_cursor: Cursor) -> None:
        stmt_cursor.execute("SELECT version();")
        print(stmt_cursor.fetchone())

    with_cursor(_report_version)


if __name__ == "__main__":
    report_version()
