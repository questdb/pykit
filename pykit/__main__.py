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

import argparse
import os
import zipfile
from pathlib import Path
import subprocess
import shutil
import sys

from pykit.core import (QDB_HOME, QDB_DB_ROOT, QDB_DB_CONF, QDB_CLONE_FOLDER)


def _update_command(force: bool = False):
    if not QDB_HOME.exists():
        QDB_HOME.mkdir()
        print(f'Created QuestDB\'s home: {QDB_HOME}')
    if not QDB_DB_ROOT.exists():
        QDB_DB_ROOT.mkdir()
        print(f'Created QuestDB\'s data ROOT dir: {QDB_DB_ROOT}')
    if force and QDB_CLONE_FOLDER.exists():
        try:
            shutil.rmtree(QDB_CLONE_FOLDER)
            print('Deleted QuestDB\'s clone')
        except OSError as e:
            print(f'Error deleting QuestDB\'s clone: {e.filename} - {e.strerror}.')
            sys.exit(1)
    if not QDB_CLONE_FOLDER.exists():
        print('Cloning QuestDB')
        subprocess.check_output(['git', 'clone', 'git@github.com:questdb/questdb.git', 'clone'], cwd=QDB_HOME)
    else:
        print('Updating QuestDB\'s clone')
        subprocess.check_output(['git', 'pull'], cwd=QDB_CLONE_FOLDER)
    print("Building QuestDB\'s clone")
    subprocess.check_output(['mvn', 'clean', 'install', '-DskipTests'], cwd=QDB_CLONE_FOLDER)
    print('Update completed')


def _start_command():
    qdb_jar = _find_jar()
    if not qdb_jar:
        print('QuestDB\'s jar not found, updating')
        _update_command(force=False)
        qdb_jar = _find_jar()
    print(f'QuestDB\'s jar: {qdb_jar}')
    _ensure_conf_exists()
    try:
        comand = [
            'java',
            '-ea',
            '-Dnoebug',
            '-XX:+UnlockExperimentalVMOptions',
            '-XX:+AlwaysPreTouch',
            '-XX:+UseParallelOldGC',
            '-Dout=/log.conf',
            '-cp', f'{QDB_DB_CONF}:{qdb_jar}',
            'io.questdb.ServerMain',
            '-d', str(QDB_DB_ROOT),
            '-n'  # disable handling of SIGHUP (close on terminal close)
        ]
        with subprocess.Popen(comand,
                              stdout=subprocess.PIPE,
                              universal_newlines=True,
                              cwd=QDB_CLONE_FOLDER) as qdb_proc_pipe:
            for stdout_line in iter(qdb_proc_pipe.stdout.readline, ''):
                print(stdout_line, end='')
            return_code = qdb_proc_pipe.wait()
        if return_code:
            raise subprocess.CalledProcessError(return_code, comand)
    except KeyboardInterrupt:
        pass


def _ensure_conf_exists() -> None:
    if not QDB_DB_CONF.exists():
        print('QuestDB\'s conf folder not found')
        import pykit

        src_file = Path(pykit.__file__).parent / 'resources' / 'conf.zip'
        with zipfile.ZipFile(src_file, 'r') as zip_ref:
            for file in zip_ref.namelist():
                zip_ref.extract(member=file, path=QDB_DB_ROOT)
            print(f'Created default QuestDB conf: {QDB_DB_CONF}')


def _find_jar() -> Path:
    dirs = [QDB_CLONE_FOLDER]
    while dirs:
        cur_dir = dirs.pop()
        if cur_dir.exists():
            for file_name in os.listdir(cur_dir):
                candidate = cur_dir / file_name
                if candidate.is_dir():
                    dirs.append(candidate)
                elif is_qdb_jar(file_name):
                    return candidate
    return None


def is_qdb_jar(file_name: str) -> bool:
    return file_name.endswith('.jar') and file_name.startswith('questdb-') and '-tests' not in file_name


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='QuestDB developer tools')
    subparsers = parser.add_subparsers(dest='command')
    update_parser = subparsers.add_parser('update', help='Updates local QuestDB\'s clone and builds it')
    update_parser.add_argument('--force', default=False, type=bool)
    start_parser = subparsers.add_parser('start', help='Starts QuestDB')
    args = parser.parse_args()
    if args.command is None:
        parser.print_help()
    elif args.command == 'update':
        _update_command(args.force)
    elif args.command == 'start':
        _start_command()
