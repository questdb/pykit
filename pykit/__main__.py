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
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

from pykit.core import (QDB_HOME, QDB_DB_ROOT, QDB_DB_CONF, QDB_CLONE_FOLDER)


def _update_command(branch_name: str, force: bool):
    if not QDB_HOME.exists():
        QDB_HOME.mkdir()
        print(f'Created home: {QDB_HOME}')
    if not QDB_DB_ROOT.exists():
        QDB_DB_ROOT.mkdir()
        print(f'Created data root: {QDB_DB_ROOT}')
    if force and QDB_CLONE_FOLDER.exists():
        try:
            shutil.rmtree(QDB_CLONE_FOLDER)
            print('Deleted clone')
        except OSError as e:
            print(f'Error deleting clone: {e.filename} - {e.strerror}.')
            sys.exit(1)
    if not QDB_CLONE_FOLDER.exists():
        subprocess.check_output(
            ['git', 'clone', '-b', branch_name, 'git@github.com:questdb/questdb.git', 'clone'],
            cwd=QDB_HOME)
    else:
        subprocess.check_output(['git', 'checkout', branch_name], cwd=QDB_CLONE_FOLDER)
        subprocess.check_output(['git', 'pull'], cwd=QDB_CLONE_FOLDER)
    subprocess.check_output(['mvn', 'clean', 'install', '-DskipTests'], cwd=QDB_CLONE_FOLDER)
    print('Update completed')


def _start_command():
    qdb_jar = _find_jar()
    if not qdb_jar:
        print(f'QuestDB jar not found, try updating first.')
        sys.exit(1)
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
        print('QuestDB conf folder not found')
        import pykit

        src_file = Path(pykit.__file__).parent / 'resources' / 'conf.zip'
        with zipfile.ZipFile(src_file, 'r') as zip_ref:
            for file in zip_ref.namelist():
                zip_ref.extract(member=file, path=QDB_DB_ROOT)
            print(f'Created default conf: {QDB_DB_CONF}')


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


def _args_parser() -> argparse.ArgumentParser:
    args_parser = argparse.ArgumentParser(description='Pykit commands to run QuestDB')
    command = args_parser.add_subparsers(dest='command')
    command.add_parser('start', help='Starts QuestDB in localhost')
    update = command.add_parser('update', help='Clones/builds QuestDB\'s github repo')
    update.add_argument(
        '--branch',
        default=None,
        type=str,
        help='prepare a QuestDB node from the latest version of BRANCH (default master)')
    update.add_argument(
        '--force',
        default=False,
        type=bool,
        help='FORCE True to delete/clone/build QuestDB\'s github repo')
    return args_parser


if __name__ == '__main__':
    parser = _args_parser()
    args = parser.parse_args()
    if args.command is None:
        parser.print_help()
    elif args.command == 'start':
        _start_command()
    elif args.command == 'update':
        _update_command(args.branch, args.force)
