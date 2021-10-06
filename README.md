# pykit

A Python-based RPC-like toolkit for interfacing with QuestDB.

## Requirements

- [Python 3.9](https://www.python.org/downloads/release/python-390/)
- [Java Azul](https://www.azul.com/downloads/?package=jdk)
- [Maven](https://maven.apache.org/download.cgi)
- [Git](https://git-scm.com/download)

## Install

```shell
git clone git@github.com:questdb/pykit.git
cd pykit
pip3 install -r requirements.txt
pip3 install -e . 
```

To uninstall:

```shell
pip3 uninstall pykit
```

To publish to Pypi:

[Read this.](https://gist.github.com/asaah18/5dfda79cbddf9ef6a5b74587dfb9e706#publish-a-package-in-pypi)

### Start/Update QuestDB

Pykit requires a local instance of QuestDB running along, for convenience, you can have Pykit launch it:

- `python3 -m pykit start`: starts QuestDB from the local build.
- `python3 -m pykit update`: clones QuestDB (or pulls master) and builds it locally (called by `start` as needed).

The running instance's working directory can be found in folder `.questdb` under the user's home folder, with structure:

- **clone**: contains QuestDB's git clone, the local build, and the logs file.
- **ROOT/conf**: contains QuestDB's configuration files (default files are created on first start).
- **ROOT/db**: contains QuestDB's database files. Each table will have a matching folder (same name) in here.

```shell
<user home>/.questdb/ROOT 
<user home>/.questdb/ROOT/conf/server.conf
<user home>/.questdb/ROOT/conf/log.conf
<user home>/.questdb/ROOT/conf/date.formats
<user home>/.questdb/ROOT/conf/mime.types
<user home>/.questdb/ROOT/db 
<user home>/.questdb/clone
<user home>/.questdb/clone/questdb.log 
```
