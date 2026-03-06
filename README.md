# clickhouse-migrator

We had a problem in Mitgo with delivery of clickhouse migrations to nodes of our cluster. We found there is no fast and easy to use scalable tool for these purposes, so inspired by [Flyway](https://github.com/flyway/flyway) and [clickhouse-migrator](https://github.com/delium/clickhouse-migrator) we created our clickhouse migration tool.

## Table of contents

- [clickhouse-migrator](#clickhouse-migrator)
- [Build & install](#build--install)
  - [Security](#security)
- [Commands](#commands)
- [Docker](#docker)
- [Migration types](#migration-types)
- [Tool restrictions](#tool-restrictions)
- [Environment Variables in Migration Scripts](#environment-variables-in-migration-scripts)
- [Maintainers](#maintainers)

## Build & install

Requires Python 3.12.x and Poetry (see [`pyproject.toml`](pyproject.toml)).

Create poetry virtual environment in PyCharm from [`poetry.lock`](poetry.lock) or install dependencies via Poetry:
```shell
poetry install
```

Execute to see tool command line options:
```shell
poetry run clickhouse-migrator --help
```

Example config file: [`env/.env.example`](env/.env.example).
For local runs, copy the example to `env/.env`.
```shell
cp env/.env.example env/.env
```
Required variables in the config file:
* `ENV` - environment label (used in logs).
* `MIGRATION_HOME` - path to the directory with migration scripts.
* `MIGRATION_TARGET_DB_NAME` - target database name for migrations.
* `MIGRATION_QUEUE_EXEC` - whether to execute migrations using queue mode (`True`/`False`).
* `CH_HOST_LIST` - JSON array string of ClickHouse hosts (e.g., `["host1","host2"]`).
* `CH_PORT` - ClickHouse port.
* `CH_USER` - ClickHouse user.
* `CH_PASSWORD` - ClickHouse password.

### Security

When `env/.env` contains secrets, restrict file permissions:
```shell
chmod 600 env/.env
```

To install `clickhouse-migrator` from wheel in other project:
* execute in project home directory (where [pyproject.toml](pyproject.toml) located):
```shell
poetry build
```
* generated wheel package can be installed via `pip` in other project (replace `0.x.x` with actual package version):
```shell
pip install ./dist/clickhouse_migrator-0.x.x-py3-none-any.whl
```

## Commands

Remove database if it doesn't exist on all nodes of given cluster.
```shell
clickhouse-migrator clean --configfile=env/.env
```

Create database on all nodes of given cluster.
```shell
clickhouse-migrator create_db --configfile=env/.env
```

Catch up migrations to last existing version.
```shell  
clickhouse-migrator migrate --configfile=env/.env
```

## Docker

Build and run the container (entrypoint executes `src/clickhouse_migrator/cli.py` and defaults to `--help`):
```shell
docker build -t clickhouse-migrator .
docker run --rm clickhouse-migrator
```

Run a command with a mounted config directory:
```shell
docker run --rm -v $PWD/env:/env clickhouse-migrator \
  migrate --configfile=/env/.env
```

Run with an env file:
```shell
docker run --rm --env-file ./env/.env clickhouse-migrator \
  migrate --configfile=./env/.env
```

## Migration types

* `v1.1.1_foo.sql` - incrementally versioned scripts or patches. They will be applied only 1 time per host. If you want to roll back your changes - you need to create new patch, that will reverse your changes. 
* `r_bar.sql` - repeatable scripts. These scripts will be reapplied every time when their hash changes. Code inside such file must be adapted to this behaviour. We use it for create or replace statements.

## Tool restrictions

* Migration schema table history is analyzed only on the first host in a cluster when migrating on cluster.
* No revert/rollback migrations are available; create a new patch to reverse changes.

## Environment Variables in Migration Scripts

Migration scripts can use environment variables as parameters using the `{{ VARIABLE_NAME }}` placeholder format. The migrator will automatically extract these placeholders, read corresponding environment variables, and execute the query with bound parameters.

Example migration script:
```sql
CREATE USER '{{ DB_USER }}'@'%' IDENTIFIED BY '{{ DB_PASSWORD }}';
GRANT SELECT ON {{ DATABASE_NAME }}.* TO '{{ DB_USER }}'@'%';
```

## Maintainers

* a.kolodkin@mitgo.com
