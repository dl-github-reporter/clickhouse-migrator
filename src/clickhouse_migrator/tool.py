import datetime
import hashlib
import json
import os
import pathlib
import re
import time
from typing import Callable, List, Generator, Dict, Tuple

from clickhouse_driver.errors import ServerException
from dotenv import dotenv_values, load_dotenv
import pandas as pd
from pandas import Series
from tabulate import tabulate
from clickhouse_driver import Client
from loguru import logger


class Settings:
    settings = None

    def __init__(self, config_file):
        if not config_file:
            raise ValueError('Env file not passed in --configfile parameter')

        self.config_file = config_file
        if os.path.exists(self.config_file):
            self.__config = dotenv_values(self.config_file)
            load_dotenv(dotenv_path=self.config_file)
            logger.info("Environment variables are loaded from .env file")
        else:
            logger.info(f"Env file {self.config_file} not found. Current working dir: {os.getcwd()}")
            raise ValueError(f"Env file {self.config_file} not found")

        self.env = self.__config['ENV']
        self.migrations_home = self.__config['MIGRATION_HOME']
        self.ch_host_list = json.loads(self.__config['CH_HOST_LIST'])
        self.ch_port = self.__config['CH_PORT']
        self.ch_user = self.__config['CH_USER']
        self.ch_password = self.__config['CH_PASSWORD']
        self.target_db_name = self.__config['MIGRATION_TARGET_DB_NAME']
        self.queue_exec = self.__config['MIGRATION_QUEUE_EXEC']


class MigrationTool:
    def __init__(self, settings: Settings):
        self.settings = settings

    def iter_get_ch_client(self, hosts_list: List[str], db_name: str) -> Generator[Client, None, None]:
        for host in hosts_list:
            yield self.get_ch_client(db_host=host, db_name=db_name)

    def execute_on_cluster(self, db_name: str, func: Callable, **kwargs) -> None:
        for client in self.iter_get_ch_client(hosts_list=self.settings.ch_host_list, db_name=db_name):
            func(client, **kwargs)
            client.disconnect()

    @staticmethod
    def execute_and_inflate(client: Client, query: str) -> pd.DataFrame:
        result = client.execute(query, with_column_types=True)
        column_names = [c[0] for c in result[len(result) - 1]]
        return pd.DataFrame([dict(zip(column_names, d)) for d in result[0]])

    def get_ch_client(self, db_host, db_name="") -> Client:
        return Client(
            db_host,
            user=self.settings.ch_user,
            password=self.settings.ch_password,
            port=self.settings.ch_port,
            database=db_name,
            secure=True,
            verify=False,
        )

    def create_schema_migration_history_table(self, client: Client) -> None:
        client.execute(
            f"CREATE TABLE IF NOT EXISTS schema_migration_history "
            f"(version UInt64, migration_type String, md5 String, script String, created_at DateTime DEFAULT now()) "
            f"ENGINE = MergeTree ORDER BY tuple(created_at)"
        )
        logger.info(
            f"Database {self.settings.target_db_name}. Table 'schema_migration_history' was created or already exists "
            f"in database {client.connection.database} on host {list(client.connection.hosts)}"
        )

    def get_tabulated_print_for_df(self, df: pd.DataFrame, message: str = "", fields: Tuple[str, ...] = ()) -> str:
        if fields:
            df = df[list(fields)]

        return (
                f"\nDatabase {self.settings.target_db_name} (migration_type: v - incremental, r - repeatable)\n" +
                f"{message}\n" +
                tabulate(
                    df.astype(str),
                    headers="keys",
                    tablefmt="fancy_grid",
                    floatfmt=".0f",
                    showindex=False,
                )
        )

    def get_incremental_migrations_to_apply(self, client: Client, incoming_state: pd.DataFrame) -> pd.DataFrame:
        logger.info(
            f"Database {self.settings.target_db_name}. Trying to find out what incremental migrations should be "
            f"applied on host {list(client.connection.hosts)}"
        )

        # incoming_state - загруженные миграции ("v" - инкрементальные)
        # current_state - исторические миграции (из db.schema_migration_history)
        incoming_state = incoming_state[incoming_state.migration_type == "v"]
        current_state = self.execute_and_inflate(
            client,
            "SELECT version AS version, script AS c_script, md5 as c_md5 "
            "from schema_migration_history where migration_type = 'v'",
        )

        if current_state.empty:
            return incoming_state

        current_state = current_state.astype({"version": "int64"})
        incoming_state = incoming_state.astype({"version": "int64"})

        state_comparison = pd.merge(current_state, incoming_state, on="version", how="outer")

        # проверка есть ли исторические миграции, для которых не было загруженных с той же version
        # c_md5 - хэш исторической миграции, md5 - хэш загруженной миграции
        missing_migrations = state_comparison[state_comparison.c_md5.notnull() & state_comparison.md5.isnull()]

        if len(missing_migrations) > 0:
            logger.info(self.get_tabulated_print_for_df(
                df=missing_migrations,
                message="These loaded migrations are missing (md5 - loaded, c_md5 - historical)",
                fields=("version", "migration_type", "script", "md5", "c_md5"),
            ))

            raise AssertionError(
                f"Database {self.settings.target_db_name}. "
                "Migrations have gone missing, your code base should not truncate migrations, "
                "use migrations to correct older migrations"
            )

        # проверка есть ли исторические миграции, для которых есть загруженные, но хэши c_md5 != md5
        different_migrations = state_comparison[
            state_comparison.c_md5.notnull()
            & state_comparison.md5.notnull()
            & ~(state_comparison.md5 == state_comparison.c_md5)
            ]

        if len(different_migrations) > 0:
            logger.info(self.get_tabulated_print_for_df(
                df=different_migrations,
                message="These migrations differ by hash (md5 - loaded, c_md5 - historical)",
                fields=("version", "migration_type", "script", "md5", "c_md5"),
            ))

            raise AssertionError(
                f"Database {self.settings.target_db_name}. "
                "Do not edit migrations once run, use migrations to correct older migrations"
            )

        # возвращаются миграции, у которых не было соответствия по md5 хэшу среди исторических
        return state_comparison[state_comparison.c_md5.isnull()][
            ["version", "migration_type", "script", "md5", "full_path"]
        ]

    def get_repeatable_migrations_to_apply(self, client: Client, incoming_state: pd.DataFrame) -> pd.DataFrame:
        logger.info(
            f"Database {self.settings.target_db_name}. Trying to find out what repeatable migrations should be applied "
            f"on host {list(client.connection.hosts)}"
        )

        # incoming_state - загруженные миграции ("r" - повторяющиеся)
        # current_state - исторические миграции (из db.schema_migration_history)
        incoming_state = incoming_state[incoming_state.migration_type == "r"]
        current_state = self.execute_and_inflate(
            client,
            """SELECT max(hst.version) AS c_version, basename(script) as script, argMax(md5, hst.version) as c_md5 
            from schema_migration_history as hst where migration_type = 'r'
            GROUP BY script""",
        )

        if current_state.empty:
            return incoming_state

        state_comparison = pd.merge(current_state, incoming_state, on="script", how="outer")

        # проверка есть ли исторические миграции, для которых не было загруженных с тем же script (именем миграции)
        # c_md5 - хэш исторической миграции, md5 - хэш загруженной миграции
        missing_migrations = state_comparison[state_comparison.c_md5.notnull() & state_comparison.md5.isnull()]

        if len(missing_migrations) > 0:
            logger.info(self.get_tabulated_print_for_df(
                df=missing_migrations,
                message="These migrations differ by hash (md5 - loaded, c_md5 - historical)",
                fields=("version", "migration_type", "script", "md5", "c_md5"),
            ))

            raise AssertionError(
                f"Database {self.settings.target_db_name}. "
                "Migrations have gone missing, your code base should not truncate migrations, "
                "use migrations to correct older migrations."
            )

        # возвращаются миграции, у которых не было соответствия по md5 хэшу среди исторических или хэш отличался
        # то есть будут накатываться только новые или измененные повторяющиеся миграции
        return state_comparison[(state_comparison.c_md5.isnull()) | (state_comparison.c_md5 != state_comparison.md5)][
            ["version", "migration_type", "script", "md5", "full_path"]]

    def insert_into_schema_migration_history(self, client: Client, migration: Series) -> None:
        insert_row = {
            "version": migration["version"],
            "migration_type": migration["migration_type"],
            "script": migration["script"],
            "md5": migration["md5"],
        }

        logger.info(
            f"Database {self.settings.target_db_name}. "
            f"Inserting history record ({insert_row}) on host {list(client.connection.hosts)}"
        )
        client.execute(
            f"INSERT INTO schema_migration_history(version, migration_type, script, md5) VALUES",
            [insert_row],
        )

    def apply_migration_on_host(self, client: Client, migration: Series, db_name: str, queue_exec=True) -> None:
        with open(migration["full_path"]) as f:
            migration_scripts = (
                json.load(f) if migration["full_path"].endswith(".json") else f.read().split('-- $new_statement')
            )

            for migration_script in migration_scripts:
                try:
                    self.pipelined(
                        client, migration_script, db_name
                    ) if queue_exec else client.execute(migration_script)
                except (ServerException, TimeoutError) as exc:
                    raise ValueError(
                        f"Database {self.settings.target_db_name}. "
                        f"Failed to apply migration: {migration_script}"
                    ) from exc

            self.insert_into_schema_migration_history(client, migration)

    def pipelined(self, client: Client, migration_script: str, db_name: str, timeout: int = 60 * 60) -> None:
        ct = datetime.datetime.now()
        current_time = ct.strftime("%Y-%m-%d %H:%M:%S")
        
        # Extract placeholders in format {{ PLACEHOLDER }} from the query
        # Example: {{ POSTGRES_HOST }} -> parameter name: POSTGRES_HOST
        jinja_like_pattern = r"\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}"

        # Replace each {{ NAME }} with %(NAME)s and collect parameter names
        found_param_names = []

        def _replace_with_ch_param(match):
            name = match.group(1)
            found_param_names.append(name)
            return f"%({name})s"

        ch_param_script = re.sub(jinja_like_pattern, _replace_with_ch_param, migration_script)

        # Build parameters dictionary from environment variables
        parameters: Dict[str, str] = {}
        for name in set(found_param_names):
            env_value = os.getenv(name)
            if env_value is not None:
                parameters[name] = env_value
            else:
                logger.warning(
                    f"Environment variable '{name}' not found for placeholder '{{{{ {name} }}}}'."
                )

        # Execute with parameters (works even if parameters is empty)
        client.execute(ch_param_script, parameters)

        while True:
            loop_time = datetime.datetime.now()
            if (loop_time - ct).total_seconds() >= timeout:
                raise ValueError(
                    f"Database {self.settings.target_db_name}. "
                    f"Transaction Timeout - Unable to complete in {timeout} seconds, migration -> {migration_script}",
                )

            mutations_to_inspect = self.execute_and_inflate(
                client,
                f"SELECT database, table, mutation_id, lower(command) as command "
                f"FROM system.mutations WHERE database='{db_name}' and create_time >= '{current_time}' and is_done=0",
            )

            if mutations_to_inspect.empty:
                break

            mutations_to_inspect["match"] = mutations_to_inspect.apply(
                lambda row: row["command"] in migration_script, axis=1
            )
            # Correct boolean filtering: use the boolean Series directly
            mutations_to_inspect = mutations_to_inspect[mutations_to_inspect["match"]]

            if mutations_to_inspect.empty:
                break

            time.sleep(5)

    def check_for_database(self, client: Client) -> None:
        result = client.execute(
            f"SELECT name, uuid FROM system.databases WHERE name = '{self.settings.target_db_name}'"
        )

        hostname = list(client.connection.hosts)
        if len(result) == 0:
            self.create_database(client)
        else:
            logger.info(
                f"Database {self.settings.target_db_name} already exists on host {hostname}. "
                f"{result}"
            )

    def drop_database_if_exists(self, client: Client):
        client.execute(f"DROP DATABASE IF EXISTS {self.settings.target_db_name}")
        logger.info(
            f"Database {self.settings.target_db_name} was dropped or "
            f"did not exist on host {list(client.connection.hosts)}"
        )

    def create_database(self, client: Client) -> None:
        hostname = list(client.connection.hosts)
        try:
            client.execute(f"CREATE DATABASE IF NOT EXISTS {self.settings.target_db_name}")
        except ServerException as exc:
            logger.error(
                f"Failed to create database {self.settings.target_db_name} on host {hostname}: "
                f"{exc}"
            )
        else:
            logger.info(f"Database {self.settings.target_db_name} successfully created on host {hostname}")

    def get_int_migration_ver(self, file_name: str, migration_type: str) -> int:
        if migration_type == "v":
            string_ver = file_name.split("_")[0].replace("v", "")
            ver_list = string_ver.split(".")

            if len(ver_list) != 3:
                raise ValueError(f'Unsupported version string {string_ver}')

            minor = int(ver_list[2])
            middle = int(ver_list[1])
            major = int(ver_list[0])
            int_ver = major * 10000000 + middle * 1000 + minor

            return int_ver
        elif migration_type == "r":
            int_ver = int(datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
            return int_ver
        else:
            raise NotImplementedError(
                f"Database {self.settings.target_db_name}. Migration type {migration_type} is not implemented"
            )

    def parse_migration_file(self, migration_file: os.DirEntry) -> dict:
        migration_type = migration_file.name[0]
        version = self.get_int_migration_ver(migration_file.name, migration_type)

        migration = {
            "version": version,
            "script": migration_file.name,
            "full_path": migration_file.path,
            "md5": hashlib.md5(
                pathlib.Path(f"{migration_file.path}").read_bytes()
            ).hexdigest(),
            "migration_type": migration_type,
        }

        return migration

    def load_and_parse_migrations(self, migrations_home: str) -> List[Dict]:
        logger.info(f"Looking for migrations in dir: {migrations_home}")
        migrations: List[Dict] = []

        for f in os.scandir(migrations_home):
            if f.name.endswith(".sql") or f.name.endswith(".json"):
                migration = self.parse_migration_file(f)
            else:
                logger.info(f"File {f.name} has unknown type and wont be processed.")
                continue

            migrations.append(migration)

        if len(migrations) == 0:
            raise AssertionError(
                f"Database {self.settings.target_db_name}. "
                f"No migrations found in directory {migrations_home}"
            )

        return migrations

    def get_migrations_to_apply(self, client: Client, migrations: pd.DataFrame) -> pd.DataFrame:
        df_incr = self.get_incremental_migrations_to_apply(client, migrations)
        df_repeatable = self.get_repeatable_migrations_to_apply(client, migrations)

        return pd.concat([df_incr, df_repeatable]).sort_values("version")

    def apply_migrations_for_database(self, migrations: List[Dict]) -> None:
        hosts_list = self.settings.ch_host_list
        db_name = self.settings.target_db_name

        if not hosts_list:
            logger.info(f"Hosts not defined, can't apply migrations to database {db_name}")
            return None

        migrations_df = pd.DataFrame(migrations).sort_values("version")
        logger.info(self.get_tabulated_print_for_df(
            df=migrations_df,
            message="Loaded and parsed migrations",
            fields=("version", "migration_type", "script", "md5"),
        ))

        host_0_client = self.get_ch_client(db_host=hosts_list[0], db_name=db_name)
        migrations_df = self.get_migrations_to_apply(client=host_0_client, migrations=migrations_df)
        host_0_client.disconnect()

        if migrations_df.empty:
            logger.info(f'No migrations to apply to database {db_name} on hosts {hosts_list}')
            return None

        logger.info(self.get_tabulated_print_for_df(
            df=migrations_df,
            message=f"These migrations will be applied on hosts {hosts_list}",
            fields=("version", "migration_type", "script", "md5"),
        ))

        for _, migration in migrations_df.iterrows():
            for client in self.iter_get_ch_client(hosts_list=hosts_list, db_name=db_name):
                self.apply_migration_on_host(client=client, migration=migration, db_name=db_name)
                client.disconnect()

        logger.info(
            f"Migrations were applied to database {db_name} on hosts {hosts_list}"
        )

    def migrate(self) -> None:
        logger.info('Migration started')

        self.execute_on_cluster('system', self.check_for_database)
        self.execute_on_cluster(self.settings.target_db_name, self.create_schema_migration_history_table)

        migrations = self.load_and_parse_migrations(migrations_home=self.settings.migrations_home)
        self.apply_migrations_for_database(migrations=migrations)

        logger.info('Migration finished successfully!')

    def clean(self) -> None:
        self.execute_on_cluster('system', self.drop_database_if_exists)

    def create_db(self) -> None:
        self.execute_on_cluster('system', self.create_database)
