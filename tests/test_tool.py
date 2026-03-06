from pathlib import Path

import pandas as pd
import pytest

from clickhouse_migrator.tool import MigrationTool, Settings


def test_settings_reads_env(tmp_path, monkeypatch):
    config = tmp_path / "env"
    config.write_text(
        "ENV=DEV\n"
        "MIGRATION_HOME=/tmp/migrations\n"
        "CH_HOST_LIST=[\"host\"]\n"
        "CH_PORT=9000\n"
        "CH_USER=user\n"
        "CH_PASSWORD=pass\n"
        "MIGRATION_TARGET_DB_NAME=db\n"
        "MIGRATION_QUEUE_EXEC=True\n"
    )

    monkeypatch.setenv("ENV", "DEV")

    settings = Settings(str(config))

    assert settings.env == "DEV"
    assert settings.migrations_home == "/tmp/migrations"
    assert settings.ch_host_list == ["host"]
    assert settings.ch_port == "9000"
    assert settings.ch_user == "user"
    assert settings.ch_password == "pass"
    assert settings.target_db_name == "db"
    assert settings.queue_exec == "True"


def test_execute_and_inflate_returns_dataframe(dummy_client):
    query = "SELECT 1"
    dummy_client.query_results = {
        query: [
            [(1,)],
            [("value", "String")],
        ]
    }

    df = MigrationTool.execute_and_inflate(dummy_client, query)

    assert df.iloc[0]["value"] == 1


def test_get_int_migration_ver_incremental(dummy_settings):
    tool = MigrationTool(dummy_settings)

    version = tool.get_int_migration_ver("v1.2.3_init.sql", "v")

    assert version == 1 * 10000000 + 2 * 1000 + 3


def test_get_int_migration_ver_repeatable(dummy_settings):
    tool = MigrationTool(dummy_settings)

    version = tool.get_int_migration_ver("r_init.sql", "r")

    assert isinstance(version, int)


def test_parse_migration_file(dummy_settings, tmp_path):
    tool = MigrationTool(dummy_settings)
    fixture_path = tmp_path / "v1.2.3_init.sql"
    fixture_path.write_text("SELECT 1;\n")

    migration = tool.parse_migration_file(next(iter(__import__("os").scandir(tmp_path))))

    assert migration["script"] == "v1.2.3_init.sql"
    assert migration["full_path"] == str(fixture_path)
    assert migration["migration_type"] == "v"


def test_get_incremental_migrations_to_apply_handles_missing_history(dummy_settings, dummy_client):
    tool = MigrationTool(dummy_settings)
    dummy_client.query_results = {
        "SELECT version AS version, script AS c_script, md5 as c_md5 from schema_migration_history where migration_type = 'v'": [
            [(1, "script", "hash")],
            [("version", "UInt64"), ("c_script", "String"), ("c_md5", "String")],
        ]
    }
    incoming_state = pd.DataFrame(
        [
            {
                "version": 1,
                "migration_type": "v",
                "script": "v1.0.0_init.sql",
                "md5": "different",
                "full_path": "/tmp/v1.0.0_init.sql",
            }
        ]
    )

    with pytest.raises(AssertionError, match="Do not edit migrations once run"):
        tool.get_incremental_migrations_to_apply(dummy_client, incoming_state)


def test_get_repeatable_migrations_to_apply_missing(dummy_settings, dummy_client):
    tool = MigrationTool(dummy_settings)
    tool.execute_and_inflate = lambda client, query: pd.DataFrame(
        [{"c_version": 1, "script": "r_init.sql", "c_md5": "hash"}]
    )
    incoming_state = pd.DataFrame(
        [
            {
                "version": 2,
                "migration_type": "r",
                "script": "r_other.sql",
                "md5": "hash",
                "full_path": "/tmp/r_other.sql",
            }
        ]
    )

    with pytest.raises(AssertionError, match="Migrations have gone missing"):
        tool.get_repeatable_migrations_to_apply(dummy_client, incoming_state)


def test_load_and_parse_migrations(dummy_settings, tmp_path):
    tool = MigrationTool(dummy_settings)
    fixtures_dir = tmp_path / "fixtures"
    fixtures_dir.mkdir()
    source_fixtures_dir = Path(__file__).parent / "fixtures"
    (fixtures_dir / "v1.2.3_init.sql").write_text((source_fixtures_dir / "v1.2.3_init.sql").read_text())
    (fixtures_dir / "r_init.sql").write_text((source_fixtures_dir / "r_init.sql").read_text())

    migrations = tool.load_and_parse_migrations(str(fixtures_dir))

    assert len(migrations) == 2


def test_load_and_parse_migrations_empty(dummy_settings, tmp_path):
    tool = MigrationTool(dummy_settings)

    with pytest.raises(AssertionError, match="No migrations found"):
        tool.load_and_parse_migrations(str(tmp_path))
