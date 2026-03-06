from click.testing import CliRunner

import clickhouse_migrator.cli as cli


def test_clean_command_invokes_tool(monkeypatch):
    calls = {"clean": False}

    class DummySettings:
        def __init__(self, config_file):
            self.env = "test"

    class DummyTool:
        def __init__(self, settings):
            self.settings = settings

        def clean(self):
            calls["clean"] = True

    monkeypatch.setattr(cli.clickhouse_migration_tool, "Settings", DummySettings)
    monkeypatch.setattr(cli.clickhouse_migration_tool, "MigrationTool", DummyTool)

    runner = CliRunner()
    result = runner.invoke(cli.migrator, ["clean", "--configfile", "env/.env"])

    assert result.exit_code == 0
    assert calls["clean"] is True


def test_migrate_command_invokes_tool(monkeypatch):
    calls = {"migrate": False}

    class DummySettings:
        def __init__(self, config_file):
            self.env = "test"

    class DummyTool:
        def __init__(self, settings):
            self.settings = settings

        def migrate(self):
            calls["migrate"] = True

    monkeypatch.setattr(cli.clickhouse_migration_tool, "Settings", DummySettings)
    monkeypatch.setattr(cli.clickhouse_migration_tool, "MigrationTool", DummyTool)

    runner = CliRunner()
    result = runner.invoke(cli.migrator, ["migrate", "--configfile", "env/.env"])

    assert result.exit_code == 0
    assert calls["migrate"] is True


def test_create_db_command_invokes_tool(monkeypatch):
    calls = {"create_db": False}

    class DummySettings:
        def __init__(self, config_file):
            self.env = "test"

    class DummyTool:
        def __init__(self, settings):
            self.settings = settings

        def create_db(self):
            calls["create_db"] = True

    monkeypatch.setattr(cli.clickhouse_migration_tool, "Settings", DummySettings)
    monkeypatch.setattr(cli.clickhouse_migration_tool, "MigrationTool", DummyTool)

    runner = CliRunner()
    result = runner.invoke(cli.migrator, ["create-db", "--configfile", "env/.env"])

    assert result.exit_code == 0
    assert calls["create_db"] is True
