import pytest


class DummyClient:
    def __init__(self, query_results=None):
        self.query_results = query_results or {}
        self.executed = []
        self.connection = type("Conn", (), {"hosts": ["host"], "database": "db"})()

    def execute(self, query, *args, **kwargs):
        self.executed.append((query, args, kwargs))
        return self.query_results.get(query, [])

    def disconnect(self):
        return None


class DummySettings:
    def __init__(self):
        self.target_db_name = "db"
        self.ch_user = "user"
        self.ch_password = "pass"
        self.ch_port = 9000
        self.ch_host_list = ["host"]
        self.migrations_home = "migrations"
        self.env = "test"
        self.queue_exec = "True"


@pytest.fixture
def dummy_settings():
    return DummySettings()


@pytest.fixture
def dummy_client():
    return DummyClient()
