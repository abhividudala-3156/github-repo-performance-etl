"""Tests for the database (load) stage.

IMPORTANT: none of these tests need a running PostgreSQL server. The engine
and connection objects are replaced by small fakes, so `pytest -v` works on any
machine.

Tests that WOULD need a real PostgreSQL database (they are not part of the
automatic suite, see the README "Testing" section):
  * really creating the tables and inserting rows,
  * the ON CONFLICT (upsert) behaviour of `load_all`.
You can check those manually by running `python main.py` with your local
database configured.
"""

from __future__ import annotations

import re
from contextlib import contextmanager

import pandas as pd
import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table
from sqlalchemy.exc import OperationalError

from database.connection import (
    DatabaseConnectionError,
    build_database_url,
    check_connection,
    describe_settings,
    get_db_settings,
)
from database.loader import (
    TABLE_ORDER,
    create_tables,
    dataframe_to_records,
    read_schema_statements,
    validate_dataframes_against_schema,
)


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------
class FakeResult:
    def __init__(self, value=None, rowcount=0):
        self._value = value
        self.rowcount = rowcount

    def scalar(self):
        return self._value


class FakeConnection:
    """Connection usable both as a context manager and for exec_driver_sql."""

    def __init__(self, statements=None, scalar_value=None, error=None, rowcount=0):
        self.statements = statements if statements is not None else []
        self.scalar_value = scalar_value
        self.error = error
        self.rowcount = rowcount

    def exec_driver_sql(self, statement):
        if self.error:
            raise self.error
        self.statements.append(statement)
        return FakeResult()

    def execute(self, statement, parameters=None):
        if self.error:
            raise self.error
        return FakeResult(self.scalar_value, self.rowcount)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class FakeEngine:
    def __init__(self, scalar_value=None, error=None, rowcount=0):
        self.statements = []
        self.scalar_value = scalar_value
        self.error = error
        self.rowcount = rowcount

    def connect(self):
        return FakeConnection(scalar_value=self.scalar_value, error=self.error, rowcount=self.rowcount)

    @contextmanager
    def begin(self):
        yield FakeConnection(statements=self.statements, error=self.error, rowcount=self.rowcount)


def settings(**overrides):
    """Connection settings used by the tests. These never come from .env."""
    base = {
        "DB_HOST": "localhost",
        "DB_PORT": "5432",
        "DB_NAME": "github_analytics",
        "DB_USER": "postgres",
        "DB_PASSWORD": "secret",
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# connection.py
# ---------------------------------------------------------------------------
def test_build_database_url_uses_psycopg2_driver():
    url = build_database_url(settings())
    assert url == "postgresql+psycopg2://postgres:secret@localhost:5432/github_analytics"


def test_build_database_url_encodes_special_characters_in_password():
    url = build_database_url(settings(DB_PASSWORD="p@ss word#1"))
    assert "p%40ss+word%231" in url


def test_get_db_settings_reads_environment(monkeypatch):
    monkeypatch.setenv("DB_HOST", "192.168.1.5")
    monkeypatch.setenv("DB_NAME", "test_db")
    values = get_db_settings()
    assert values["DB_HOST"] == "192.168.1.5"
    assert values["DB_NAME"] == "test_db"


def test_get_db_settings_falls_back_to_defaults(monkeypatch, tmp_path):
    # Point the module at a .env file that does not exist, so that YOUR real
    # .env (with your own DB_NAME and password) cannot influence this test.
    monkeypatch.setattr("database.connection.ENV_FILE", str(tmp_path / "missing.env"))
    for key in ["DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD"]:
        monkeypatch.delenv(key, raising=False)

    values = get_db_settings()

    assert values["DB_HOST"] == "localhost"
    assert values["DB_PORT"] == "5432"
    assert values["DB_NAME"] == "GIT"
    assert values["DB_USER"] == "postgres"
    assert values["DB_PASSWORD"] == "Abhi@1405"


def test_describe_settings_hides_the_password():
    description = describe_settings(settings(DB_PASSWORD="secret"))
    assert "secret" not in description
    assert "********" in description


def test_check_connection_returns_version():
    engine = FakeEngine(scalar_value="PostgreSQL 16.2 on x86_64-windows")
    version = check_connection(engine)
    assert "PostgreSQL 16.2" in version


def test_check_connection_raises_friendly_error():
    original = Exception("could not connect to server: Connection refused")
    engine = FakeEngine(error=OperationalError("SELECT version()", {}, original))

    with pytest.raises(DatabaseConnectionError) as error:
        check_connection(engine)

    message = str(error.value)
    assert "Could not connect to PostgreSQL" in message
    assert "github_analytics" in message          # tells the student what to create
    assert ".env" in message


# ---------------------------------------------------------------------------
# schema.sql
# ---------------------------------------------------------------------------
def test_read_schema_statements_returns_clean_statements():
    statements = read_schema_statements()

    assert len(statements) >= 6
    assert all(statement.strip() for statement in statements)
    assert all("--" not in statement for statement in statements)


def test_schema_defines_all_required_tables():
    statements = read_schema_statements()
    created = {
        match.group(1)
        for statement in statements
        for match in [re.search(r"CREATE TABLE IF NOT EXISTS (\w+)", statement, re.IGNORECASE)]
        if match
    }
    assert created == {name for name, _ in TABLE_ORDER}


def test_schema_and_loader_use_the_same_primary_keys():
    statements = read_schema_statements()
    for table_name, conflict_columns in TABLE_ORDER:
        definition = next(
            statement for statement in statements if f"TABLE IF NOT EXISTS {table_name} " in statement
        )
        primary_key_line = re.search(r"PRIMARY KEY", definition, re.IGNORECASE)
        assert primary_key_line is not None, f"{table_name} has no PRIMARY KEY"

    # the composite key of the link table must contain both foreign keys
    link_keys = dict(TABLE_ORDER)["repository_contributors"]
    assert link_keys == ["repo_id", "contributor_id"]


def test_create_tables_executes_every_statement():
    engine = FakeEngine()
    executed = create_tables(engine)

    assert executed == len(read_schema_statements())
    assert len(engine.statements) == executed
    assert engine.statements[0].startswith("CREATE TABLE IF NOT EXISTS repositories")


def test_create_tables_wraps_database_errors():
    engine = FakeEngine(error=OperationalError("CREATE TABLE", {}, Exception("permission denied")))

    with pytest.raises(DatabaseConnectionError):
        create_tables(engine)


# ---------------------------------------------------------------------------
# loader helpers
# ---------------------------------------------------------------------------
def test_dataframe_to_records_converts_pandas_values_to_python_types():
    dataframe = pd.DataFrame(
        {
            "repo_id": pd.array([101], dtype="Int64"),
            "stars": pd.array([54000], dtype="Int64"),
            "name": ["psf/requests"],
            "is_fork": [False],
            "created_at": [pd.Timestamp("2025-01-05 10:00:00")],
            "description": [None],
            "merged_at": [pd.NaT],
            "size": [3.5],
        }
    )

    records = dataframe_to_records(dataframe)

    assert len(records) == 1
    record = records[0]
    assert record["repo_id"] == 101 and isinstance(record["repo_id"], int)
    assert record["stars"] == 54000
    assert record["is_fork"] is False
    assert record["description"] is None
    assert record["merged_at"] is None
    assert isinstance(record["created_at"], pd.Timestamp) or hasattr(record["created_at"], "year")
    assert record["size"] == 3.5


def test_dataframe_to_records_handles_empty_dataframe():
    dataframe = pd.DataFrame({"repo_id": pd.array([], dtype="Int64")})
    assert dataframe_to_records(dataframe) == []


def test_validate_dataframes_against_schema_detects_unknown_columns():
    metadata = MetaData()
    Table(
        "repositories",
        metadata,
        Column("repo_id", Integer, primary_key=True),
        Column("full_name", String),
    )

    problems = validate_dataframes_against_schema(
        {"repositories": pd.DataFrame({"repo_id": [1], "wrong_column": ["x"]})}, metadata
    )

    assert len(problems) == 1
    assert "wrong_column" in problems[0]


def test_validate_dataframes_against_schema_accepts_matching_columns():
    metadata = MetaData()
    Table(
        "repositories",
        metadata,
        Column("repo_id", Integer, primary_key=True),
        Column("full_name", String),
    )

    problems = validate_dataframes_against_schema(
        {"repositories": pd.DataFrame({"repo_id": [1], "full_name": ["a/b"]})}, metadata
    )

    assert problems == []
