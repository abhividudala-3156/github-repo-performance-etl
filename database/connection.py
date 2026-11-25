"""PostgreSQL connection configuration.

>>> THIS IS THE ONLY FILE YOU NEED TO LOOK AT FOR DATABASE SETTINGS. <<<

The actual values are NOT written in this file. They are read from the local
`.env` file in the project root (created by copying `.env.example`):

    DB_HOST=localhost
    DB_PORT=5432
    DB_NAME=github_analytics
    DB_USER=postgres
    DB_PASSWORD=your_real_password

Why a .env file? Because `.env` is listed in `.gitignore`, so your password is
never pushed to GitHub, while `.env.example` (placeholders only) is pushed so
that anybody can see which settings are required.

If you prefer to hard-code the values, change the defaults in
`get_db_settings()` below - but never commit a real password to Git.
"""

from __future__ import annotations

import os
import sys
from typing import Dict, Optional
from urllib.parse import quote_plus

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError, SQLAlchemyError

# Project root = the folder that contains main.py and .env
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_FILE = os.path.join(PROJECT_ROOT, ".env")

# Load .env once when this module is imported. Values already present in the
# environment are kept (override=False).
load_dotenv(ENV_FILE, override=False)

# Defaults used when a key is missing from .env
DEFAULT_SETTINGS: Dict[str, str] = {
    "DB_HOST": "localhost",
    "DB_PORT": "5432",
    "DB_NAME": "GIT",
    "DB_USER": "postgres",
    "DB_PASSWORD": "Abhi@1405",
}


class DatabaseConnectionError(Exception):
    """Raised when the PostgreSQL database cannot be reached or is misconfigured."""


def get_db_settings() -> Dict[str, str]:
    """Return the current connection settings (environment first, then defaults)."""
    load_dotenv(ENV_FILE, override=False)
    return {key: os.getenv(key, default) for key, default in DEFAULT_SETTINGS.items()}


def build_database_url(settings: Optional[Dict[str, str]] = None) -> str:
    """Build the SQLAlchemy connection URL for PostgreSQL.

    The password is URL-encoded, so special characters like @ or # are safe.
    """
    settings = settings or get_db_settings()
    password = quote_plus(str(settings.get("DB_PASSWORD", "")))
    user = quote_plus(str(settings.get("DB_USER", "")))
    host = settings.get("DB_HOST", "localhost")
    port = settings.get("DB_PORT", "5432")
    name = settings.get("DB_NAME", "github_analytics")

    # Optional: on Linux/macOS a PostgreSQL server can be reached through a Unix
    # socket. In that case DB_HOST holds the socket directory (e.g. /tmp).
    # On Windows DB_HOST is always a normal host name like 'localhost'.
    if str(host).startswith("/"):
        return f"postgresql+psycopg2://{user}:{password}@/{name}?host={host}"

    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{name}"


def get_engine(settings: Optional[Dict[str, str]] = None, echo: bool = False) -> Engine:
    """Create a SQLAlchemy engine for the local PostgreSQL server."""
    return create_engine(
        build_database_url(settings),
        echo=echo,
        pool_pre_ping=True,   # checks the connection before using it
        future=True,
    )


def describe_settings(settings: Optional[Dict[str, str]] = None) -> str:
    """Return a log-safe description of the settings (password hidden)."""
    settings = settings or get_db_settings()
    password = settings.get("DB_PASSWORD", "")
    hidden = "********" if password and password != "CHANGE_ME" else "(not set)"
    return (
        f"host={settings.get('DB_HOST')} port={settings.get('DB_PORT')} "
        f"database={settings.get('DB_NAME')} user={settings.get('DB_USER')} "
        f"password={hidden}"
    )


def check_connection(engine: Engine) -> str:
    """Run `SELECT version()` to prove the database is reachable.

    Returns the PostgreSQL version string.
    Raises:
        DatabaseConnectionError: with a student-friendly explanation.
    """
    try:
        with engine.connect() as connection:
            version = connection.execute(text("SELECT version()")).scalar()
        return str(version)
    except OperationalError as exc:
        raise DatabaseConnectionError(
            "Could not connect to PostgreSQL.\n"
            f"  Settings used: {describe_settings()}\n"
            "  Please check:\n"
            "   1. PostgreSQL service is running (pgAdmin can connect to the server).\n"
            "   2. The database 'github_analytics' exists "
            "(pgAdmin -> right click Databases -> Create -> Database).\n"
            "   3. DB_USER / DB_PASSWORD in your .env file are correct.\n"
            f"  Database said: {str(exc.orig).strip() if getattr(exc, 'orig', None) else exc}"
        ) from exc
    except SQLAlchemyError as exc:
        raise DatabaseConnectionError(f"Unexpected database error: {exc}") from exc


def test_connection() -> bool:
    """Small helper you can run directly: `python -m database.connection`."""
    engine = get_engine()
    print(f"Connecting with: {describe_settings()}")
    try:
        version = check_connection(engine)
    except DatabaseConnectionError as error:
        print(f"[FAILED] {error}")
        return False
    print(f"[OK] Connected to PostgreSQL: {version.split(',')[0]}")
    return True


if __name__ == "__main__":
    # exit code 1 makes the check usable in scripts: python -m database.connection
    sys.exit(0 if test_connection() else 1)
