"""Load stage: create the tables and insert the cleaned DataFrames.

Key behaviour
-------------
* The tables are created from `database/schema.sql` - the same file that is
  documented in the README and can be pasted into pgAdmin. There is therefore
  only ONE definition of the schema.
* Inserts use PostgreSQL `INSERT ... ON CONFLICT (primary key) DO UPDATE`, so
  running the pipeline twice does not create duplicate rows.
* Table objects are *reflected* from the database after creation, which means
  the loader always uses the real columns and keys of the database.
* Any connection problem raises DatabaseConnectionError with a readable
  message; the pipeline then stops and reports failure (it never pretends the
  data was loaded).
"""

from __future__ import annotations

import logging
import math
import os
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd
from sqlalchemy import MetaData
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from database.connection import DatabaseConnectionError

logger = logging.getLogger(__name__)

SCHEMA_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "schema.sql")

# Load order (parents before children) and the conflict key of each table.
TABLE_ORDER: List[Tuple[str, List[str]]] = [
    ("repositories", ["repo_id"]),
    ("contributors", ["contributor_id"]),
    ("repository_contributors", ["repo_id", "contributor_id"]),
    ("commits", ["commit_sha"]),
    ("issues", ["issue_id"]),
    ("pull_requests", ["pr_id"]),
]

DEFAULT_CHUNK_SIZE = 500


# ---------------------------------------------------------------------------
# Schema handling
# ---------------------------------------------------------------------------
def read_schema_statements(schema_path: str = SCHEMA_PATH) -> List[str]:
    """Read schema.sql and return the individual SQL statements.

    Comment-only lines are removed and the text is split on ';'. This is enough
    for our schema because it contains no functions, triggers or string
    literals with semicolons.
    """
    if not os.path.exists(schema_path):
        raise FileNotFoundError(f"Schema file not found: {schema_path}")

    with open(schema_path, encoding="utf-8") as handle:
        lines = [
            line
            for line in handle.read().splitlines()
            if line.strip() and not line.strip().startswith("--")
        ]

    statements = [statement.strip() for statement in "\n".join(lines).split(";")]
    return [statement for statement in statements if statement]


def create_tables(engine: Engine, schema_path: str = SCHEMA_PATH) -> int:
    """Execute schema.sql. Every statement is `IF NOT EXISTS`, so it is safe to
    run the pipeline many times.

    Returns the number of statements executed.
    """
    statements = read_schema_statements(schema_path)
    try:
        with engine.begin() as connection:
            for statement in statements:
                connection.exec_driver_sql(statement)
    except SQLAlchemyError as exc:
        raise DatabaseConnectionError(
            f"Failed while creating the database tables: {exc}"
        ) from exc

    logger.info("Database schema applied (%s statements)", len(statements))
    return len(statements)


def reflect_tables(engine: Engine) -> MetaData:
    """Reflect the project tables from the live database."""
    metadata = MetaData()
    try:
        metadata.reflect(bind=engine, only=[name for name, _ in TABLE_ORDER])
    except SQLAlchemyError as exc:
        raise DatabaseConnectionError(f"Could not read the database tables: {exc}") from exc
    return metadata


def validate_dataframes_against_schema(
    tables: Dict[str, pd.DataFrame], metadata: MetaData
) -> List[str]:
    """Compare DataFrame columns with the database columns.

    Returns a list of problems (empty list = everything matches).
    """
    problems: List[str] = []
    for table_name, dataframe in tables.items():
        if table_name not in metadata.tables:
            problems.append(f"Table '{table_name}' does not exist in the database schema.")
            continue
        if dataframe is None or dataframe.empty:
            continue
        table_columns = set(metadata.tables[table_name].columns.keys())
        for column in dataframe.columns:
            if column not in table_columns:
                problems.append(
                    f"Column '{column}' of the '{table_name}' DataFrame does not exist "
                    f"in table '{table_name}'."
                )
    return problems


# ---------------------------------------------------------------------------
# Value conversion helpers
# ---------------------------------------------------------------------------
def dataframe_to_records(dataframe: pd.DataFrame) -> List[Dict[str, object]]:
    """Convert a DataFrame into a list of plain Python dictionaries.

    pandas uses NaN, NaT, pd.NA and numpy scalar types; psycopg2 needs plain
    Python values (None, int, float, bool, datetime, str).
    """
    records: List[Dict[str, object]] = []
    for row in dataframe.itertuples(index=False, name=None):
        values: List[object] = []
        for value in row:
            if value is None or value is pd.NaT or value is pd.NA:
                values.append(None)
            elif isinstance(value, float) and math.isnan(value):
                values.append(None)
            elif isinstance(value, (np.bool_, bool)):
                values.append(bool(value))
            elif isinstance(value, np.integer):
                values.append(int(value))
            elif isinstance(value, np.floating):
                values.append(float(value))
            elif isinstance(value, pd.Timestamp):
                values.append(value.to_pydatetime())
            else:
                values.append(value)
        records.append(dict(zip(dataframe.columns, values)))
    return records


def _chunks(records: List[Dict[str, object]], size: int) -> Iterable[List[Dict[str, object]]]:
    for start in range(0, len(records), size):
        yield records[start : start + size]


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------
def load_table(
    engine: Engine,
    dataframe: pd.DataFrame,
    table_name: str,
    conflict_columns: List[str],
    metadata: MetaData,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> int:
    """Upsert one DataFrame into one table. Returns the number of rows written."""
    if dataframe is None or dataframe.empty:
        logger.info("Skipping empty table: %s", table_name)
        return 0

    table = metadata.tables[table_name]
    unknown_columns = [column for column in dataframe.columns if column not in table.columns]
    if unknown_columns:
        raise ValueError(
            f"Columns {unknown_columns} of DataFrame '{table_name}' do not exist in the "
            f"database table '{table_name}'. Check schema.sql and transformation/cleaning.py."
        )

    records = dataframe_to_records(dataframe)
    insert_statement = pg_insert(table)
    update_columns = {
        column: insert_statement.excluded[column]
        for column in dataframe.columns
        if column not in conflict_columns
    }
    statement = insert_statement.on_conflict_do_update(
        index_elements=conflict_columns, set_=update_columns
    )

    written = 0
    try:
        with engine.begin() as connection:
            for chunk in _chunks(records, chunk_size):
                result = connection.execute(statement, chunk)
                written += result.rowcount if result.rowcount and result.rowcount > 0 else 0
    except SQLAlchemyError as exc:
        raise DatabaseConnectionError(
            f"Failed to load data into table '{table_name}': {exc}"
        ) from exc

    logger.info("Loaded %s row(s) into %s", written, table_name)
    return written


def load_all(
    engine: Engine,
    tables: Dict[str, pd.DataFrame],
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    metadata: Optional[MetaData] = None,
) -> Dict[str, int]:
    """Load every processed DataFrame into PostgreSQL, in foreign-key order."""
    metadata = metadata or reflect_tables(engine)

    problems = validate_dataframes_against_schema(tables, metadata)
    if problems:
        raise DatabaseConnectionError(
            "DataFrame and database schema do not match:\n  - " + "\n  - ".join(problems)
        )

    loaded: Dict[str, int] = {}
    for table_name, conflict_columns in TABLE_ORDER:
        dataframe = tables.get(table_name)
        loaded[table_name] = load_table(
            engine,
            dataframe,
            table_name,
            conflict_columns,
            metadata,
            chunk_size=chunk_size,
        )
    return loaded


def table_row_counts(engine: Engine) -> Dict[str, int]:
    """Return the number of rows currently stored in each table."""
    from sqlalchemy import text

    counts: Dict[str, int] = {}
    with engine.connect() as connection:
        for table_name, _ in TABLE_ORDER:
            counts[table_name] = int(
                connection.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar() or 0
            )
    return counts
