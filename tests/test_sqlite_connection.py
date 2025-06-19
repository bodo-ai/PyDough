"""
Tests support for the DatabaseConnection class using a SQLite database backend.
"""

import re
import sys

import pandas as pd
import pytest

from pydough.database_connectors import (
    DatabaseConnection,
    DatabaseContext,
    DatabaseDialect,
    load_database_context,
)


def test_query_execution(sqlite_people_jobs: DatabaseConnection) -> None:
    """
    Test that the DatabaseConnection can execute a query on the SQLite database.

    Args:
        sqlite_people_jobs (DatabaseConnection): The DatabaseConnection object to test.
    """
    query: str = """
        SELECT PEOPLE.person_id, COUNT(*) as num_entries FROM PEOPLE
        JOIN JOBS
            ON PEOPLE.person_id = JOBS.person_id
        GROUP BY PEOPLE.person_id
    """
    result: pd.DataFrame = sqlite_people_jobs.execute_query_df(query)
    columns = ["person_id", "num_entries"]
    data = [(i, 2) for i in range(10)]
    expected = pd.DataFrame(data, columns=columns)
    pd.testing.assert_frame_equal(result, expected)


@pytest.mark.parametrize(
    "database_name",
    [
        pytest.param("sqlite", id="lowercase"),
        pytest.param("SQLITE", id="uppercase"),
    ],
)
def test_sqlite_context(database_name: str) -> None:
    """
    Test that we can execute SQL against load_database_context.
    """
    context: DatabaseContext = load_database_context(database_name, database=":memory:")
    result: pd.DataFrame = context.connection.execute_query_df("Select 1 as A")
    pd.testing.assert_frame_equal(result, pd.DataFrame({"A": [1]}))
    assert context.dialect == DatabaseDialect.SQLITE


def test_sqlite_context_no_path() -> None:
    """
    Test that we error if a Database path is not provided.
    """
    with pytest.raises(ValueError, match="SQLite connection requires a database path."):
        load_database_context("sqlite")


def test_sqlite_context_wrong_name() -> None:
    """
    Test that we error if the database name is incorrect.
    """
    with pytest.raises(ValueError, match="Unsupported database: sqlite3"):
        load_database_context("sqlite3", database=":memory:")


def test_sqlite_context_invalid_arg() -> None:
    """
    Test that load_database_context errors if useless
    argument is provided.
    """
    error1: str = "invalid_kwarg is an invalid keyword argument"
    error2: str = re.escape(
        "'invalid_kwarg' is an invalid keyword argument for Connection()"
    )

    # Error message changes in Python 3.10
    error_message: str = error2 if sys.version_info >= (3, 10) else error1

    with pytest.raises(
        TypeError,
        match=error_message,
    ):
        load_database_context("sqlite", database=":memory:", invalid_kwarg="foo")


def test_unsupported_database() -> None:
    """
    Test that we error if an unsupported database is provided.

    TODO: Remove when we support mysql or move to a more generic file.
    """
    with pytest.raises(ValueError):
        load_database_context("mysql", database=":memory:")
