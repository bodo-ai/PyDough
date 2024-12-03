"""
Tests support for the DatabaseConnection class
using a SQLite database backend.
"""

import sqlite3

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


@pytest.mark.skip("__del__ is disabled while we decide on the right behavior.")
def test_unusable_after_del() -> None:
    """
    Test that the underlying connection is closed when the DatabaseConnection
    object is deleted. This ensures the semantics of the DatabaseConnection
    are to assume ownership of the connection.

    Args:
        sqlite_database (sqlite3.Connection): The connection for building the
        database connection..
    """
    # Create a standalone connection to the database so we can inspect the garbage count.
    db: DatabaseConnection = DatabaseConnection(sqlite3.connect(":memory:"))
    connection = db._connection
    del db
    with pytest.raises(sqlite3.ProgrammingError):
        connection.execute("SELECT 1")


def test_sqlite_context() -> None:
    context: DatabaseContext = load_database_context("sqlite", database=":memory:")
    result: pd.DataFrame = context.connection.execute_query_df("Select 1 as A")
    pd.testing.assert_frame_equal(result, pd.DataFrame({"A": [1]}))
    assert context.dialect == DatabaseDialect.SQLITE
    with pytest.raises(ValueError, match="SQLite connection requires a database path."):
        context = load_database_context("sqlite")
    with pytest.raises(ValueError):
        context = load_database_context("sqlite3")
    with pytest.raises(
        TypeError,
        match="'invalid_kwarg' is an invalid keyword argument",
    ):
        context = load_database_context(
            "sqlite", database=":memory:", invalid_kwarg="foo"
        )
    with pytest.raises(ValueError):
        # TODO: Potentially remove/move in the future.
        context = load_database_context("mysql", database=":memory:")
