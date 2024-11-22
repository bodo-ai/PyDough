"""
Tests support for the DatabaseConnection class
using a SQLite database backend.
"""

import sqlite3

import pytest

from pydough.database_connectors.database_connector import DatabaseConnection


def test_query_execution(sqlite_people_jobs: DatabaseConnection) -> None:
    """
    Test that the DatabaseConnection can execute a query on the SQLite database.

    Args:
        sqlite_people_jobs (DatabaseConnection): The DatabaseConnection object to test.
    """
    query: str = """
        SELECT PEOPLE.person_id, COUNT(*) FROM PEOPLE
        JOIN JOBS
            ON PEOPLE.person_id = JOBS.person_id
        GROUP BY PEOPLE.person_id
    """
    result: list[sqlite3.Row] = sqlite_people_jobs.execute_query(query)
    assert len(result) == 10, "Expected 10 rows"
    assert len(result[0]) == 2, "Expected 2 columns"
    output: dict[int, int] = {row[0]: row[1] for row in result}
    assert output == {i: 2 for i in range(10)}, "Unexpected result"


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
