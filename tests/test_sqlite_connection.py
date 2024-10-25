"""
Tests support for the DatabaseConnection class
using a SQLite database backend.
"""

import pytest
import sqlite3
from pydough.database_connectors.database_connector import DatabaseConnection


def setup_database(connection: sqlite3.Connection) -> None:
    """
    Initializes a SQLite database with a several tables for
    testing connections.

    Args:
        connection (sqlite3.Connection): The SQLite connection
        that is assumed to be an empty database dedicated to
        these tests.
    """

    create_table_1: str = """
        CREATE TABLE PEOPLE (
            person_id BIGINT PRIMARY KEY,
            name TEXT
        )
    """
    create_table_2: str = """
        CREATE TABLE JOBS (
            job_id BIGINT PRIMARY KEY,
            person_id BIGINT,
            title TEXT,
            salary FLOAT
        )
    """
    cursor: sqlite3.Cursor = connection.cursor()
    cursor.execute(create_table_1)
    cursor.execute(create_table_2)
    for i in range(10):
        cursor.execute(f"""
            INSERT INTO PEOPLE (person_id, name)
            VALUES ({i}, 'Person {i}')
        """)
        for j in range(2):
            cursor.execute(f"""
                INSERT INTO JOBS (job_id, person_id, title, salary)
                VALUES ({(2 * i) + j}, {i}, 'Job {i}', {(i + j + 5.7) * 1000})
            """)
    connection.commit()
    cursor.close()


@pytest.fixture
def sqlite_database() -> sqlite3.Connection:
    """
    Return a SQLite database connection a new in memory database.
    Note: This isn't set at a module level because we want to
    close the connection after the test is done.

    Returns:
        sqlite3.Connection: A connection to an in-memory SQLite database.
    """
    # Note the :memory: is special and just defines the database to be in memory.
    connection: sqlite3.Connection = sqlite3.connect(":memory:")
    setup_database(connection)
    return connection


@pytest.fixture
def database_connector(sqlite_database: sqlite3.Connection) -> DatabaseConnection:
    """
    Return a DatabaseConnection object that wraps the SQLite connection.

    Args:
        sqlite_database (sqlite3.Connection): The SQLite connection to wrap.

    Returns:
        DatabaseConnection: A DatabaseConnection object that wraps the SQLite connection.
    """
    return DatabaseConnection(sqlite_database)


def test_query_execution(database_connector: DatabaseConnection) -> None:
    """
    Test that the DatabaseConnection can execute a query on the SQLite database.

    Args:
        database_connector (DatabaseConnection): The DatabaseConnection object to test.
    """
    query: str = """
        SELECT PEOPLE.person_id, COUNT(*) FROM PEOPLE
        JOIN JOBS
            ON PEOPLE.person_id = JOBS.person_id
        GROUP BY PEOPLE.person_id
    """
    result: list[sqlite3.Row] = database_connector.execute_query(query)
    assert len(result) == 10, "Expected 10 rows"
    assert len(result[0]) == 2, "Expected 2 columns"
    output: dict[int, int] = {row[0]: row[1] for row in result}
    assert output == {i: 2 for i in range(10)}, "Unexpected result"


def test_unusable_after_del(sqlite_database: sqlite3.Connection) -> None:
    """
    Test that the underlying connection is closed when the DatabaseConnection
    object is deleted. This ensures the semantics of the DatabaseConnection
    are to assume ownership of the connection.

    Args:
        sqlite_database (sqlite3.Connection): The connection for building the
        database connection..
    """
    # Create a standalone connection to the database so we can inspect the garbage count.
    connection: DatabaseConnection = DatabaseConnection(sqlite_database)
    del connection
    with pytest.raises(sqlite3.ProgrammingError):
        sqlite_database.execute("SELECT * FROM PEOPLE")
