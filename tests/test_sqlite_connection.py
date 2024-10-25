"""
Tests support for the DatabaseConnection class
using a SQLite database backend.
"""

import pytest
import sqlite3


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
        CREATE OR REPLACE TABLE PEOPLE (
            person_id BIGINT PRIMARY KEY,
            name TEXT
        )
    """
    create_table_2: str = """
        CREATE OR REPLACE TABLE JOBS (
            job_id BIGINT PRIMARY KEY,
            person_id BIGINT,
            title TEXT
            salary FLOAT
        )
    """
    cursor: sqlite3.Cursor = connection.cursor()
    cursor.execute(create_table_1)
    cursor.execute(create_table_2)
    for i in range(10):
        cursor.execute(f"""
            INSERT INTO PEOPLE (person_id, job_id, name)
            VALUES ({i}, 'Person {i}')
        """)
        for j in range(2):
            cursor.execute(f"""
                INSERT INTO JOBS (job_id, title, salary)
                VALUES ({(2 * i) + j}, {i}, 'Job {i}', {(i + j + 5.7) * 1000})
            """)
    connection.commit()
    cursor.close()


@pytest.fixture(scope="module")
def sqlite_database() -> sqlite3.Connection:
    """
    Return a SQLite database connection a new in memory database.

    Returns:
        sqlite3.Connection: A connection to an in-memory SQLite database.
    """
    # Note the :memory: is special and just defines the database to be in memory.
    connection: sqlite3.Connection = sqlite3.connect(":memory:")
    setup_database(connection)
    return connection
