"""
PyDough implementation of a generic connection to database
by leveraging PEP 249 (Python Database API Specification v2.0).
https://peps.python.org/pep-0249/
"""
# Copyright (C) 2024 Bodo Inc. All rights reserved.

import sqlite3
import typing as pt


class DatabaseConnection:
    """
    Class that manages a generic DB API 2.0 connection. This basically
    dispatches to the DB API 2.0 API on the underlying object and represents
    storing the state of the active connection.
    """

    # Database connection that follows DB API 2.0 specification.
    # sqlite3 contains the connection specification and is packaged
    # with Python.
    _connection: sqlite3.Connection

    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection

    def __del__(self) -> None:
        # The connection should close automatically when __del__ is called
        # on it, but this enforces our model of transferring ownership
        # of the connection to the DatabaseConnection object.
        self._connection.close()

    def execute_query(self, sql: str) -> list[pt.Any]:
        """Create a cursor object using the connection and execute the query,
        returning the entire result.
        TODO: Support parameters.

        Args:
            sql (str): The SQL query to execute.

        Returns:
            list[pt.Any]: A list of rows returned by the query.
        """
        cursor: sqlite3.Cursor = self._connection.cursor()
        cursor.execute(sql)
        # No need to close the cursor, as its closed by del.
        # TODO: Cache the cursor?
        return cursor.fetchall()

    @property
    def connection(self) -> sqlite3.Connection:
        """
        Get the database connection. This API may be removed if all
        the functionality can be encapsulated in the DatabaseConnection.

        Returns:
            sqlite3.Connection: The connection PyDough is managing.
        """
        return self._connection
