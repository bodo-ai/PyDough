"""
Class to represent an "empty" connection that should error if any SQL
execution is attempted. This is configured to enable creating a default
session with no active backend, but which can still be used to generate
ANSI SQL.
"""

from sqlite3 import Connection

__all__ = ["empty_connection"]

from .database_connector import DatabaseConnection


class EmptyConnection(Connection):
    """
    An empty connection class that raises an error if any SQL defined
    connection method is execution.
    """

    def __init__(self):
        pass

    def commit(self):
        raise ValueError("No SQL Database is specified.")

    def close(self):
        raise ValueError("No SQL Database is specified.")

    def rollback(self):
        raise ValueError("No SQL Database is specified.")

    def cursor(self, *args, **kwargs):
        raise ValueError("No SQL Database is specified.")


empty_connection: DatabaseConnection = DatabaseConnection(EmptyConnection())
