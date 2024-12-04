"""
Contains the steps/information to connect to a database and select a dialect
based on the database type.
"""

import sqlite3

from .database_connector import DatabaseConnection, DatabaseContext, DatabaseDialect

__all__ = ["load_database_context", "load_sqlite_connection"]


def load_database_context(database_name: str, **kwargs) -> DatabaseContext:
    """
    Load the database context with the appropriate connection and dialect.

    Args:
        database (str): The name of the database to connect to.
        **kwargs: Additional keyword arguments to pass to the connection.
            All arguments must be accepted using the supported connect API
            for the dialect.

    Returns:
        DatabaseContext: The database context object.
    """
    supported_databases = {"sqlite"}
    connection: DatabaseConnection
    dialect: DatabaseDialect
    match database_name.lower():
        case "sqlite":
            connection = load_sqlite_connection(**kwargs)
            dialect = DatabaseDialect.SQLITE
        case _:
            raise ValueError(
                f"Unsupported database: {database_name}. The supported databases are: {supported_databases}."
                "Any other database must be created manually by specifying the connection and dialect."
            )
    return DatabaseContext(connection, dialect)


def load_sqlite_connection(**kwargs) -> DatabaseConnection:
    """
    Loads a SQLite database connection. This is done by providing a wrapper
    around the DB 2.0 connect API.

    Returns:
        DatabaseConnection: A database connection object for SQLite.
    """
    if "database" not in kwargs:
        raise ValueError("SQLite connection requires a database path.")
    connection: sqlite3.Connection = sqlite3.connect(**kwargs)
    return DatabaseConnection(connection)
