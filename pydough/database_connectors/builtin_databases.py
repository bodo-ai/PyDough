"""
Contains the steps/information to connect to a database and select a dialect
based on the database type.
"""

import sqlite3

from .database_connector import DatabaseConnection, DatabaseContext, DatabaseDialect

__all__ = [
    "load_database_context",
    "load_postgres_connection",
    "load_sqlite_connection",
]


def load_database_context(database_name: str, **kwargs) -> DatabaseContext:
    """
    Load the database context with the appropriate connection and dialect.

    Args:
        `database_name`: The name of the database to connect to.
        `**kwargs`: Additional keyword arguments to pass to the connection.
            All arguments must be accepted using the supported connect API
            for the dialect.

    Returns:
        The database context object.
    """
    supported_databases = {"postgres", "sqlite"}
    connection: DatabaseConnection
    dialect: DatabaseDialect
    match database_name.lower():
        case "sqlite":
            connection = load_sqlite_connection(**kwargs)
            dialect = DatabaseDialect.SQLITE
        case "postgres" | "postgresql":
            connection = load_postgres_connection(**kwargs)
            dialect = DatabaseDialect.POSTGRES
        case _:
            raise ValueError(
                f"Unsupported database: {database_name}. The supported databases are: {supported_databases}."
                "Any other database must be created manually by specifying the connection and dialect."
            )
    return DatabaseContext(connection, dialect)


def load_postgres_connection(**kwargs) -> DatabaseConnection:
    """
    Loads a PostgreSQL database connection. This is done by providing a wrapper
    around the DB 2.0 connect API.
    Returns:
        A database connection object for PostgreSQL.
    """

    try:
        import psycopg2
    except ImportError:
        raise ImportError(
            "PostgreSQL connector psycopg2 is not installed. Please install it with"
            " `uv pip install psycopg2-binary`."
        )

    # PostgreSQL python connector
    connection: psycopg2.extensions.connection
    if connection := kwargs.pop("connection", None):
        # If a connection object is provided, return it wrapped in
        # DatabaseConnection
        return DatabaseConnection(connection)

    # PostgreSQL connection requires specific parameters:
    # user, password, dbname.
    # Raise an error if any of these are missing.
    # NOTE: host, port are optional and will default to the psycopg2 defaults.
    # See: https://www.psycopg.org/docs/module.html#psycopg2.connect

    required_keys = ["user", "password", "dbname"]
    if not all(key in kwargs for key in required_keys):
        raise ValueError(
            "PostgreSQL connection requires at least the following arguments: "
            + ", ".join(required_keys)
        )

    # Connect to PostgreSQL using DB API 2.0 parameters
    connection = psycopg2.connect(**kwargs)

    return DatabaseConnection(connection)


def load_sqlite_connection(**kwargs) -> DatabaseConnection:
    """
    Loads a SQLite database connection. This is done by providing a wrapper
    around the DB 2.0 connect API.

    Returns:
        A database connection object for SQLite.
    """
    if "database" not in kwargs:
        raise ValueError("SQLite connection requires a database path.")
    connection: sqlite3.Connection = sqlite3.connect(**kwargs)
    return DatabaseConnection(connection)
