"""
Contains the steps/information to connect to a database and select a dialect
based on the database type.
"""

import sqlite3

from .database_connector import DatabaseConnection, DatabaseContext, DatabaseDialect

__all__ = [
    "load_database_context",
    "load_mysql_connection",
    "load_sqlite_connection",
]


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
    supported_databases = {"sqlite", "mysql"}
    connection: DatabaseConnection
    dialect: DatabaseDialect
    match database_name.lower():
        case "sqlite":
            connection = load_sqlite_connection(**kwargs)
            dialect = DatabaseDialect.SQLITE
        case "mysql":
            connection = load_mysql_connection(**kwargs)
            dialect = DatabaseDialect.MYSQL
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


def load_mysql_connection(**kwargs) -> DatabaseConnection:
    """
    TODO IMPLEMENTATION + DOCSTRING
    """

    try:
        import mysql.connector
    except ImportError:
        raise ImportError(
            "MySQL connector is not installed. Please install it with"
            " `pip install mysql-connector-python`."
        )

    # mysql python connector
    connection: mysql.connector.MySQLConnection
    if connection := kwargs.pop("connection", None):
        # If a connection object is provided, return it wrapped in
        # DatabaseConnection
        return DatabaseConnection(connection)

    # MySQL connection requires specific parameters:
    # user, password, database.
    # Raise an error if any of these are missing.
    # NOTE: host, port are optional and
    # will default to the user's settings.
    # See: https://dev.mysql.com/doc/connector-python/en/connector-python-connectargs.html

    required_keys = ["user", "password", "database"]
    if not all(key in kwargs for key in required_keys):
        raise ValueError(
            "MySQL connection requires the following arguments: "
            + ", ".join(required_keys)
        )
    # Create a Snowflake connection using the provided keyword arguments
    connection = mysql.connector.connect(**kwargs)
    return DatabaseConnection(connection)
