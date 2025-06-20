"""
Contains the steps/information to connect to a database and select a dialect
based on the database type.
"""

import sqlite3

from .database_connector import DatabaseConnection, DatabaseContext, DatabaseDialect

__all__ = [
    "load_database_context",
    "load_snowflake_connection",
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
    supported_databases = {"sqlite", "snowflake"}
    connection: DatabaseConnection
    dialect: DatabaseDialect
    match database_name.lower():
        case "sqlite":
            connection = load_sqlite_connection(**kwargs)
            dialect = DatabaseDialect.SQLITE
        case "snowflake":
            connection = load_snowflake_connection(**kwargs)
            dialect = DatabaseDialect.SNOWFLAKE
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


def load_snowflake_connection(**kwargs) -> DatabaseConnection:
    """
    Loads a Snowflake database connection.
    If a connection object is provided in the keyword arguments,
    it will be used directly. Otherwise, the connection will be created
    using the provided keyword arguments.
    Args:
        **kwargs:
            The Snowflake connection or its connection parameters.
            This includes the required parameters for connecting to Snowflake,
            such as `user`, `password`, and `account`. Optional parameters
            like `database`, `schema`, and `warehouse` can also be provided.
    Raises:
        ImportError: If the Snowflake connector is not installed.
        ValueError: If required connection parameters are missing.

    Returns:
        DatabaseConnection: A database connection object for Snowflake.
    """
    try:
        import snowflake.connector
    except ImportError:
        raise ImportError(
            "Snowflake connector is not installed. Please install it with `pip install snowflake-connector-python`."
        )

    connection: snowflake.connector.connection.SnowflakeConnection
    if connection := kwargs.pop("connection", None):
        # If a connection object is provided, return it wrapped in DatabaseConnection
        return DatabaseConnection(connection)
    # Snowflake connection requires specific parameters:
    # user, password, account.
    # Raise an error if any of these are missing.
    # NOTE: database, schema, and warehouse are optional and
    # will default to the user's settings.
    # See: https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-api#label-snowflake-connector-methods-connect
    required_keys = ["user", "password", "account"]
    if not all(key in kwargs for key in required_keys):
        raise ValueError(
            "Snowflake connection requires the following arguments: "
            + ", ".join(required_keys)
        )
    # Create a Snowflake connection using the provided keyword arguments
    connection = snowflake.connector.connect(**kwargs)
    return DatabaseConnection(connection)
