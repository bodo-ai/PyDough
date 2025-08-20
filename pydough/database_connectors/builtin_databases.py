"""
Contains the steps/information to connect to a database and select a dialect
based on the database type.
"""

import sqlite3
import time

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
        `database`: The name of the database to connect to.
        `**kwargs`: Additional keyword arguments to pass to the connection.
            All arguments must be accepted using the supported connect API
            for the dialect.

    Returns:
        The database context object.
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
        A database connection object for SQLite.
    """
    if "database" not in kwargs:
        raise ValueError("SQLite connection requires a database path.")
    connection: sqlite3.Connection = sqlite3.connect(**kwargs)
    return DatabaseConnection(connection)


def load_mysql_connection(**kwargs) -> DatabaseConnection:
    """
    Loads a MySQL database connection. This is done by providing a wrapper
    around the DB 2.0 connect API.

    Args:
        **kwargs: Either a MySQL connection object (as `connection=<object>`)
            or the required connection parameters:
            - user: MySQL username (str)
            - password: MySQL password (str)
            - database: Database name (str)
            Optionally, you can provide:
            - host: MySQL server host (str, default: "127.0.0.1"/"localhost")
            - port: MySQL server port (int, default: 3306)
            - connection_timeout: Timeout for the connection (float, default: 3 seconds).
            - attempts (not a MySQL connector parameter): Number of connection attempts (int, default: 3)
            - delay (not a MySQL connector parameter): Delay between connection attempts (float, default: 2 seconds).
            If a connection object is provided, it will be used directly.
            Optional parameters such as host, port, etc. can also be provided.
            All arguments must be accepted by the MySQL connector connect API.

    Raises:
        ImportError: If the MySQL connector is not installed.
        ValueError: If required connection parameters are missing.

    Returns:
        A database connection object for MySQL.
    """

    try:
        import mysql.connector
    except ImportError:
        raise ImportError(
            "MySQL connector is not installed. Please install it with"
            " `pip install mysql-connector-python`."
        )

    # MySQL Python connector
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

    required_keys: list[str] = ["user", "password", "database"]
    if not all(key in kwargs for key in required_keys):
        raise ValueError(
            "MySQL connection requires the following arguments: "
            + ", ".join(required_keys)
        )

    # Default timeout for connection
    if "connection_timeout" not in kwargs or kwargs["connection_timeout"] <= 0:
        kwargs["connection_timeout"] = 3

    # Default attempts for connection if not given
    if not (attempts := kwargs.pop("attempts", None)):
        attempts = 1

    # Default delay between attempts for connection if not given
    if not (delay := kwargs.pop("delay", None)):
        delay = 2.0

    attempt: int = 1

    # For each attempt a connection is tried
    # If it fails, there is a delay before another attempt is executed
    while attempt <= attempts:
        try:
            connection = mysql.connector.connect(**kwargs)
            return DatabaseConnection(connection)

        except (OSError, mysql.connector.Error) as err:
            if attempt >= attempts:
                raise ValueError(
                    f"Failed to connect to MySQL after {attempts} attempts: {err}"
                )
            # Delay for another attempt
            time.sleep(delay)
            attempt += 1

    raise ValueError(f"Failed to connect to MySQL after {attempts} attempts")
