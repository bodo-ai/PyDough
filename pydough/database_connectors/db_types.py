"""
Type aliases for database connections and cursors used in type hints.
use `if TYPE_CHECKING:` to import database-specific modules in a way
that allows static type checkers to understand the types without triggering
runtime imports. This avoids runtime errors when some optional dependencies
(e.g., `snowflake-connector-python`) are not installed.
"""

from typing import TYPE_CHECKING, Any, TypeAlias

if TYPE_CHECKING:
    # Importing database-specific modules only for type checking
    # This allows us to use type hints for SQLite and Snowflake connections
    # without requiring these modules at runtime unless they are actually used.
    import sqlite3

    import snowflake.connector
    import snowflake.connector.cursor

    SQLiteConn: TypeAlias = sqlite3.Connection
    SQLiteCursor: TypeAlias = sqlite3.Cursor
    SnowflakeConn: TypeAlias = snowflake.connector.Connection
    SnowflakeCursor: TypeAlias = snowflake.connector.cursor.SnowflakeCursor

    DBConnection: TypeAlias = SQLiteConn | SnowflakeConn
    DBCursor: TypeAlias = SQLiteCursor | SnowflakeCursor
else:
    DBConnection: TypeAlias = Any
    DBCursor: TypeAlias = Any
