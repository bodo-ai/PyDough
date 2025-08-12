"""
Type aliases for database connections and cursors used in type hints.
use `if TYPE_CHECKING:` to import database-specific modules in a way
that allows static type checkers to understand the types without triggering
runtime imports. This avoids runtime errors when some optional dependencies
are not installed.
"""

from typing import TYPE_CHECKING, Any, TypeAlias

if TYPE_CHECKING:
    # Importing database-specific modules only for type checking
    # This allows us to use type hints for SQL dialect connections
    # (PostgreSQL, SQLite, ..etc.)
    # without requiring these modules at runtime unless they are actually used.
    import sqlite3

    import psycopg2

    PostgreSQLConn: TypeAlias = psycopg2.connection  # type: ignore
    PostgreSQLCursor: TypeAlias = psycopg2.cursor  # type: ignore

    SQLiteConn: TypeAlias = sqlite3.Connection
    SQLiteCursor: TypeAlias = sqlite3.Cursor

    # TBD: Placeholder lines to add other dialects.
    # 1. Replace with actual dialect module
    # import dialect1_module
    # 2. Replace with other dialect connections
    # Dialect1_Conn: TypeAlias = dialect1_module.Connection
    # 3. Replace with other dialect cursors
    # Dialect1_Cursor: TypeAlias = dialect1_module.Cursor

    # 4. Define the type aliases for database connections and cursors
    DBConnection: TypeAlias = SQLiteConn | PostgreSQLConn  # | Dialect1_Conn
    DBCursor: TypeAlias = SQLiteCursor | PostgreSQLCursor  # | Dialect1_Cursor
else:
    DBConnection: TypeAlias = Any
    DBCursor: TypeAlias = Any
    PostgreSQLConn: TypeAlias = Any
    PostgreSQLCursor: TypeAlias = Any
    SQLiteConn: TypeAlias = Any
    SQLiteCursor: TypeAlias = Any
    # Dialect1_Conn: TypeAlias = Any
    # Dialect1_Cursor: TypeAlias = Any

# This allows us to use these type aliases in the rest of the code
# without worrying about whether the specific database modules are available.
__all__ = [
    "DBConnection",
    "DBCursor",
    "PostgreSQLConn",
    "PostgreSQLCursor",
    "SQLiteConn",
    "SQLiteCursor",
    # "Dialect1_Conn",
    # "Dialect1_Cursor",
]
# The type aliases are used to provide a consistent interface for database connections
# and cursors across different database backends, allowing for easier
# type hinting and code readability without requiring the actual database modules.
