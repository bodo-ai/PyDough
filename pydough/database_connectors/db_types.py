from typing import TYPE_CHECKING, Any, TypeAlias

if TYPE_CHECKING:
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
