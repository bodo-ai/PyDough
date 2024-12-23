# Database Connectors

This subdirectory of the PyDough directory deals with the connection to various databases and the execution of SQL queries.

The database connectors module provides functionality to manage database connections, execute queries, and handle different database dialects.

## Available APIs

### [database_connector.py](database_connector.py)

- `DatabaseConnection`: Manages a generic DB API 2.0 connection.
    - `execute_query_df`: Executes a SQL query and returns the result as a Pandas DataFrame.
    - `connection`: Returns the underlying database connection.
- `DatabaseDialect`: Enum for the supported database dialects.
    - `from_string`: Converts a string to a DatabaseDialect enum.
    - Supported dialects:
        - `ANSI`: Represents the ANSI SQL dialect.
        - `SQLITE`: Represents the SQLite SQL dialect.
- `DatabaseContext`: Dataclass that manages the database connection and the corresponding dialect.
    - Fields:
        - `connection`: The `DatabaseConnection` object.
        - `dialect`: The `DatabaseDialect` enum.

### [empty_connection.py](empty_connection.py)

- `empty_connection`: Represents an "empty" connection that raises an error if any SQL execution is attempted. It is used to create a default session with no active backend but can still be used to generate ANSI SQL.

### [builtin_databases.py](builtin_databases.py)

- `load_database_context`: Loads the database context with the appropriate connection and dialect.
- `load_sqlite_connection`: Loads a SQLite database connection.

## Usage

To use the database connectors module, you can import the necessary functions and classes and call them with the appropriate arguments. For example:

```python
from pydough.database_connectors import load_database_context, DatabaseDialect

# Load a SQLite database context
db_context = load_database_context("sqlite", database="path/to/database.db")

# Execute a SQL query and get the results as a DataFrame
df = db_context.connection.execute_query_df("SELECT * FROM table_name")
```