# Transform Bindings Module

This module provides the binding infrastructure that maps PyDough operators to implementations for converting them to SQLGlot expressions. It facilitates the transformation of PyDough function calls from relational expressions into the SQLGlot Abstract Syntax Tree (AST), which can then be converted to SQL strings for various database dialects.

## Available APIs

The Transform Bindings module provides the following notable APIs:

- `BaseTransformBindings`: The base class for converting function calls from relational expressions into the SQLGlot AST, used for generic ANSI SQL.
- `SQLiteTransformBindings`: Subclass of `BaseTransformBindings` for the SQLite dialect, providing SQLite-specific implementations.
- `SnowflakeTransformBindings`: Subclass of `BaseTransformBindings` for the Snowflake dialect, providing Snowflake-specific implementations.
- `MySQLTransformBindings`: Subclass of `BaseTransformBindings` for the MySQL dialect, providing MySQL-specific implementations.
- `PostgresTransformBindings`: Subclass of `BaseTransformBindings` for the Postgres dialect, providing Postgres-specific implementations.
- `bindings_from_dialect`: Factory function that returns the appropriate binding instance for a specific database dialect.

## Core Components

### BaseTransformBindings

The `BaseTransformBindings` class provides the foundation for transforming PyDough function calls to SQLGlot expressions. It includes:

- Standard function mappings: Direct mappings between PyDough operators and SQLGlot functions
- Binary operator mappings: Mappings for binary operators to their SQLGlot equivalents
- Conversion methods: Specialized methods for converting complex PyDough operations to their SQLGlot representations
- DateTime handling: Utilities for working with date and time operations across different dialects

### Dialect-Specific Bindings

The module includes specific binding implementations for different SQL dialects:

- `SQLiteTransformBindings`: Provides SQLite-specific implementations, handling quirks and features unique to SQLite.
- `MySQLTransformBindings`: Provides MySQL-specific implementations, handling quirks and features unique to MySQL.
- `PostgresTransformBindings`: Provides Postgres-specific implementations, handling quirks and features unique to Postgres.

These dialect-specific bindings override the base implementations as needed to provide correct SQL generation for each supported database system.

### Utility Functions

The module includes utility functions in `sqlglot_transform_utils.py` to support the transformation process:

- `DateTimeUnit`: Enum representing valid date/time units that can be used in PyDough
- `apply_parens`: Helper for adding parentheses to expressions when needed for operator precedence
- `positive_index`: Converts negative string indices to positive indices
- `pad_helper`: Utility for implementing string padding functions
- Regular expression patterns for working with date/time values

## Usage

The transform bindings are used internally by the SQLGlot module when converting PyDough relational trees to SQL. Typically, they are accessed through the `bindings_from_dialect` factory function:

```python
from pydough.database_connectors import DatabaseDialect
from pydough.configs import PyDoughConfigs
from pydough.sqlglot.transform_bindings import bindings_from_dialect

# Create bindings for a specific dialect
config = PyDoughConfigs()
bindings = bindings_from_dialect(DatabaseDialect.SQLITE, config)

# Use the bindings to convert a PyDough operator to SQLGlot
sqlglot_expr = bindings.convert_call_to_sqlglot(operator, args, types)
```

## Extending for New Dialects

To add support for a new SQL dialect:

1. Create a new subclass of `BaseTransformBindings`
2. Override methods as needed to implement dialect-specific behavior
3. Update the `bindings_from_dialect` function to return your new binding class for the appropriate dialect

For example:

```python
class DuckDBTransformBindings(BaseTransformBindings):
    """
    Subclass of BaseTransformBindings for the DuckDB dialect.
    """

    def convert_call_to_sqlglot(
        self,
        operator: pydop.PyDoughExpressionOperator,
        args: list[SQLGlotExpression],
        types: list[PyDoughType],
    ) -> SQLGlotExpression:
        # Implement Postgres-specific conversions
        match operator:
            case pydop.STARTSWITH:
                return self.convert_startswith(args, types)
        # Fall back to base implementation
        return super().convert_call_to_sqlglot(operator, args, types)

    def convert_startswith(
        self,
        args: list[SQLGlotExpression],
        types: list[PyDoughType],
    ) -> SQLGlotExpression:
        """
        Override the default STARTSWITH implementation for Postgres.
        Uses the more efficient native LIKE 'pattern%' syntax instead of POSITION.
        """
        # Postgres-specific implementation here
        pass
```
