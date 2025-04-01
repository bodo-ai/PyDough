# SQLGlot Module

This module handles the conversion of PyDough relational trees to SQL using SQLGlot. It includes utilities for transforming PyDough operators into SQLGlot expressions, executing SQL queries, and managing SQLGlot expressions.

## Available APIs

The SQLGlot module provides the following notable APIs:

- `convert_relation_to_sql`: Converts a PyDough relational tree to a SQL string using a specified SQL dialect.
- `execute_df`: Executes a PyDough relational tree on a given database context and returns the result as a Pandas DataFrame.
- `convert_dialect_to_sqlglot`: Converts a PyDough `DatabaseDialect` to the corresponding SQLGlot dialect.
- `SQLGlotRelationalVisitor`: Visitor pattern for creating SQLGlot expressions from the relational tree.
- `SQLGlotRelationalExpressionVisitor`: Visitor pattern for creating SQLGlot expressions from relational expressions.
- `find_identifiers`: Finds all unique identifiers in a SQLGlot expression.
- `find_identifiers_in_list`: Finds all unique identifiers in a list of SQLGlot expressions.
- `get_glot_name`: Gets the name of a SQLGlot expression.
- `set_glot_alias`: Sets an alias for a SQLGlot expression.
- `unwrap_alias`: Unwraps an alias from a SQLGlot expression.

## Usage

### Converting a Relational Tree to SQL, vs Executing It

To convert a PyDough relational tree to a SQL string, use the `convert_relation_to_sql` function. For example:

```python
import pydough
from pydough.relational import RelationalRoot
from pydough.database_connectors import DatabaseDialect
from sqlglot.dialects import Dialect

config = pydough.active_session.config

# Define the relational tree
relational_tree = RelationalRoot(...)

# Convert the relational tree to SQL using the dialect from the context.
sql = convert_relation_to_sql(relational_tree, ctx.dialect, config)
ctx = DatabaseContext(...)
print(sql)

# Execute the relational tree and get the result as a DataFrame
df = execute_df(relational_tree, ctx, config)
print(df)
```

### Finding Identifiers in SQLGlot Expressions

To find all unique identifiers in a SQLGlot expression, use the `find_identifiers` function. For example:

```python
from pydough.sqlglot import find_identifiers
from sqlglot.expressions import Identifier

# Define a SQLGlot expression
expr = Identifier(this="column_name")

# Find all unique identifiers in the expression
identifiers = find_identifiers(expr)
```

### Setting and Unwrapping Aliases in SQLGlot Expressions

To set an alias for a SQLGlot expression, use the `set_glot_alias` function. To unwrap an alias from a SQLGlot expression, use the `unwrap_alias` function. For example:

```python
from pydough.sqlglot import set_glot_alias, unwrap_alias
from sqlglot.expressions import Identifier

# Define a SQLGlot expression
expr = Identifier(this="column_name")

# Set an alias for the expression
aliased_expr = set_glot_alias(expr, "alias_name")
print(aliased_expr)

# Unwrap the alias from the expression
unwrapped_expr = unwrap_alias(aliased_expr)
print(unwrapped_expr)
```

By using these APIs, the SQLGlot module provides a comprehensive set of tools for converting PyDough relational trees to SQL, executing SQL queries, and managing SQLGlot expressions.
