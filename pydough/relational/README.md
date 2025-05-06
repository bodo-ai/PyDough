# Relational

This subdirectory of the PyDough directory deals with the representation and manipulation of relational algebra used to build the final SQL query.

The relational module is divided into two main submodules: relational expressions and relational nodes.

## [Relational Expressions](relational_expressions/README.md)

The relational_expressions submodule provides functionality to define and manage various relational expressions that can be used within PyDough.

### Available APIs

- `RelationalExpression`: The abstract base class for all relational expressions.
- `ColumnReference`: The expression implementation for accessing a column in a relational node.
- `CallExpression`: The expression implementation for calling a function on a relational node.
- `WindowCallExpression`: The expression implementation for calling a window function on relational nodes.
- `LiteralExpression`: The expression implementation for a literal value in a relational node.
- `ExpressionSortInfo`: The representation of ordering for an expression within a relational node.
- `RelationalExpressionVisitor`: The basic Visitor pattern to perform operations across the expression components of a relational tree.
- `ColumnReferenceFinder`: Finds all unique column references in a relational expression.
- `CorrelatedReference`: The expression implementation for accessing a correlated column reference in a relational node.
- `CorrelatedReferenceFinder`: Finds all unique correlated references in a relational expression.
- `RelationalExpressionShuttle`: Specialized form of the visitor pattern that returns a relational expression.
- `ColumnReferenceInputNameModifier`: Shuttle implementation designed to update all uses of a column reference's input name to a new input name based on a dictionary.

### Usage

To use the relational_expressions module, you can import the necessary classes and call them with the appropriate arguments. For example:

```python
from pydough.relational.relational_expressions import (
    CallExpression,
    ColumnReference,
    LiteralExpression,
    ExpressionSortInfo,
    ColumnReferenceFinder,
    ColumnReferenceInputNameModifier,
    CorrelatedReferenceFinder,
    WindowCallExpression,
)
from pydough.pydough_operators import ADD, RANKING
from pydough.types import NumericType

# Create a column reference
column_ref = ColumnReference("column_name", NumericType())

# Create a literal expression
literal_expr = LiteralExpression(10, NumericType())

# Create a call expression for addition
call_expr = CallExpression(ADD, NumericType(), [column_ref, literal_expr])

# Create an expression sort info
sort_info = ExpressionSortInfo(call_expr, ascending=True, nulls_first=False)

# Create a call to a window function
window_call = WindowCallExpression(RANKING, NumericType(), [], [], [sort_info], {})

# Convert the call expression to a string
call_expr_str = call_expr.to_string()

# Find all unique column references in the call expression
finder = ColumnReferenceFinder()
call_expr.accept(finder)
unique_column_refs = finder.get_column_references()

# Modify the input name of column references in the call expression
modifier = ColumnReferenceInputNameModifier({"old_input_name": "new_input_name"})
modified_call_expr = call_expr.accept_shuttle(modifier)

# Find all unique correlated references in the call expression
correlated_finder = CorrelatedReferenceFinder()
call_expr.accept(correlated_finder)
unique_correlated_refs = correlated_finder.get_correlated_references()
```

## [Relational Nodes](relational_nodes/README.md)

The relational_nodes submodule provides functionality to define and manage various relational nodes that can be used within PyDough.

### Available APIs

- `Relational`: The abstract base class for all relational nodes.
- `RelationalRoot`: The root node in any relational tree.
- `SingleRelational`: The base abstract class for relational nodes that have a single input.
- `Scan`: The relational node representing a base table in the relational tree.
- `Filter`: The relational node representing a filter operation in the relational tree.
- `Project`: The relational node representing a project operation in the relational tree.
- `Limit`: The relational node representing a limit operation in the relational tree.
- `Join`: The relational node representing a join operation in the relational tree.
    - `JoinType`: Enum class used to describe the various types of joins.
- `Aggregate`: The relational node representing an aggregation operation in the relational tree.
- `EmptySingleton`: The relational node representing a constant table with 1 row and 0 columns.
- `ColumnPruner`: Module responsible for pruning columns from relational expressions.
- `RelationalVisitor`: The basic Visitor pattern to perform operations across an entire relational tree.
- `RelationalExpressionDispatcher`: Implementation of a visitor that works by visiting every expression for each node.
- `TreeStringVisitor`: Implementation of a visitor that converts relational nodes into a tree string.

### Usage

To use the relational_nodes module, you can import the necessary classes and call them with the appropriate arguments. For example:

```python
from pydough.relational.relational_nodes import (
    Scan,
    Filter,
    Project,
    Limit,
    Join,
    RelationalRoot,
    JoinType,
)
from pydough.relational.relational_expressions import (
    ColumnReference,
    LiteralExpression,
)
from pydough.types import NumericType, BooleanType

# Create a scan node
scan_node = Scan("table_name", {"column_name": ColumnReference("column_name", NumericType())})

# Create a filter node
filter_condition = LiteralExpression(True, BooleanType())
filter_node = Filter(scan_node, filter_condition, scan_node.columns)

# Create a project node
project_node = Project(filter_node, {"column_name": ColumnReference("column_name", NumericType())})

# Create a limit node
limit_expression = LiteralExpression(10, NumericType())
limit_node = Limit(project_node, limit_expression, project_node.columns)

# Create a join node
join_condition = LiteralExpression(True, BooleanType())
join_node = Join([scan_node, filter_node], [join_condition], [JoinType.INNER], scan_node.columns)

# Create a relational root node
root_node = RelationalRoot(limit_node, [("column_name", ColumnReference("column_name", NumericType()))])

# Convert the root node to a string
root_node_str = root_node.to_string()
```

## [Relational Utilities](rel_util.py)

The rel_util file contains a myriad of useful utilities for dealing with relational nodes and relational expressions.
