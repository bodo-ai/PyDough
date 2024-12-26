# Relational Expressions

This subdirectory of the PyDough directory deals with the representation of relational expressions used in the relational tree to build the final SQL query.

The relational_expressions module provides functionality to define and manage various relational expressions that can be used within PyDough.

## Available APIs

### [abstract_expression.py](abstract_expression.py)

- `RelationalExpression`: The abstract base class for all relational expressions. Each implementation class must define the following:
    - `data_type`: The data type of the relational expression.
    - `form_conjunction`: Builds a condition from a conjunction of terms.
    - `equals`: Determines if two relational expressions are exactly identical.
    - `to_string`: Converts the relational expression to a string.
    - `accept`: Visits the relational expression with the provided visitor.
    - `accept_shuttle`: Visits the relational expression with the provided shuttle and returns the new expression.

### [call_expression.py](call_expression.py)

- `CallExpression`: The expression implementation for calling a function on a relational node.

### [column_reference.py](column_reference.py)

- `ColumnReference`: The expression implementation for accessing a column in a relational node.

### [literal_expression.py](literal_expression.py)

- `LiteralExpression`: The expression implementation for a literal value in a relational node.

### [expression_sort_info.py](expression_sort_info.py)

- `ExpressionSortInfo`: The representation of ordering for an expression within a relational node.

### [relational_expression_visitor.py](relational_expression_visitor.py)

- `RelationalExpressionVisitor`: The basic Visitor pattern to perform operations across the expression components of a relational tree.

### [column_reference_finder.py](column_reference_finder.py)

- `ColumnReferenceFinder`: Finds all unique column references in a relational expression.

### [relational_expression_shuttle.py](relational_expression_shuttle.py)

- `RelationalExpressionShuttle`: Specialized form of the visitor pattern that returns a relational expression. This is used to handle the common case where we need to modify a type of input.

### [column_reference_input_name_remover.py](column_reference_input_name_remover.py)

- `ColumnReferenceInputNameRemover`: Shuttle implementation designed to remove the input name from any column reference whose name is not found in the given set.

### [column_reference_input_name_modifier.py](column_reference_input_name_modifier.py)

- `ColumnReferenceInputNameModifier`: Shuttle implementation designed to update all uses of a column reference's input name to a new input name based on a dictionary.

## Usage

To use the relational_expressions module, you can import the necessary classes and call them with the appropriate arguments. For example:

```python
from pydough.relational.relational_expressions import (
    CallExpression,
    ColumnReference,
    LiteralExpression,
    ExpressionSortInfo,
    ColumnReferenceFinder,
    ColumnReferenceInputNameModifier,
)
from pydough.pydough_operators import ADD
from pydough.types import Int64Type

# Create a column reference
column_ref = ColumnReference("column_name", Int64Type())

# Create a literal expression
literal_expr = LiteralExpression(10, Int64Type())

# Create a call expression for addition
call_expr = CallExpression(ADD, Int64Type(), [column_ref, literal_expr])

# Create an expression sort info
sort_info = ExpressionSortInfo(call_expr, ascending=True, nulls_first=False)

# Convert the call expression to a string
call_expr_str = call_expr.to_string()

# Find all unique column references in the call expression
finder = ColumnReferenceFinder()
call_expr.accept(finder)
unique_column_refs = finder.get_column_references()

# Modify the input name of column references in the call expression
modifier = ColumnReferenceInputNameModifier({"old_input_name": "new_input_name"})
modified_call_expr = call_expr.accept_shuttle(modifier)
```
