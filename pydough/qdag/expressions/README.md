# PyDough QDAG Expressions Module

This module defines the various expression types used in the QDAG (Qualified Directed Acyclic Graph) structure of PyDough. These expressions represent different ways of accessing and manipulating data within the graph.

## Available Classes

### Base Class

`PyDoughExpressionQDAG` is the base class for all QDAG expression nodes. It provides a common interface for all expression types. That interface includes the following APIs, in addition to the base QDAG interface:

- `pydough_type`: a property indicating what type an expression returns.
- `is_aggregation`: a property indicating whether an expression collapses multiple records into a single value.
- `is_singular`: a method that takes in a collection as a context and returns whether the expression is singular with regards to that context (e.g. there is only one value of `name` or `region.name` for each record of `Nations`, but several values of `nations.name` for each value of `Regions`).
- `to_string`: converts the expression to a string. The string conversion can be different depending on whether the `tree_form` argument is True or False.
- `requires_enclosing_parens`: takes in a parent expression and returns whether the current expression should be placed inside parenthesis when converting it into a string that goes inside the string representation of the parent.

### Expression Implementation Classes

- `Literal`: Represents a literal value in the QDAG.
- `ColumnProperty`: Represents a column property of a collection.
- `ExpressionFunctionCall`: Represents a function call expression. Contains a PyDough operator that returns an expression, as well as a list of QDAG nodes (collections or expressions).
- `WindowCall`: Represents a window function call expression. Contains a PyDough operator for a window function that returns an expression, as well as the QDAG nodes for the window ordering, the number of ancestor levels that the window is relative to, and any additional keyword arguments.
- `Reference`: Represents a reference to an expression in a preceding context.
- `ChildReferenceExpression`: Represents a reference to an expression in a child collection.
- `BackReferenceExpression`: Represents a reference to an expression in an ancestor collection.
- `HiddenBackReferenceExpression`: Represents a regular reference expression that is actually a back reference because the ancestor it refers to is hidden by a compound relationship (e.g. accessing `availqty` from `supply_records` in `parts.suppliers_of_part` where that relationship is an alias for `parts.supply_records.supplier`).
- `CollationExpression`: Wraps another expression QDAG node in information about how to use it to order a collection by (e.g. ascending vs descending, nulls first vs last).
- `PartitionKey`: Wraps another expression QDAG node to indicate that it is used as a key to partition by (should only wrap a child reference to an expression from the partition data).
