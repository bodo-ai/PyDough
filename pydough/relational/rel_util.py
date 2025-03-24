"""
A mixture of utility functions for relational nodes and expressions.
"""

from collections.abc import Callable, Iterable, Mapping

import pydough.pydough_operators as pydop
from pydough.types import BooleanType

from .relational_expressions import (
    CallExpression,
    ColumnReference,
    CorrelatedReference,
    ExpressionSortInfo,
    LiteralExpression,
    RelationalExpression,
    WindowCallExpression,
)
from .relational_nodes import (
    Filter,
    RelationalNode,
)

null_propagating_operators = {
    pydop.EQU,
    pydop.LET,
    pydop.LEQ,
    pydop.GRT,
    pydop.GEQ,
    pydop.LET,
    pydop.NEQ,
    pydop.STARTSWITH,
    pydop.ENDSWITH,
    pydop.CONTAINS,
    pydop.LIKE,
    pydop.LOWER,
    pydop.UPPER,
    pydop.LENGTH,
    pydop.YEAR,
    pydop.MONTH,
    pydop.DAY,
    pydop.HOUR,
    pydop.MINUTE,
    pydop.SECOND,
    pydop.DATETIME,
    pydop.DATEDIFF,
    pydop.JOIN_STRINGS,
    pydop.ADD,
    pydop.SUB,
    pydop.MUL,
    pydop.DIV,
}
"""
A set of operators with the property that the output is null if any of the
inputs are null.
"""


def get_conjunctions(expr: RelationalExpression) -> set[RelationalExpression]:
    """
    Extract conjunctions from the given expression.

    Args:
        `expr`: The expression to extract conjunctions from.

    Returns:
        The set of filter conditions whose conjunction forms `expr`.
    """
    if isinstance(expr, LiteralExpression) and expr.value:
        # If the expression is a true literal, there are no predicates as the
        # conjunction is always True.
        return set()
    elif isinstance(expr, CallExpression) and expr.op == pydop.BAN:
        # If the expression is an AND call, flatten to obtain the conjunction
        # by gathering the conjunction of all of the inputs.
        result = set()
        for arg in expr.inputs:
            result.update(get_conjunctions(arg))
        return result
    else:
        # Otherwise, the expression itself is the conjunction.
        return {expr}


def partition_expressions(
    expressions: Iterable[RelationalExpression],
    predicate: Callable[[RelationalExpression], bool],
) -> tuple[set[RelationalExpression], set[RelationalExpression]]:
    """
    Partition the given relational expressions into two sets based on the given
    predicate.

    Args:
        `expressions`: The expressions to partition.
        `predicate`: The predicate to use for partitioning.

    Returns:
        A tuple of two sets of expressions, the first of expressions that cause
        the predicate to return True and the second of the remainder.
    """
    true_expressions: set[RelationalExpression] = set()
    false_expressions: set[RelationalExpression] = set()
    for expr in expressions:
        if predicate(expr):
            true_expressions.add(expr)
        else:
            false_expressions.add(expr)
    return true_expressions, false_expressions


def only_references_columns(
    expr: RelationalExpression, allowed_columns: set[str]
) -> bool:
    """
    Checks if a relational expression contains only column references from the set of allowed columns.

    Args:
        `expr`: The expression to check.
        `allowed_columns`: The set of allowed columns.

    Returns:
        Whether `expr` meets the criteria.
    """
    match expr:
        case LiteralExpression() | CorrelatedReference():
            return True
        case ColumnReference():
            return expr.name in allowed_columns
        case CallExpression():
            return all(
                only_references_columns(arg, allowed_columns) for arg in expr.inputs
            )
        case WindowCallExpression():
            return (
                all(
                    only_references_columns(arg, allowed_columns) for arg in expr.inputs
                )
                and all(
                    only_references_columns(arg, allowed_columns)
                    for arg in expr.partition_inputs
                )
                and all(
                    only_references_columns(order_arg.expr, allowed_columns)
                    for order_arg in expr.order_inputs
                )
            )
        case _:
            raise NotImplementedError(
                f"only_references_columns not implemented for {expr.__class__.__name__}"
            )


def false_when_null_columns(expr: RelationalExpression, null_columns: set[str]) -> bool:
    """
    Returns whether an expression is guaranteed to be False, as far as a filter
    is concerned, if certain columns are null.

    Args:
        `expr`: The expression to check.
        `null_columns`: The set of columns that are null.

    Returns:
        Whether `expr` meets the criteria.
    """
    match expr:
        case LiteralExpression() | CorrelatedReference():
            return False
        case ColumnReference():
            return expr.name in null_columns
        case CallExpression():
            if expr.op in null_propagating_operators:
                return any(
                    false_when_null_columns(arg, null_columns) for arg in expr.inputs
                )
            return False
        case WindowCallExpression():
            return False
        case _:
            raise NotImplementedError(
                f"false_when_null_columns not implemented for {expr.__class__.__name__}"
            )


def contains_window(expr: RelationalExpression) -> bool:
    """
    Returns whether a relational expression contains a window function.

    Args:
        `expr`: The expression to check.

    Returns:
        Whether `expr` contains a window function.
    """
    match expr:
        case LiteralExpression() | CorrelatedReference() | ColumnReference():
            return False
        case CallExpression():
            return any(contains_window(arg) for arg in expr.inputs)
        case WindowCallExpression():
            return True
        case _:
            raise NotImplementedError(
                f"contains_window not implemented for {expr.__class__.__name__}"
            )


def passthrough_column_mapping(node: RelationalNode) -> dict[str, RelationalExpression]:
    """
    Builds a mapping of column names to their corresponding column references
    for the given relational node.

    Args:
        `node`: The relational node to build the mapping from.

    Returns:
        A dictionary mapping column names to their corresponding column
        references from `node`.
    """
    result: dict[str, RelationalExpression] = {}
    for name, expr in node.columns.items():
        result[name] = ColumnReference(name, expr.data_type)
    return result


def build_filter(
    node: RelationalNode, filters: set[RelationalExpression]
) -> RelationalNode:
    """
    Build a filter node with the given filters on top of an input node.

    Args:
        `node`: The input node to build the filter on top of.
        `filters`: The set of filters to apply.

    Returns:
        A filter node with the given filters applied on top of `node`. If
        the set of filters is empty, just returns `node`. Ignores any filter
        condition that is always True.
    """
    filters.discard(LiteralExpression(True, BooleanType()))
    condition: RelationalExpression
    if len(filters) == 0:
        return node
    elif len(filters) == 1:
        condition = filters.pop()
    else:
        condition = CallExpression(pydop.BAN, BooleanType(), sorted(filters, key=repr))
    return Filter(node, condition, passthrough_column_mapping(node))


def transpose_expression(
    expr: RelationalExpression, columns: Mapping[str, RelationalExpression]
) -> RelationalExpression:
    """
    Rewrites an expression by replacing its column references based on a given
    column mapping, allowing the expression to be pushed beneath the node that
    introduced the mapping. For example, if a node renamed columns, this
    function translates the expression from the new column names back to the
    original names.

    Args:
        `expr`: The expression to transposed.
        `columns`: The mapping of column names to their corresponding
        expressions.

    Returns:
        The transposed expression with updated column references.
    """
    match expr:
        case LiteralExpression() | CorrelatedReference():
            return expr
        case ColumnReference():
            new_column = columns.get(expr.name)
            assert isinstance(new_column, ColumnReference)
            if new_column.input_name is not None:
                new_column = new_column.with_input(None)
            return new_column
        case CallExpression():
            return CallExpression(
                expr.op,
                expr.data_type,
                [transpose_expression(arg, columns) for arg in expr.inputs],
            )
        case WindowCallExpression():
            return WindowCallExpression(
                expr.op,
                expr.data_type,
                [transpose_expression(arg, columns) for arg in expr.inputs],
                [transpose_expression(arg, columns) for arg in expr.partition_inputs],
                [
                    ExpressionSortInfo(
                        transpose_expression(order_arg.expr, columns),
                        order_arg.ascending,
                        order_arg.nulls_first,
                    )
                    for order_arg in expr.order_inputs
                ],
                expr.kwargs,
            )
        case _:
            raise NotImplementedError(
                f"transpose_expression not implemented for {expr.__class__.__name__}"
            )
