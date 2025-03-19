"""
TODO
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


def get_conjunctions(expr: RelationalExpression) -> set[RelationalExpression]:
    """
    Extract conjunctions from the given expression.
    """
    if isinstance(expr, LiteralExpression) and expr.value:
        return set()
    if isinstance(expr, CallExpression) and expr.op == pydop.BAN:
        result = set()
        for arg in expr.inputs:
            result.update(get_conjunctions(arg))
        return result
    return {expr}


def partition_nodes(
    nodes: Iterable[RelationalExpression],
    predicate: Callable[[RelationalExpression], bool],
) -> tuple[set[RelationalExpression], set[RelationalExpression]]:
    """
    Partition the given nodes into two sets based on the given predicate.
    """
    true_nodes: set[RelationalExpression] = set()
    false_nodes: set[RelationalExpression] = set()
    for node in nodes:
        if predicate(node):
            true_nodes.add(node)
        else:
            false_nodes.add(node)
    return true_nodes, false_nodes


def only_references_columns(
    node: RelationalExpression, allowed_columns: set[str]
) -> bool:
    """
    TODO
    """
    match node:
        case LiteralExpression() | CorrelatedReference():
            return True
        case ColumnReference():
            return node.name in allowed_columns
        case CallExpression():
            return all(
                only_references_columns(arg, allowed_columns) for arg in node.inputs
            )
        case WindowCallExpression():
            return (
                all(
                    only_references_columns(arg, allowed_columns) for arg in node.inputs
                )
                and all(
                    only_references_columns(arg, allowed_columns)
                    for arg in node.partition_inputs
                )
                and all(
                    only_references_columns(order_arg.expr, allowed_columns)
                    for order_arg in node.order_inputs
                )
            )
        case _:
            raise NotImplementedError(
                f"transpose_node not implemented for {node.__class__.__name__}"
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
TODO
"""


def false_when_null_columns(
    node: RelationalExpression, allowed_columns: set[str]
) -> bool:
    """
    TODO
    """
    match node:
        case LiteralExpression() | CorrelatedReference():
            return False
        case ColumnReference():
            return node.name in allowed_columns
        case CallExpression():
            if node.op in null_propagating_operators:
                return any(
                    false_when_null_columns(arg, allowed_columns) for arg in node.inputs
                )
            return False
        case WindowCallExpression():
            return False
        case _:
            raise NotImplementedError(
                f"transpose_node not implemented for {node.__class__.__name__}"
            )


def contains_window(node: RelationalExpression) -> bool:
    """
    TODO
    """
    match node:
        case LiteralExpression() | CorrelatedReference() | ColumnReference():
            return False
        case CallExpression():
            return any(contains_window(arg) for arg in node.inputs)
        case WindowCallExpression():
            return True
        case _:
            raise NotImplementedError(
                f"transpose_node not implemented for {node.__class__.__name__}"
            )


def passthrough_column_mapping(node: RelationalNode) -> dict[str, RelationalExpression]:
    """
    TODO
    """
    result: dict[str, RelationalExpression] = {}
    for name, expr in node.columns.items():
        result[name] = ColumnReference(name, expr.data_type)
    return result


def build_filter(
    node: RelationalNode, filters: set[RelationalExpression]
) -> RelationalNode:
    """
    Build a filter node with the given filters.
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


def transpose_node(
    node: RelationalExpression, columns: Mapping[str, RelationalExpression]
) -> RelationalExpression:
    """
    TODO
    """
    match node:
        case LiteralExpression() | CorrelatedReference():
            return node
        case ColumnReference():
            new_column = columns.get(node.name)
            assert isinstance(new_column, ColumnReference)
            if new_column.input_name is not None:
                new_column = new_column.with_input(None)
            return new_column
        case CallExpression():
            return CallExpression(
                node.op,
                node.data_type,
                [transpose_node(arg, columns) for arg in node.inputs],
            )
        case WindowCallExpression():
            return WindowCallExpression(
                node.op,
                node.data_type,
                [transpose_node(arg, columns) for arg in node.inputs],
                [transpose_node(arg, columns) for arg in node.partition_inputs],
                [
                    ExpressionSortInfo(
                        transpose_node(order_arg.expr, columns),
                        order_arg.ascending,
                        order_arg.nulls_first,
                    )
                    for order_arg in node.order_inputs
                ],
                node.kwargs,
            )
        case _:
            raise NotImplementedError(
                f"transpose_node not implemented for {node.__class__.__name__}"
            )
