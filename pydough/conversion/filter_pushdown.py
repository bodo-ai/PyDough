""" """

__all__ = ["push_filters"]


from collections.abc import Callable, Iterable, Mapping

import pydough.pydough_operators as pydop
from pydough.relational import (
    Aggregate,
    CallExpression,
    ColumnReference,
    EmptySingleton,
    Filter,
    Join,
    JoinType,
    Limit,
    LiteralExpression,
    Project,
    RelationalExpression,
    RelationalNode,
    Scan,
)
from pydough.relational.rel_util import get_conjunctions
from pydough.types import BooleanType


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
    expr: RelationalExpression, allowed_columns: set[str]
) -> bool:
    """ """
    return False


def contains_window(expr: RelationalExpression) -> bool:
    """ """
    return False


def passthrough_column_mapping(node: RelationalNode) -> dict[str, RelationalExpression]:
    """ """
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
    return node


def push_filters(
    node: RelationalNode, filters: set[RelationalExpression]
) -> RelationalNode:
    """
    Transpose filter conditions down as far as possible.
    """
    remaining_filters: set[RelationalExpression]
    pushable_filters: set[RelationalExpression]
    match node:
        case Filter():
            filters.update(get_conjunctions(node.condition))
            remaining_filters, pushable_filters = partition_nodes(
                filters, contains_window
            )
            pushable_filters = {
                transpose_node(expr, node.columns) for expr in pushable_filters
            }
            return build_filter(
                push_filters(node.input, pushable_filters), remaining_filters
            )
        case Project():
            allowed_cols: set[str] = set()
            for name, expr in node.columns.items():
                if isinstance(expr, ColumnReference):
                    allowed_cols.add(name)
            pushable_filters, remaining_filters = partition_nodes(
                remaining_filters,
                lambda expr: only_references_columns(expr, allowed_cols),
            )
            pushable_filters = {
                transpose_node(expr, node.columns) for expr in pushable_filters
            }
            node._input = push_filters(node.input, pushable_filters)
            return build_filter(node, remaining_filters)
        case Join():
            remaining_filters = filters
            input_cols: list[set[str]] = [set() for _ in range(len(node.inputs))]
            for name, expr in node.columns.items():
                assert isinstance(expr, ColumnReference)
                input_name: str | None = expr.input_name
                input_idx = node.default_input_aliases.index(input_name)
                input_cols[input_idx].add(name)
            for idx, child in enumerate(node.inputs):
                if idx > 0 and node.join_types[idx - 1] != JoinType.INNER:
                    continue
                pushable_filters, remaining_filters = partition_nodes(
                    remaining_filters,
                    lambda expr: only_references_columns(expr, input_cols[idx]),
                )
                pushable_filters = {
                    transpose_node(expr, node.columns) for expr in pushable_filters
                }
                node.inputs[idx] = push_filters(child, pushable_filters)
            return build_filter(node, remaining_filters)
        case Aggregate():
            pushable_filters, remaining_filters = partition_nodes(
                remaining_filters,
                lambda expr: only_references_columns(expr, set(node.keys)),
            )
            pushable_filters = {
                transpose_node(expr, node.columns) for expr in pushable_filters
            }
            node._input = push_filters(node.input, pushable_filters)
            return build_filter(node, remaining_filters)
        case Limit():
            node._input = push_filters(node.input, set())
            return build_filter(node, filters)
        case EmptySingleton() | Scan():
            return build_filter(node, filters)
        case _:
            raise NotImplementedError(
                f"push_filters not implemented for {node.__class__.__name__}"
            )
