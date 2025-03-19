""" """

__all__ = ["push_filters"]


from pydough.relational import (
    Aggregate,
    ColumnReference,
    EmptySingleton,
    Filter,
    Join,
    JoinType,
    Limit,
    Project,
    RelationalExpression,
    RelationalNode,
    Scan,
)
from pydough.relational.rel_util import (
    build_filter,
    contains_window,
    false_when_null_columns,
    get_conjunctions,
    only_references_columns,
    partition_nodes,
    transpose_node,
)


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
            remaining_filters, pushable_filters = partition_nodes(
                get_conjunctions(node.condition), contains_window
            )
            if remaining_filters:
                remaining_filters.update(filters)
                remaining_filters.update(pushable_filters)
                pushable_filters = set()
            else:
                pushable_filters.update(filters)
            return build_filter(
                push_filters(node.input, pushable_filters), remaining_filters
            )
        case Project():
            if any(contains_window(expr) for expr in node.columns.values()):
                pushable_filters, remaining_filters = set(), filters
            else:
                allowed_cols: set[str] = set()
                for name, expr in node.columns.items():
                    if isinstance(expr, ColumnReference):
                        allowed_cols.add(name)
                pushable_filters, remaining_filters = partition_nodes(
                    filters,
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
                if not isinstance(expr, ColumnReference):
                    continue
                input_name: str | None = expr.input_name
                input_idx = node.default_input_aliases.index(input_name)
                input_cols[input_idx].add(name)
            for idx, child in enumerate(node.inputs):
                if idx > 0 and node.join_types[idx - 1] == JoinType.LEFT:
                    pushable_filters, remaining_filters = partition_nodes(
                        remaining_filters,
                        lambda expr: only_references_columns(expr, input_cols[idx])
                        and false_when_null_columns(expr, input_cols[idx]),
                    )
                    if pushable_filters:
                        node.join_types[idx - 1] = JoinType.INNER
                else:
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
                filters,
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
