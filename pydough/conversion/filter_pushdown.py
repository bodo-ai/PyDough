"""
Logic used to transpose filters lower into relational trees.
"""

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
    partition_expressions,
    transpose_expression,
)


def push_filters(
    node: RelationalNode, filters: set[RelationalExpression]
) -> RelationalNode:
    """
    Transpose filter conditions down as far as possible.

    Args:
        `node`: The current node of the relational tree.
        `filters`: The set of filter conditions to push down representing
        predicates from ancestor nodes that can be pushed this far down.

    Returns:
        The transformed version of `node` with the conditions of `filters`
        pushed down as far as possible, either materializing them above the
        node or into one of its inputs, or possibly both if there are multiple
        filters.
    """
    remaining_filters: set[RelationalExpression]
    pushable_filters: set[RelationalExpression]
    match node:
        case Filter():
            # Add all of the conditions from the filters pushed down this far
            # with the filters from the current node. If there is a window
            # function, materialize all of them at this point, otherwise push
            # all of them further.
            filters.update(get_conjunctions(node.condition))
            if contains_window(node.condition):
                return build_filter(push_filters(node.input, set()), filters)
            else:
                return push_filters(node.input, filters)
        case Project():
            if any(contains_window(expr) for expr in node.columns.values()):
                # If there is a window function, materialize all filters at
                # this point.
                pushable_filters, remaining_filters = set(), filters
            else:
                # Otherwise push all filters that only depend on columns in
                # the project that are pass-through of another column.
                # For example consider the following:
                # `filters`: `{a > 0, b > 0, a > b}`
                # `node`: `Project(columns={"a": "x", "b": "LENGTH(y)"})`
                # Then the only filter that can be pushed down is `a > 0`,
                # which becomes `x > 0`.
                allowed_cols: set[str] = set()
                for name, expr in node.columns.items():
                    if isinstance(expr, ColumnReference):
                        allowed_cols.add(name)
                pushable_filters, remaining_filters = partition_expressions(
                    filters,
                    lambda expr: only_references_columns(expr, allowed_cols),
                )
                pushable_filters = {
                    transpose_expression(expr, node.columns)
                    for expr in pushable_filters
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
            # For each input to the join, push down any filters that only
            # reference columns from that input.
            for idx, child in enumerate(node.inputs):
                if idx > 0 and node.join_types[idx - 1] == JoinType.LEFT:
                    # If doing a left join, only push filters if they depend
                    # on the input and are false if the input is null. If this
                    # happens, the left join becomes an inner join.
                    pushable_filters, remaining_filters = partition_expressions(
                        remaining_filters,
                        lambda expr: only_references_columns(expr, input_cols[idx])
                        and false_when_null_columns(expr, input_cols[idx]),
                    )
                    if pushable_filters:
                        node.join_types[idx - 1] = JoinType.INNER
                else:
                    pushable_filters, remaining_filters = partition_expressions(
                        remaining_filters,
                        lambda expr: only_references_columns(expr, input_cols[idx]),
                    )
                pushable_filters = {
                    transpose_expression(expr, node.columns)
                    for expr in pushable_filters
                }
                node.inputs[idx] = push_filters(child, pushable_filters)
            # Materialize all of the remaining filters.
            return build_filter(node, remaining_filters)
        case Aggregate():
            # Only push filters that only depend on the key columns of the
            # aggregate, since they will delete entire groups.
            pushable_filters, remaining_filters = partition_expressions(
                filters,
                lambda expr: only_references_columns(expr, set(node.keys)),
            )
            pushable_filters = {
                transpose_expression(expr, node.columns) for expr in pushable_filters
            }
            node._input = push_filters(node.input, pushable_filters)
            # Materialize all of the remaining filters.
            return build_filter(node, remaining_filters)
        case Limit():
            # Materialize all of the remaining filters since a filter cannot
            # be transposed beneath a limit without changing its output.
            node._input = push_filters(node.input, set())
            return build_filter(node, filters)
        case EmptySingleton() | Scan():
            # For remaining nodes, materialize all of the remaining filters.
            return build_filter(node, filters)
        case _:
            raise NotImplementedError(
                f"push_filters not implemented for {node.__class__.__name__}"
            )
