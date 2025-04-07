"""
Logic used to partially transpose aggregates beneath joins when splittable into
a partial aggregation.
"""

__all__ = ["split_partial_aggregates"]


import pydough.pydough_operators as pydop
from pydough.relational import (
    Aggregate,
    CallExpression,
    ColumnReference,
    ColumnReferenceFinder,
    Join,
    JoinType,
    RelationalExpression,
    RelationalNode,
)
from pydough.relational.rel_util import (
    transpose_expression,
)

partial_aggregates: dict[
    pydop.PyDoughExpressionOperator,
    tuple[pydop.PyDoughExpressionOperator, pydop.PyDoughExpressionOperator],
] = {
    pydop.SUM: (pydop.SUM, pydop.SUM),
    pydop.COUNT: (pydop.SUM, pydop.COUNT),
    pydop.MIN: (pydop.MIN, pydop.MIN),
    pydop.MAX: (pydop.MAX, pydop.MAX),
}
"""
The aggregation functions that are possible to split into partial aggregations.
The key is the original aggregation function, and the value is a tuple of
(top_partial_agg_function, bottom_partial_agg_function).
"""


def extract_equijoin_keys(
    join: Join,
) -> tuple[list[ColumnReference], list[ColumnReference]]:
    """
    Extracts the equi-join keys from a join condition with two inputs.

    Args:
        `join`: the Join node whose condition is being parsed.

    Returns:
        A tuple where the first element are the equi-join keys from the LHS,
        and the second is a list of the the corresponding RHS keys.
    """
    assert len(join.inputs) == 2
    lhs_keys: list[ColumnReference] = []
    rhs_keys: list[ColumnReference] = []
    stack: list[RelationalExpression] = [*join.conditions]
    lhs_name: str | None = join.default_input_aliases[0]
    rhs_name: str | None = join.default_input_aliases[1]
    while stack:
        condition: RelationalExpression = stack.pop()
        if isinstance(condition, CallExpression):
            if condition.op == pydop.BAN:
                stack.extend(condition.inputs)
            elif condition.op == pydop.EQU and len(condition.inputs) == 2:
                lhs_input: RelationalExpression = condition.inputs[0]
                rhs_input: RelationalExpression = condition.inputs[1]
                if isinstance(lhs_input, ColumnReference) and isinstance(
                    rhs_input, ColumnReference
                ):
                    if (
                        lhs_input.input_name == lhs_name
                        and rhs_input.input_name == rhs_name
                    ):
                        lhs_keys.append(lhs_input)
                        rhs_keys.append(rhs_input)
                    elif (
                        lhs_input.input_name == rhs_name
                        and rhs_input.input_name == lhs_name
                    ):
                        lhs_keys.append(rhs_input)
                        rhs_keys.append(lhs_input)

    return lhs_keys, rhs_keys


def split_partial_aggregates(node: RelationalNode) -> RelationalNode:
    """
    TODO
    """
    if (
        isinstance(node, Aggregate)
        and isinstance(node.input, Join)
        and len(node.input.inputs) == 2
    ):
        join: Join = node.input
        # TODO: deal with AVG if the others are valid.
        # Verify all of the aggfuncs are from the functions that can be split.
        if all(call.op in partial_aggregates for call in node.aggregations.values()):
            # Parse the join condition to identify the lists of equi-join keys
            # from the LHS and RHS, and verify that all of the columns used by
            # the condition are in those lists.
            lhs_keys, rhs_keys = extract_equijoin_keys(join)
            finder: ColumnReferenceFinder = ColumnReferenceFinder()
            for cond in join.conditions:
                cond.accept(finder)
            condition_cols: set[ColumnReference] = finder.get_column_references()
            if all(col in lhs_keys or col in rhs_keys for col in condition_cols):
                # Identify which side of the join the aggfuncs refer to, and
                # make sure it is an INNER (+ there is only one side).
                finder.reset()
                for agg_call in node.aggregations.values():
                    transpose_expression(agg_call, join.columns, True).accept(finder)
                agg_input_names: set[str | None] = {
                    ref.input_name for ref in finder.get_column_references()
                }
                # TODO: make sure all agg keys either come from the same side,
                # as the aggregations, or are equi-join keys.
                if len(agg_input_names) == 1:
                    agg_input_name: str | None = agg_input_names.pop()
                    agg_side: int = (
                        0 if agg_input_name == join.default_input_aliases[0] else 1
                    )
                    side_keys: list[ColumnReference] = (lhs_keys, rhs_keys)[agg_side]
                    # Prune columns from join from that side, except for
                    # the agg keys.
                    join_columns_to_prune: set[str] = set()
                    for name, col in join.columns.items():
                        if (
                            isinstance(col, ColumnReference)
                            and (col.input_name == agg_input_name)
                            and (name not in node.keys)
                            and (col not in side_keys)
                        ):
                            join_columns_to_prune.add(name)
                    if agg_side == 0 or join.join_types[0] == JoinType.INNER:
                        agg_input: RelationalNode = join.inputs[agg_side]
                        # Calculate the aggregate terms to go above vs below
                        # the join.
                        top_aggs: dict[str, CallExpression] = {}
                        input_aggs: dict[str, CallExpression] = {}
                        for name, agg in node.aggregations.items():
                            top_aggfunc, bottom_aggfunc = partial_aggregates[agg.op]
                            top_aggs[name] = CallExpression(
                                top_aggfunc,
                                agg.data_type,
                                [ColumnReference(name, agg.data_type)],
                            )
                            input_aggs[name] = CallExpression(
                                bottom_aggfunc,
                                agg.data_type,
                                [
                                    transpose_expression(arg, join.columns)
                                    for arg in agg.inputs
                                ],
                            )
                            if (
                                name in join.columns
                                and name not in join_columns_to_prune
                            ):
                                raise NotImplementedError
                            join_columns_to_prune.discard(name)
                            join.columns[name] = ColumnReference(
                                name, agg.data_type, agg_input_name
                            )
                        for name in join_columns_to_prune:
                            join.columns.pop(name)
                        # Derive which columns are used as aggregate keys by
                        # the input.
                        input_keys: dict[str, ColumnReference] = {}
                        for ref in side_keys:
                            transposed_ref = transpose_expression(ref, join.columns)
                            assert isinstance(transposed_ref, ColumnReference)
                            input_keys[transposed_ref.name] = transposed_ref
                        for agg_key in node.keys.values():
                            # TODO: if not, then use equijoin key
                            transposed_agg_key = transpose_expression(
                                agg_key, join.columns, keep_input_names=True
                            )
                            assert isinstance(transposed_agg_key, ColumnReference)
                            if transposed_agg_key.input_name == agg_input_name:
                                input_keys[transposed_agg_key.name] = (
                                    transposed_agg_key.with_input(None)
                                )

                        # Push the bottom-aggregate beneath the join
                        join.inputs[agg_side] = Aggregate(
                            agg_input, input_keys, input_aggs
                        )
                        # Replace the aggregation above the join with the top
                        # side of the aggregations
                        node._aggregations = top_aggs
                        node._columns = {**node.columns, **top_aggs}

    # Recursively invoke the procedure on all inputs to the node.
    return node.copy(inputs=[split_partial_aggregates(input) for input in node.inputs])
