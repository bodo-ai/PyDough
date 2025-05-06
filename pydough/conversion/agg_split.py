"""
Logic used to partially transpose aggregates beneath joins when splittable into
a partial aggregation.
"""

__all__ = ["split_partial_aggregates"]


import pydough.pydough_operators as pydop
from pydough.configs import PyDoughConfigs
from pydough.relational import (
    Aggregate,
    CallExpression,
    ColumnReference,
    ColumnReferenceFinder,
    Join,
    JoinType,
    LiteralExpression,
    Project,
    RelationalExpression,
    RelationalNode,
)
from pydough.relational.rel_util import (
    extract_equijoin_keys,
    fetch_or_insert,
    transpose_expression,
)
from pydough.types import NumericType

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

decomposable_aggfuncs: set[pydop.PyDoughExpressionOperator] = {pydop.AVG}
"""
The aggregation functions that are decomposable into multiple calls to partial
aggregations.
"""


def decompose_aggregations(node: Aggregate, config: PyDoughConfigs) -> RelationalNode:
    """
    Splits up an aggregate node into an aggregate followed by a projection when
    the aggregate contains 1+ calls to functions that can be split into 1+
    calls to partial aggregates, e.g. how AVG(X) = SUM(X)/COUNT(X).

    Args:
        `node`: the aggregate node to be decomposed.
        `config`: the current configuration settings.

    Returns:
        The projection node on top of the new aggregate, overall containing the
        equivalent to the original aggregations.
    """
    decomposable: dict[str, CallExpression] = {}
    new_aggregations: dict[str, RelationalExpression] = {}
    final_agg_columns: dict[str, RelationalExpression] = {}
    # First, separate the aggregations that should be decomposed from those
    # that should not. Place the ones that should in the decomposable dict
    # to deal with later, and place the rest in the new output dictionaries.
    for name, agg in node.aggregations.items():
        if agg.op in decomposable_aggfuncs:
            decomposable[name] = agg
        else:
            new_aggregations[name] = agg
            final_agg_columns[name] = ColumnReference(name, agg.data_type)

    # For each decomposable agg call, invoke the procedure to split it into
    # multiple aggregation calls that are placed in `new_aggregations` (without
    # adding any new duplicates), then the logic to combine them with scalar
    # computations in `final_agg_columns`.
    for name, agg in decomposable.items():
        # Decompose the aggregate into its components.
        agg_input: RelationalExpression = agg.inputs[0]
        if agg.op == pydop.AVG:
            # AVG is decomposed into SUM and COUNT, and then the division
            # is done in the projection.
            sum_call: CallExpression = CallExpression(
                pydop.SUM,
                agg.data_type,
                [agg_input],
            )
            count_call: CallExpression = CallExpression(
                pydop.COUNT,
                NumericType(),
                [agg_input],
            )
            sum_name: str = fetch_or_insert(new_aggregations, sum_call)
            count_name: str = fetch_or_insert(new_aggregations, count_call)
            avg_call: CallExpression = CallExpression(
                pydop.DIV,
                agg.data_type,
                [
                    ColumnReference(sum_name, sum_call.data_type),
                    ColumnReference(count_name, count_call.data_type),
                ],
            )
            # If the config specifies that the default value for AVG should be
            # zero, wrap the division in a DEFAULT_TO call.
            if config.avg_default_zero:
                avg_call = CallExpression(
                    pydop.DEFAULT_TO,
                    agg.data_type,
                    [avg_call, LiteralExpression(0, NumericType())],
                )
            final_agg_columns[name] = avg_call
        else:
            raise NotImplementedError(f"Unsupported aggregate function: {agg.op}")

    # Build the new aggregate with the new projection on top of it to derive
    # any aggregation calls by combining aggfunc values.
    aggs: dict[str, CallExpression] = {}
    for name, agg_expr in new_aggregations.items():
        assert isinstance(agg_expr, CallExpression)
        aggs[name] = agg_expr
    new_aggregate: Aggregate = Aggregate(node.input, node.keys, aggs)
    project_columns: dict[str, RelationalExpression] = {}
    for name, expr in node.keys.items():
        project_columns[name] = expr
    project_columns.update(
        {name: final_agg_columns[name] for name in node.aggregations}
    )
    return Project(new_aggregate, project_columns)


def transpose_aggregate_join(
    node: Aggregate,
    join: Join,
    agg_side: int,
    side_keys: list[ColumnReference],
    config: PyDoughConfigs,
) -> RelationalNode:
    """
    Transposes the aggregate node above the join into two aggregate nodes,
    one above the join and one below the join. Does the transformation
    in-place, and either returns the node or a post-processing aggregation
    on top of it, then recursively transforms the inputs by passing back to
    split_partial_aggregates.

    Args:
        `node`: the aggregate node to be split.
        `join`: the join node that the aggregate is above.
        `agg_side`: the index of the input to the join that the aggregate is
        being pushed into.
        `side_keys`: the list of equi-join keys from the side of the join
        that the aggregate is being pushed into.
        `config`: the current configuration settings.

    Returns:
        The transformed node. The transformation is also done-in-place.
    """
    agg_input_name: str | None = join.default_input_aliases[agg_side]
    # Keep a dictionary for the projection columns that will be used to post-process
    # the output of the aggregates, if needed.
    need_projection: bool = False
    projection_columns: dict[str, RelationalExpression] = {**node.keys}
    # Mark columns from the pushdown side of the join to be pruned, except for
    # the agg/join keys.
    join_columns_to_prune: set[str] = set()
    for name, col in join.columns.items():
        if (
            isinstance(col, ColumnReference)
            and (col.input_name == agg_input_name)
            and (name not in node.keys)
            and (col not in side_keys)
        ):
            join_columns_to_prune.add(name)

    # Calculate the aggregate terms to go above vs below the join.
    agg_input: RelationalNode = join.inputs[agg_side]
    top_aggs: dict[str, CallExpression] = {}
    input_aggs: dict[str, CallExpression] = {}
    for name, agg in node.aggregations.items():
        # Pick the name of the aggregate output column that
        # does not collide with an existing used name.
        bottom_name: str = name
        idx: int = 0
        while bottom_name in join.columns and bottom_name not in join_columns_to_prune:
            bottom_name = f"{name}_{idx}"
            idx += 1
        # Build the aggregation calls for before/after the join, and place them
        # in the dictionaries that will build the new aggregate nodes.
        top_aggfunc, bottom_aggfunc = partial_aggregates[agg.op]
        top_aggs[name] = CallExpression(
            top_aggfunc,
            agg.data_type,
            [ColumnReference(bottom_name, agg.data_type)],
        )
        # Insert the column reference for the top-aggfunc into the projection,
        # and if needed wrap it in a DEFAULT_TO call for COUNT. This is
        # required for left joins, or no-groupby aggregates.
        if agg.op == pydop.COUNT and (
            join.join_types[0] != JoinType.INNER or len(node.keys) == 0
        ):
            projection_columns[name] = CallExpression(
                pydop.DEFAULT_TO,
                agg.data_type,
                [
                    ColumnReference(name, agg.data_type),
                    LiteralExpression(0, NumericType()),
                ],
            )
            need_projection = True
        else:
            projection_columns[name] = ColumnReference(name, agg.data_type)
        input_aggs[bottom_name] = CallExpression(
            bottom_aggfunc,
            agg.data_type,
            [transpose_expression(arg, join.columns) for arg in agg.inputs],
        )
        join_columns_to_prune.discard(bottom_name)
        join.columns[bottom_name] = ColumnReference(
            bottom_name, agg.data_type, agg_input_name
        )
    # Remove the columns that are no longer needed from the join.
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
        transposed_agg_key = transpose_expression(
            agg_key, join.columns, keep_input_names=True
        )
        assert isinstance(transposed_agg_key, ColumnReference)
        if transposed_agg_key.input_name == agg_input_name:
            input_keys[transposed_agg_key.name] = transposed_agg_key.with_input(None)

    # Push the bottom-aggregate beneath the join
    join.inputs[agg_side] = Aggregate(agg_input, input_keys, input_aggs)
    # Replace the aggregation above the join with the top
    # side of the aggregations
    node._aggregations = top_aggs
    node._columns = {**node.columns, **top_aggs}
    if need_projection:
        return Project(node, projection_columns)
    else:
        return node


def attempt_join_aggregate_transpose(
    node: Aggregate, join: Join, config: PyDoughConfigs
) -> tuple[RelationalNode, bool]:
    """
    Determine whether the aggregate join transpose operation can occur, and if
    so invoke it, otherwise return the top node un-modified.

    Args:
        `node`: the aggregate node to be transformed.
        `join`: the join node that the aggregate is above.
        `config`: the current configuration settings.

    Returns:
        A tuple where the first element is the transformed node, and the second
        is a boolean indicating whether the output needs to have its inputs
        recursively transformed (if False, it means they have already been
        recursively transformed).
    """
    # Verify there are exactly two inputs to the join
    if len(join.inputs) != 2:
        return node, True

    # Verify all of the aggfuncs are from the functions that can be split.
    if not all(
        call.op in partial_aggregates or call.op in decomposable_aggfuncs
        for call in node.aggregations.values()
    ):
        return node, True

    # Parse the join condition to identify the lists of equi-join keys
    # from the LHS and RHS, and verify that all of the columns used by
    # the condition are in those lists.
    lhs_keys, rhs_keys = extract_equijoin_keys(join)
    finder: ColumnReferenceFinder = ColumnReferenceFinder()
    for cond in join.conditions:
        cond.accept(finder)
    condition_cols: set[ColumnReference] = finder.get_column_references()
    if not all(col in lhs_keys or col in rhs_keys for col in condition_cols):
        return node, True

    # Identify which side of the join the aggfuncs refer to, and
    # make sure it is an INNER (+ there is only one side).
    finder.reset()
    for agg_call in node.aggregations.values():
        transpose_expression(agg_call, join.columns, True).accept(finder)
    agg_input_names: set[str | None] = {
        ref.input_name for ref in finder.get_column_references()
    }
    if len(agg_input_names) != 1:
        return node, True

    agg_input_name: str | None = agg_input_names.pop()
    agg_side: int = 0 if agg_input_name == join.default_input_aliases[0] else 1
    side_keys: list[ColumnReference] = (lhs_keys, rhs_keys)[agg_side]
    # Make sure the aggregate is being pushed into an INNER side.
    if agg_side == 1 and join.join_types[0] != JoinType.INNER:
        return node, True

    # If there are any AVG calls, rewrite the aggregate into
    # a call with SUM and COUNT derived, with a projection
    # dividing the two, then repeat the process.
    if any(call.op in decomposable_aggfuncs for call in node.aggregations.values()):
        return split_partial_aggregates(
            decompose_aggregations(node, config), config
        ), False

    # Otherwise, invoke the transposition procedure.
    return transpose_aggregate_join(node, join, agg_side, side_keys, config), True


def split_partial_aggregates(
    node: RelationalNode, config: PyDoughConfigs
) -> RelationalNode:
    """
    Splits partial aggregates above joins into two aggregates, one above the
    join and one below the join, from the entire relational plan rooted at the
    current node.

    Args:
        `node`: the root node of the relational plan to be transformed.
        `config`: the current configuration settings.

    Returns:
        The transformed node. The transformation is also done-in-place.
    """
    # If the aggregate+join pattern is detected, attempt to do the transpose.
    handle_inputs: bool = True
    if isinstance(node, Aggregate) and isinstance(node.input, Join):
        node, handle_inputs = attempt_join_aggregate_transpose(node, node.input, config)

    # If needed, recursively invoke the procedure on all inputs to the node.
    if handle_inputs:
        node = node.copy(
            inputs=[split_partial_aggregates(input, config) for input in node.inputs]
        )
    return node
