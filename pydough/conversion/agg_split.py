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
    ExpressionTranspositionShuttle,
    add_input_name,
    extract_equijoin_keys,
    fetch_or_insert,
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
    pydop.ANYTHING: (pydop.ANYTHING, pydop.ANYTHING),
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
        project_columns[name] = ColumnReference(name, expr.data_type)
    project_columns.update(
        {name: final_agg_columns[name] for name in node.aggregations}
    )
    return Project(new_aggregate, project_columns)


def transpose_aggregate_join(
    node: Aggregate,
    join: Join,
    agg_side: int,
    call_names: list[str],
    side_keys: list[ColumnReference],
    projection_columns: dict[str, RelationalExpression],
) -> tuple[bool, RelationalExpression | None]:
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
        Tuple of two values:
        1. Whether the aggregation transformation will require an additional
        projection on top of the final aggregate using the values in
        `projection_columns` (e.g. to wrap COUNT in DEFAULT_TO calls).
        2. A reference to the COUNT column if one was pushed down with no
        arguments (i.e. COUNT(*)), otherwise None.
    """
    count_ref: RelationalExpression | None = None
    agg_input_name: str | None = join.default_input_aliases[agg_side]
    need_projection: bool = False

    finder: ColumnReferenceFinder = ColumnReferenceFinder()
    transposer: ExpressionTranspositionShuttle = ExpressionTranspositionShuttle(
        join, False
    )

    # Calculate the aggregate terms to go above vs below the join.
    agg_input: RelationalNode = join.inputs[agg_side]
    top_aggs: dict[str, CallExpression] = {}
    input_aggs: dict[str, CallExpression] = {}
    for name, agg in node.aggregations.items():
        if name not in call_names:
            # If the aggregate is not in the list of calls to be pushed down,
            # it is not part of the join transpose, so skip it.
            top_aggs[name] = agg
            continue
        # Pick the name of the aggregate output column that
        # does not collide with an existing used name.
        bottom_name: str = name
        idx: int = 0
        while bottom_name in join.columns:
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
            join.join_type != JoinType.INNER or len(node.keys) == 0
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
            [arg.accept_shuttle(transposer) for arg in agg.inputs],
        )
        join.columns[bottom_name] = ColumnReference(
            bottom_name, agg.data_type, agg_input_name
        )
        if agg.op == pydop.COUNT and len(agg.inputs) == 0:
            count_ref = ColumnReference(name, agg.data_type)

    # Derive which columns are used as aggregate keys by
    # the input.
    input_keys: dict[str, RelationalExpression] = {}
    for ref in side_keys:
        input_keys[ref.name] = ref.with_input(None)
    transposer.toggle_keep_input_names(True)
    for agg_key_name, agg_key in node.keys.items():
        finder.reset()
        transposed_agg_key = agg_key.accept_shuttle(transposer)
        transposed_agg_key.accept(finder)
        if {col.input_name for col in finder.get_column_references()} == {
            agg_input_name
        }:
            if isinstance(transposed_agg_key, ColumnReference):
                input_keys[transposed_agg_key.name] = add_input_name(
                    transposed_agg_key, None
                )
            else:
                if agg_key_name in join.columns and (
                    agg_key_name in input_keys or agg_key_name in input_aggs
                ):
                    # An edge cases that is theoretically possible but never
                    # encountered so far, and where the behavior is undefined.
                    raise NotImplementedError("Undefined behavior")
                input_keys[agg_key_name] = add_input_name(transposed_agg_key, None)
                join.columns[agg_key_name] = ColumnReference(
                    agg_key_name, agg_key.data_type, agg_input_name
                )
                node.keys[agg_key_name] = ColumnReference(
                    agg_key_name, agg_key.data_type
                )
            projection_columns[agg_key_name] = ColumnReference(
                agg_key_name, agg_key.data_type
            )

    # Push the bottom-aggregate beneath the join
    join.inputs[agg_side] = Aggregate(agg_input, input_keys, input_aggs)

    # Replace the aggregation above the join with the top
    # side of the aggregations
    node._aggregations = top_aggs
    node._columns = {**node.keys, **top_aggs}

    return need_projection, count_ref


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
    agg_input_names: set[str | None]

    if len(join.inputs) != 2:
        # If the join does not have exactly two inputs, we cannot
        # push the aggregate down.
        return node, True

    # Verify that all of the aggregation keys strictly come from one side of the
    # join.
    finder: ColumnReferenceFinder = ColumnReferenceFinder()
    for key_expr in node.keys.values():
        finder.reset()
        key_expr.accept(finder)
        if len({ref.input_name for ref in finder.get_column_references()}) > 1:
            return node, True

    # Break down the aggregation calls by which input they refer to.
    lhs_aggs: list[str] = []
    rhs_aggs: list[str] = []
    count_aggs: list[str] = []
    transposer: ExpressionTranspositionShuttle = ExpressionTranspositionShuttle(
        join, True
    )
    for agg_name, agg_call in node.aggregations.items():
        finder.reset()
        agg_call.accept_shuttle(transposer).accept(finder)
        agg_input_names = {ref.input_name for ref in finder.get_column_references()}
        if len(agg_input_names) == 0:
            if agg_call.op == pydop.COUNT:
                # If the aggregate does not refer to any input, it is a
                # COUNT(*) and can be pushed down via splitting.
                count_aggs.append(agg_name)
            else:
                # Otherwise, the aggregation is malformed and cannot be pushed.
                return node, True
        elif len(agg_input_names) > 1:
            # If the aggregate refers to multiple inputs, it cannot be pushed.
            return node, True
        else:
            agg_input_name: str | None = agg_input_names.pop()
            if agg_input_name == join.default_input_aliases[0]:
                # If the aggregate refers to the first input, it is a LHS aggregate.
                lhs_aggs.append(agg_name)
            elif agg_input_name == join.default_input_aliases[1]:
                # Otherwise, it is a RHS aggregate.
                rhs_aggs.append(agg_name)
            else:
                return node, True

    need_count_aggs: bool = len(count_aggs) > 0
    can_push_left: bool = len(lhs_aggs) > 0 or need_count_aggs
    can_push_right: bool = len(rhs_aggs) > 0 or need_count_aggs
    # If the join is not INNER, we cannot push the aggregate down into the
    # right side.
    if join.join_type != JoinType.INNER:
        can_push_right = False

    # Optimization: don't push down aggregates into the inputs of a join
    # if joining first will reduce the number of rows that get aggregated.
    if join.cardinality.filters:
        can_push_left = False
        can_push_right = False

    # If any of the aggregations to either side cannot be pushed down, then
    # we cannot perform the transpose on that side.
    for agg_name in lhs_aggs:
        lhs_op: pydop.PyDoughExpressionOperator = node.aggregations[agg_name].op
        if lhs_op not in partial_aggregates and lhs_op not in decomposable_aggfuncs:
            can_push_left = False
            break
    for agg_name in rhs_aggs:
        rhs_op: pydop.PyDoughExpressionOperator = node.aggregations[agg_name].op
        if rhs_op not in partial_aggregates and rhs_op not in decomposable_aggfuncs:
            can_push_right = False
            break

    if not can_push_left and not can_push_right:
        # If we cannot push the aggregate down into either side, we cannot
        # perform the transpose.
        return node, True

    if need_count_aggs and not (can_push_left and can_push_right):
        # If we need to push down COUNT(*) aggregates, but cannot push into
        # both sides of the join, we cannot perform the transpose.
        return node, True

    # Parse the join condition to identify the lists of equi-join keys
    # from the LHS and RHS, and verify that all of the columns used by
    # the condition are in those lists.
    lhs_keys, rhs_keys = extract_equijoin_keys(join)
    finder.reset()
    join.condition.accept(finder)
    condition_cols: set[ColumnReference] = finder.get_column_references()
    if not all(col in lhs_keys or col in rhs_keys for col in condition_cols):
        return node, True

    # If there are any AVG calls, rewrite the aggregate into
    # a call with SUM and COUNT derived, with a projection
    # dividing the two, then repeat the process.
    for col in node.aggregations.values():
        if col.op in decomposable_aggfuncs:
            return split_partial_aggregates(
                decompose_aggregations(node, config), config
            ), False

    # Keep a dictionary for the projection columns that will be used to post-process
    # the output of the aggregates, if needed.
    projection_columns: dict[str, RelationalExpression] = {}
    need_projection: bool = False

    # If we need count aggregates, add one to each side of the join.
    if need_count_aggs:
        assert len(count_aggs) > 0
        lhs_aggs.append(count_aggs.pop())
        new_agg_name: str
        idx: int = 0
        while True:
            new_agg_name = f"agg_{idx}"
            if new_agg_name not in node.columns:
                break
            idx += 1
        node.aggregations[new_agg_name] = CallExpression(
            pydop.COUNT,
            NumericType(),
            [],
        )
        rhs_aggs.append(new_agg_name)

    # Loop over both inputs and perform the pushdown into whichever one(s)
    # will allow an aggregate to be pushed into them.
    count_refs: list[RelationalExpression] = []
    for agg_side in range(2):
        can_push: bool = (can_push_left, can_push_right)[agg_side]
        if not can_push:
            # If we cannot push the aggregate down into this side, skip it.
            continue
        side_keys: list[ColumnReference] = (lhs_keys, rhs_keys)[agg_side]
        side_call_names: list[str] = (lhs_aggs, rhs_aggs)[agg_side]
        side_needs_projection, side_count_ref = transpose_aggregate_join(
            node,
            join,
            agg_side,
            side_call_names,
            side_keys,
            projection_columns,
        )
        if side_needs_projection:
            need_projection = True
        if side_count_ref is not None:
            count_refs.append(side_count_ref)

    # For each COUNT(*) aggregate, replace with the product of the COUNT(*)
    # calls that were pushed into each side of the join.
    for count_call_name in count_aggs:
        assert len(count_refs) > 1
        product: RelationalExpression = CallExpression(
            pydop.MUL, NumericType(), count_refs
        )
        projection_columns[count_call_name] = product
        need_projection = True

    # If the node requires projection at the end, create a new Project node on
    # top of the top aggregate.
    if need_projection:
        return Project(node, projection_columns), True
    else:
        return node, True


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
