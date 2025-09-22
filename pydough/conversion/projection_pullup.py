"""
Logic used to pull up projections in the relational plan so function calls
happen as late as possible, ideally after filters, filtering joins, and
aggregations.
"""

__all__ = ["pullup_projections"]


import pydough.pydough_operators as pydop
from pydough.relational import (
    Aggregate,
    CallExpression,
    ColumnReference,
    ExpressionSortInfo,
    Filter,
    Join,
    JoinType,
    Limit,
    LiteralExpression,
    Project,
    RelationalExpression,
    RelationalNode,
    RelationalRoot,
    RelationalShuttle,
)
from pydough.relational.rel_util import (
    ExpressionTranspositionShuttle,
    add_input_name,
    apply_substitution,
    contains_window,
)
from pydough.types import BooleanType, NumericType

from .merge_projects import merge_adjacent_projects


def widen_columns(
    node: RelationalNode,
) -> dict[RelationalExpression, RelationalExpression]:
    """
    Modifies a relational node in-place to ensure every column in the node's
    inputs is also present in the node's output columns. Returns a substitution
    mapping such that any expression pulled into the parent of the node can be
    transformed to point to the node's output columns.

    Args:
        `node`: The relational node to "widen" by adding more columns to.

    Returns:
        A mapping that can be used for substitution if expressions from the
        node are pulled up into the parent of the node.
    """
    # The substitution mapping that will be built by the functions and returned
    # to the calling site.
    substitutions: dict[RelationalExpression, RelationalExpression] = {}

    # Mapping of every expression in the input nodes columns to a reference to
    # the column of the node that points to it. This is used to keep track of
    # which expressions are already present in the node's columns versus the
    # ones that should be added to un-prune the node.
    existing_vals: dict[RelationalExpression, RelationalExpression] = {
        expr: ColumnReference(name, expr.data_type)
        for name, expr in node.columns.items()
    }

    # Pull all the columns from each input to the node into the node's output
    # columns if they are not already in the node's output columns. Make sure
    # not to include no-op mappings.
    for input_idx in range(len(node.inputs)):
        input_alias: str | None = node.default_input_aliases[input_idx]
        input_node: RelationalNode = node.inputs[input_idx]
        for name, expr in input_node.columns.items():
            ref_expr: RelationalExpression = ColumnReference(
                name, expr.data_type, input_name=input_alias
            )

            # If the expression is not already in the node's columns, then
            # inject it so the node can use it later if a pull-up occurs that
            # would need to reference this expression.
            if ref_expr not in existing_vals:
                new_name: str = name
                idx: int = 0
                while new_name in node.columns:
                    idx += 1
                    new_name = f"{name}_{idx}"
                new_ref: ColumnReference = ColumnReference(new_name, expr.data_type)
                node.columns[new_name] = ref_expr
                existing_vals[ref_expr] = new_ref
                substitutions[ref_expr] = new_ref
            else:
                substitutions[ref_expr] = existing_vals[ref_expr]

    # Return the substitution mapping
    return substitutions


def pull_non_columns(node: Join | Filter | Limit) -> RelationalNode:
    """
    Pulls up non-column expressions from the output columns of a Join, Filter,
    or Limit node into a parent projection.

    Args:
        `node`: The Join, Filter, or Limit node to pull up non-column expressions
            from.

    Returns:
        Either the original node if this rewrite is not applicable, or a
        project node that contains the non-column expressions pulled up from
        the output columns of the node, pointing to `node` as its input.
    """
    # The columns that will be used in the parent projection.
    new_project_columns: dict[str, RelationalExpression] = {}

    # A boolean to indicate if any columns were pulled besides no-ops. If this
    # never becomes true, then we skip the rewrite and return the original.
    needs_pull: bool = False

    # Iterate through the columns of the node and check if they are column
    # references or not. If they are not, then we need to pull them up into
    # the parent projection.
    for name, expr in node.columns.items():
        if isinstance(expr, ColumnReference):
            new_project_columns[name] = ColumnReference(name, expr.data_type)
        else:
            new_project_columns[name] = expr
            needs_pull = True

    # Skip the rewrite if no columns were pulled up.
    if not needs_pull:
        return node

    # Ensure every column in the node's inputs is also present in the output
    # columns of the node. This will ensure that any function calls that are
    # pulled into a parent projection can have their inputs substituted with
    # references to the node's output columns. Ensure the substitutions do not
    # have any input names in the values.
    substitutions: dict[RelationalExpression, RelationalExpression] = widen_columns(
        node
    )
    substitutions = {k: add_input_name(v, None) for k, v in substitutions.items()}

    # Create the columns of the new projection by applying the substitutions
    # to the expressions pulled up earlier.
    for name, expr in new_project_columns.items():
        new_project_columns[name] = apply_substitution(expr, substitutions, {})

    # Build the new project node pointing to the input but with the new columns.
    return Project(input=node, columns=new_project_columns)


def pull_project_helper(
    project: Project,
    input_name: str | None,
) -> dict[RelationalExpression, RelationalExpression]:
    """
    Main helper utility for pulling up columns from a Project node into a
    a parent Filter/Join/Limit/Aggregate node. This function modifies the input
    project in-place to ensure every column in the project's inputs is available
    to the parent node, and returns a mapping of expressions that can be used
    to substitute the columns in the parent node's output columns or conditions.

    Args:
        `project`: The Project node to pull columns from.
        `input_name`: The name of the input to the parent node that the project
            node is connected to. This is used to add input names to the
            expressions pulled from the project node when dealing with joins.

    Returns:
        A mapping of expressions that can be used to substitute the columns in
        the parent node's output columns or conditions. This mapping will
        ensure columns are only pulled up if they do not contain window
        functions.
    """
    # Ensure every column in the project's inputs is also present in the output
    # columns of the project. This will ensure that any function calls that are
    # pulled into the parent can have their inputs substituted with references
    # to columns from the project.
    transfer_substitutions: dict[RelationalExpression, RelationalExpression] = (
        widen_columns(project)
    )

    # Iterate through the columns of the project to see which ones can be
    # pulled up into the parent, dding them to a substitutions mapping that
    # will be used to apply the transformations.
    substitutions: dict[RelationalExpression, RelationalExpression] = {}
    for name, expr in project.columns.items():
        new_expr: RelationalExpression = add_input_name(
            apply_substitution(expr, transfer_substitutions, {}), input_name
        )
        if not contains_window(new_expr):
            ref_expr: ColumnReference = ColumnReference(
                name, expr.data_type, input_name=input_name
            )
            substitutions[ref_expr] = new_expr
    return substitutions


def pull_project_into_join(node: Join, input_index: int) -> None:
    """
    Attempts to pull columns from a Project node that is an input to a Join
    into the output columns of the Join node, and into its join condition.
    This transformation is done in-place.

    Args:
        `node`: The Join node to pull the Project columns into.
        `input_index`: The index of the input to the Join node that should have
        its columns pulled up, if it is a project node. This is assumed to be
        either 0 (for the LHS) or 1 (for the RHS).
    """

    # Skip if the input at the specified input is not a Project node.
    if not isinstance(node.inputs[input_index], Project):
        return
    project = node.inputs[input_index]
    assert isinstance(project, Project)

    # Invoke the common helper for Join/Filter/Limit/Aggregate to identify
    # which columns from the project can be pulled up into the join's output
    # columns or condition, and modifies the project node in-place to ensure
    # every column in the project's inputs is available to the current node.
    substitutions: dict[RelationalExpression, RelationalExpression] = (
        pull_project_helper(project, node.default_input_aliases[input_index])
    )

    # Apply the substitutions to the join's condition and output columns.
    node._condition = apply_substitution(node.condition, substitutions, {})
    node._columns = {
        name: apply_substitution(expr, substitutions, {})
        for name, expr in node.columns.items()
    }


def pull_project_into_filter(node: Filter) -> None:
    """
    Attempts to pull columns from a Project node that is an input to a Filter
    into the output columns of the Filter node, and into the filter condition.
    This transformation is done in-place.

    Args:
        `node`: The Filter node to pull the Project columns into.
    """

    # Skip if the filter's input is not a Project node.
    if not isinstance(node.input, Project):
        return

    # Invoke the common helper for Join/Filter/Limit/Aggregate to identify
    # which columns from the project can be pulled up into the filter's output
    # columns or condition, and modifies the project node in-place to ensure
    # every column in the project's inputs is available to the current node.
    substitutions: dict[RelationalExpression, RelationalExpression] = (
        pull_project_helper(node.input, None)
    )

    # Apply the substitutions to the filter's condition and output columns.
    node._condition = apply_substitution(node.condition, substitutions, {})
    node._columns = {
        name: apply_substitution(expr, substitutions, {})
        for name, expr in node.columns.items()
    }


def pull_project_into_limit(node: Limit) -> None:
    """
    Attempts to pull columns from a Project node that is an input to a Limit
    into the output columns of the Limit node, and into the ordering
    expressions. This transformation is done in-place.

    Args:
        `node`: The Limit node to pull the Project columns into.
    """

    # Skip if the limit's input is not a Project node.
    if not isinstance(node.input, Project):
        return

    # Invoke the common helper for Join/Filter/Limit/Aggregate to identify
    # which columns from the project can be pulled up into the limit's output
    # columns or orderings, and modifies the project node in-place to ensure
    # every column in the project's inputs is available to the current node.
    substitutions: dict[RelationalExpression, RelationalExpression] = (
        pull_project_helper(node.input, None)
    )

    # Apply the substitutions to the limit's orderings and output columns.
    node._columns = {
        name: apply_substitution(expr, substitutions, {})
        for name, expr in node.columns.items()
    }
    node._orderings = [
        ExpressionSortInfo(
            apply_substitution(order_expr.expr, substitutions, {}),
            order_expr.ascending,
            order_expr.nulls_first,
        )
        for order_expr in node.orderings
    ]


def simplify_agg(
    keys: dict[str, RelationalExpression], agg: CallExpression, name: str
) -> tuple[RelationalExpression, CallExpression | None]:
    """
    Simplifies an aggregation call by checking if the combination of the
    function versus its inputs can be rewritten in another form. The rewrite
    allows expressions to be done after aggregation since there will be a
    parent projection on top of the aggregate.

    Args:
        `keys`: The keys of the aggregation, used for simplifications when an
            aggregation function is called on a key.
        `agg`: The aggregation call to simplify.
        `name`: The name of the aggregation, used to build a reference in the
            parent project node to the output of the aggregation.

    Returns:
        A tuple containing two terms:
        - The first term is the output expression that should be used in the
          parent project node to refer to the final result of the aggregation
          after any post-processing is done. This may contain a reference to
          column `name` of the aggregation.
        - The second term is the aggregation call that should be referred to
          by the parent project when deriving the final answer. If this is
          `None`, then the output expression can be derived entirely in the
          project and does not require an aggregation call.
    """
    arg: RelationalExpression

    # Build a mapping from every key expression to its name.
    reverse_keys: dict[RelationalExpression, str] = {
        expr: name for name, expr in keys.items()
    }

    # Commonly used terms:
    # - Reference to the output of the aggregation
    # - Literal 0
    # - Literal 1
    # - COUNT(*) call
    out_ref: RelationalExpression = ColumnReference(name, agg.data_type)
    zero_expr: RelationalExpression = LiteralExpression(0, agg.data_type)
    one_expr: RelationalExpression = LiteralExpression(1, agg.data_type)
    count_star: CallExpression = CallExpression(
        op=pydop.COUNT,
        return_type=NumericType(),
        inputs=[],
    )

    # Can optimize SUM, COUNT and NDISTINCT aggregations on literals.
    if (
        agg.op in (pydop.SUM, pydop.COUNT, pydop.NDISTINCT)
        and len(agg.inputs) == 1
        and isinstance(agg.inputs[0], LiteralExpression)
    ):
        arg = agg.inputs[0]
        if agg.op == pydop.SUM and (
            isinstance(arg.data_type, NumericType) or arg.value is None
        ):
            # SUM(NULL) -> NULL
            if arg.value is None:
                return arg, None

            # SUM(0) -> 0
            elif arg.value == 0:
                return zero_expr, None

            # SUM(1) -> COUNT(*)
            # SUM(n) = COUNT(*) * n
            elif arg.value != 1:
                out_ref = CallExpression(
                    op=pydop.MUL,
                    return_type=agg.data_type,
                    inputs=[out_ref, LiteralExpression(arg.value, agg.data_type)],
                )
            return out_ref, count_star

        elif agg.op == pydop.COUNT:
            # COUNT(NULL) -> 0
            if arg.value is None:
                return zero_expr, None

            # COUNT(n) -> COUNT(*)
            else:
                return out_ref, count_star

        elif agg.op == pydop.NDISTINCT:
            # NDISTINCT(NULL) -> 0
            # NDISTINCT(n) -> 1
            return zero_expr if arg.value is None else one_expr, None

    # SUM(DEFAULT_TO(x, 0)) -> DEFAULT_TO(SUM(x), 0)
    if (
        agg.op == pydop.SUM
        and len(agg.inputs) == 1
        and isinstance(agg.inputs[0], CallExpression)
        and agg.inputs[0].op == pydop.DEFAULT_TO
        and isinstance(agg.inputs[0].inputs[1], LiteralExpression)
        and isinstance(agg.inputs[0].inputs[1].data_type, NumericType)
        and agg.inputs[0].inputs[1].value == 0
    ):
        return CallExpression(
            pydop.DEFAULT_TO, agg.data_type, [out_ref, zero_expr]
        ), CallExpression(pydop.SUM, agg.data_type, [agg.inputs[0].inputs[0]])

    # For many aggregations, if the argument is a key, we can just use the key.
    if (
        agg.op
        in (
            pydop.SUM,
            pydop.MIN,
            pydop.MAX,
            pydop.ANYTHING,
            pydop.AVG,
            pydop.QUANTILE,
            pydop.MEDIAN,
            pydop.COUNT,
            pydop.NDISTINCT,
        )
        and len(agg.inputs) >= 1
    ):
        arg = agg.inputs[0]
        if arg in reverse_keys:
            # Reference to the key from the perspective of the project.
            key_ref: RelationalExpression = ColumnReference(
                reverse_keys[arg], agg.data_type
            )

            # COUNT(key) -> COUNT(*) * INTEGER(PRESENT(key))
            if agg.op == pydop.COUNT:
                return CallExpression(
                    pydop.MUL,
                    agg.data_type,
                    [
                        out_ref,
                        CallExpression(
                            pydop.IFF,
                            NumericType(),
                            [
                                CallExpression(pydop.PRESENT, BooleanType(), [key_ref]),
                                one_expr,
                                zero_expr,
                            ],
                        ),
                    ],
                ), count_star

            # NDISTINCT(key) -> INTEGER(PRESENT(key))
            if agg.op == pydop.NDISTINCT:
                return CallExpression(
                    pydop.INTEGER,
                    NumericType(),
                    [CallExpression(pydop.PRESENT, BooleanType(), [key_ref])],
                ), None

            # Otherwise, FUNC(key) -> key
            return key_ref, None

    # If running a selection aggregation on a literal or a grouping key, can
    # just return the input.
    if (
        agg.op
        in (
            pydop.MIN,
            pydop.MAX,
            pydop.ANYTHING,
            pydop.AVG,
            pydop.MEDIAN,
            pydop.QUANTILE,
        )
        and len(agg.inputs) >= 1
    ):
        arg = agg.inputs[0]
        if isinstance(arg, LiteralExpression) or arg in reverse_keys:
            return arg, None
    # In all other cases, we just return the aggregation as is.
    return out_ref, agg


def pull_project_into_aggregate(node: Aggregate) -> Aggregate:
    """
    Attempts to pull columns from a Project node that is an input to an
    Aggregate into the inputs of the aggregation calls of the Aggregate, and
    into the grouping keys. Additionally, simplifies the aggregation calls when
    possible. This transformation is done in-place.

    Args:
        `node`: The Aggregate node to pull the Project columns into.

    Returns:
        The transformed version of the Aggregate node.
    """
    if not isinstance(node.input, Project):
        return node

    # Invoke the common helper for Join/Filter/Limit/Aggregate to identify
    # which columns from the project can be pulled up into the aggregation's
    # keys or used as inputs to its aggregation calls, and modifies the project
    # node in-place to ensure every column in the project's inputs is available
    # to the current node.
    substitutions: dict[RelationalExpression, RelationalExpression] = (
        pull_project_helper(node.input, None)
    )

    # Apply the substitutions to the keys and aggregations of the aggregate.
    new_keys: dict[str, RelationalExpression] = {
        name: apply_substitution(expr, substitutions, {})
        for name, expr in node.keys.items()
    }

    # Apply the substitutions to the aggregation calls of the aggregate,
    # then try to simplify them, before updating the `new_columns`.
    new_aggs: dict[str, CallExpression] = {}
    for name, expr in node.aggregations.items():
        new_expr = apply_substitution(expr, substitutions, {})
        assert isinstance(new_expr, CallExpression)
        new_aggs[name] = new_expr

    return Aggregate(
        input=node.input,
        keys=new_keys,
        aggregations=new_aggs,
    )


def transform_aggregations(node: Aggregate) -> RelationalNode:
    """
    Transforms an Aggregate node by running various simplifications on its
    aggregation calls, potentially creating compound expressions in a parent
    projection.

    Args:
        `node`: The Aggregate node to transform.

    Returns:
        Either the original node if no transformations were applicable, or a
        new relational node that contains the transformed aggregation logic.
    """

    # Build up the columns of a new project that points to all of the output
    # columns of the aggregate. Start with just the keys, since the aggs will
    # be added later.
    new_columns: dict[str, RelationalExpression] = {
        name: ColumnReference(name, expr.data_type) for name, expr in node.keys.items()
    }

    # For every aggregation, try to simplify it, potentially adding new columns
    # to the parent project.
    new_aggs: dict[str, CallExpression] = {}
    out_expr: RelationalExpression
    new_agg_expr: CallExpression | None
    for name, expr in node.aggregations.items():
        # Simplify agg returns the value used in the project to store the
        # answer, and the aggregation value used to derive it (if needed). If
        # the aggregation value is None, then it means the aggregation was
        # simplified in a way that could be derived entirely in the project.
        # Otherwise, the aggregation value is referenced in the project via
        # a reference to `name`.
        out_expr, new_agg_expr = simplify_agg(node.keys, expr, name)
        new_columns[name] = out_expr
        if new_agg_expr is not None:
            new_aggs[name] = new_agg_expr

    # Build the new aggregation with the new keys/aggs, then wrap the new
    # project around it. The new project is required in case `simplify_agg`
    # returned any `output_expr` values that post-process the aggregation
    # results, e.g. replacing `SUM(3)` with `3 * COUNT(*)`, or `MIN(key)` with
    # `key`.
    agg: Aggregate = Aggregate(
        input=node.input,
        keys=node.keys,
        aggregations=new_aggs,
    )
    return merge_adjacent_projects(Project(input=agg, columns=new_columns))


def merge_adjacent_aggregations(node: Aggregate) -> Aggregate:
    """
    Attempts to merge two adjacent Aggregate nodes into a single Aggregate
    node.

    Args:
        `node`: The Aggregate node to merge with its input.

    Returns:
        Either the original node if the merge is not applicable, or a new
        Aggregate node that uses the keys of the top aggregate node, but
        modifies the aggregations to not require the original input round of
        aggregation.
    """

    # Skip if the input to the node is not an Aggregate.
    if not isinstance(node.input, Aggregate):
        return node

    input_agg: Aggregate = node.input
    transposer: ExpressionTranspositionShuttle = ExpressionTranspositionShuttle(
        input_agg, False
    )

    # Identify all of the keys in the top vs bottom aggregations, transposing
    # the top keys so they can be expressed in the same terms as the bottom
    # keys.
    top_keys: set[RelationalExpression] = {
        expr.accept_shuttle(transposer) for expr in node.keys.values()
    }
    bottom_keys: set[RelationalExpression] = set(input_agg.keys.values())

    # If there are any top keys that are not present in the bottom keys,
    # then the merge fails.
    if len(top_keys - bottom_keys) > 0:
        return node

    # Identify any bottom keys that are not present in the top keys. This is
    # needed for situations with COUNT(*) in the top aggregation.
    bottom_only_keys: set[RelationalExpression] = bottom_keys - top_keys

    # Iterate across all of the aggregations in the top Aggregate node and
    # transform each of them, building the result in `new_aggs`. If any of them
    # cannot be transformed, then the merge fails and we return the original
    # node.
    new_aggs: dict[str, CallExpression] = {}
    input_expr: RelationalExpression
    for agg_name, agg_expr in node.aggregations.items():
        match agg_expr.op:
            case pydop.COUNT if len(agg_expr.inputs) == 0:
                # top_keys: {x, y}
                # bottom_keys: {x, y}
                # COUNT(*) -> ANYTHING(1)
                if len(bottom_only_keys) == 0:
                    new_aggs[agg_name] = CallExpression(
                        op=pydop.ANYTHING,
                        return_type=agg_expr.data_type,
                        inputs=[LiteralExpression(1, agg_expr.data_type)],
                    )

                # top_keys: {x, y}
                # bottom_keys: {x, y, z}
                # COUNT(*) -> NDISTINCT(z)
                elif len(bottom_only_keys) == 1:
                    new_aggs[agg_name] = CallExpression(
                        op=pydop.NDISTINCT,
                        return_type=agg_expr.data_type,
                        inputs=[next(iter(bottom_only_keys))],
                    )

                # Otherwise, the merge fails.
                else:
                    return node

            case pydop.SUM:
                # SUM(SUM(x)) -> SUM(x)
                # SUM(COUNT(x)) -> COUNT(x)
                input_expr = agg_expr.inputs[0].accept_shuttle(transposer)
                if isinstance(input_expr, CallExpression) and input_expr.op in (
                    pydop.SUM,
                    pydop.COUNT,
                ):
                    new_aggs[agg_name] = input_expr

                # Otherwise, the merge fails.
                else:
                    return node

            case pydop.MIN | pydop.MAX | pydop.ANYTHING:
                # MIN(MIN(x)) -> MIN(x)
                # MIN(ANYTHING(x)) -> MIN(x)
                # MAX(MAX(x)) -> MAX(x)
                # MAX(ANYTHING(x)) -> MAX(x)
                # ANYTHING(ANYTHING(x)) -> ANYTHING(x)
                input_expr = agg_expr.inputs[0].accept_shuttle(transposer)
                if isinstance(input_expr, CallExpression) and input_expr.op in (
                    agg_expr.op,
                    pydop.ANYTHING,
                ):
                    new_aggs[agg_name] = input_expr

                # Otherwise, the merge fails.
                else:
                    return node

            # Otherwise, the merge fails.
            case _:
                return node

    # If none of the aggregations caused a merge failure, we can return a new
    # Aggregate node using the top keys and the merged aggregation calls.
    new_keys: dict[str, RelationalExpression] = {
        name: expr.accept_shuttle(transposer) for name, expr in node.keys.items()
    }
    return Aggregate(
        input=input_agg.input,
        keys=new_keys,
        aggregations=new_aggs,
    )


class ProjectionPullupShuttle(RelationalShuttle):
    """
    Relational shuttle that performs projection pull-up on a relational node and
    its inputs, ensuring that expression calculations, such as function calls,
    are done as late as possible in the plan. This is done by pulling up
    projections from the inputs of the node into the node itself, and then
    attempting to eject such expressions from the current node into its parent,
    while also combining adjacent nodes when possible. Several forms of
    simplification involving aggregation calls are also performed at this stage.
    """

    def visit_project(self, node: Project) -> RelationalNode:
        # Attempt to squish the project with its input, if possible.
        new_node = self.generic_visit_inputs(node)
        assert isinstance(new_node, Project)
        return merge_adjacent_projects(new_node)

    def visit_root(self, node: RelationalRoot) -> RelationalNode:
        # Attempt to squish the root with its input, if possible.
        new_node = self.generic_visit_inputs(node)
        assert isinstance(new_node, RelationalRoot)
        return merge_adjacent_projects(new_node)

    def visit_join(self, node: Join) -> RelationalNode:
        # For Join nodes, pull projections from the left input (also the right
        # for INNER joins), then eject the non-column expressions
        # into a parent projection.
        new_node = self.generic_visit_inputs(node)
        assert isinstance(new_node, Join)
        pull_project_into_join(new_node, 0)
        if new_node.join_type == JoinType.INNER:
            pull_project_into_join(new_node, 1)
        return pull_non_columns(new_node)

    def visit_filter(self, node: Filter) -> RelationalNode:
        # For Filter nodes, pull projections into the filter's condition and
        # output columns, then eject the non-column expressions into a parent
        # projection.
        new_node = self.generic_visit_inputs(node)
        assert isinstance(new_node, Filter)
        pull_project_into_filter(new_node)
        return pull_non_columns(new_node)

    def visit_limit(self, node: Limit) -> RelationalNode:
        # For Limit nodes, pull projections into the limit's orderings and
        # output columns, then eject the non-column expressions into a parent
        # projection.
        new_node = self.generic_visit_inputs(node)
        assert isinstance(new_node, Limit)
        pull_project_into_limit(new_node)
        return pull_non_columns(new_node)

    def visit_aggregate(self, node: Aggregate) -> RelationalNode:
        # For Aggregate nodes, pull projections into the aggregation keys and
        # aggregations (also simplifying aggregate calls when possible), then
        # merge adjacent aggregations if possible.
        new_node = self.generic_visit_inputs(node)
        assert isinstance(new_node, Aggregate)
        new_node = merge_adjacent_aggregations(new_node)
        new_node = pull_project_into_aggregate(new_node)
        return transform_aggregations(new_node)


def pullup_projections(node: RelationalNode) -> RelationalNode:
    """
    Perform projection pull-up on a relational node and its inputs, ensuring
    that expression calculations, such as function calls, are done as late as
    possible in the plan.

    Args:
        `node`: The relational node to pull projections up from.

    Returns:
        The transformed node with projections pulled up on it and all of its
        descendants.
    """
    pullup_shuttle: ProjectionPullupShuttle = ProjectionPullupShuttle()
    return node.accept_shuttle(pullup_shuttle)
