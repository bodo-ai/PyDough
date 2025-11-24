"""
Logical plan transformation to pull aggregates above joins when possible for
optimization purposes.
"""

__all__ = ["pull_aggregates_above_joins"]


from collections.abc import Iterable

import pydough.pydough_operators as pydop
from pydough.relational import (
    Aggregate,
    CallExpression,
    ColumnReference,
    ColumnReferenceFinder,
    Join,
    JoinCardinality,
    JoinType,
    LiteralExpression,
    Project,
    RelationalExpression,
    RelationalNode,
    RelationalRoot,
    RelationalShuttle,
)
from pydough.relational.rel_util import (
    add_input_name,
    apply_substitution,
    extract_equijoin_keys,
)
from pydough.types import BooleanType, NumericType


class JoinAggregateTransposeShuttle(RelationalShuttle):
    """
    Relational shuttle Transposes joins and aggregates in the relational
    algebra, moving the currently aggregate underneath the join to be above
    the join instead for performance gains.
    """

    left_join_case_ops = {
        pydop.COUNT,
        pydop.MIN,
        pydop.MAX,
        pydop.SUM,
        pydop.ANYTHING,
        pydop.MEDIAN,
        pydop.QUANTILE,
        pydop.SAMPLE_VAR,
        pydop.SAMPLE_STD,
        pydop.POPULATION_VAR,
        pydop.POPULATION_STD,
    }
    """
    The set of aggregation operators that are safe transpose under a LEFT JOIN
    when the aggregate is on the right side of the join.
    """

    def __init__(self):
        self.finder: ColumnReferenceFinder = ColumnReferenceFinder()

    def reset(self):
        self.finder.reset()

    def visit_join(self, node: Join) -> RelationalNode:
        result: RelationalNode | None = None

        # Attempt the transpose where the left input is an Aggregate. If it
        # succeeded, use that as the result and recursively transform its
        # inputs.
        if isinstance(node.inputs[0], Aggregate):
            result = self.join_aggregate_transpose(node, node.inputs[0], True)
            if result is not None:
                return result.accept_shuttle(self)

        # If the attempt failed, then attempt the transpose where the right
        # input is an Aggregate. If this attempt succeeded, use that as the
        # result and recursively transform its inputs.
        if isinstance(node.inputs[1], Aggregate):
            result = self.join_aggregate_transpose(node, node.inputs[1], False)
            if result is not None:
                return result.accept_shuttle(self)

        # If this attempt failed, fall back to the regular implementation.
        return super().visit_join(node)

    def generate_name(self, base: str, used_names: Iterable[str]) -> str:
        """
        Generates a new name for a column based on the base name and the existing
        columns in the join. This is used to ensure that the new column names are
        unique and do not conflict with existing names.
        """
        if base not in used_names:
            return base
        i: int = 0
        while True:
            name: str = f"{base}_{i}"
            if name not in used_names:
                return name
            i += 1

    def join_aggregate_transpose(
        self, join: Join, aggregate: Aggregate, is_left_agg: bool
    ) -> RelationalNode | None:
        """
        Transposes a Join above an Aggregate into an Aggregate above a Join,
        when possible and it would be better for performance to use the join
        first to filter some of the rows before aggregating.

        Args:
            `join`: the Join node above the Aggregate.
            `aggregate`: the Aggregate node that is the left input to the Join.
            `is_left_agg`: whether the Aggregate is the left input to the Join
            (True) or the right input (False).

        Returns:
            The new RelationalNode tree with the Join and Aggregate transposed,
            or None if the transpose is not possible.
        """
        join_name: str
        agg_name: str

        # The cardinality with regards to the input being considered must be
        # singular (unless the aggregations allow plural), and must be
        # filtering (since the point of joining before aggregation is to reduce
        # the number of rows to aggregate).
        cardinality: JoinCardinality = (
            join.cardinality if is_left_agg else join.reverse_cardinality
        )

        left_join_case: bool = (
            join.join_type == JoinType.LEFT
            and not is_left_agg
            and all(
                agg.op in JoinAggregateTransposeShuttle.left_join_case_ops
                for agg in aggregate.aggregations.values()
            )
        )

        # Verify the cardinality meets the specified criteria, and that the join
        # type is INNER/SEMI (since LEFT would not be filtering), where SEMI is
        # only allowed if the aggregation is on the left.
        if not (
            (
                (join.join_type == JoinType.INNER)
                or (join.join_type == JoinType.SEMI and is_left_agg)
                or left_join_case
            )
            and cardinality.filters
            and cardinality.singular
        ):
            return None

        # The alias of the input to the join that corresponds to the
        # aggregate.
        desired_alias: str | None = (
            join.default_input_aliases[0]
            if is_left_agg
            else join.default_input_aliases[1]
        )

        # Find all of the columns used in the join condition that come from the
        # aggregate side of the join, and the other side as well.
        self.finder.reset()
        join.condition.accept(self.finder)
        agg_condition_columns: set[ColumnReference] = {
            col
            for col in self.finder.get_column_references()
            if col.input_name == desired_alias
        }

        # Verify ALL of the condition columns from that side of the join are
        # in the aggregate keys.
        if len(agg_condition_columns) == 0 or any(
            col.name not in aggregate.keys for col in agg_condition_columns
        ):
            return None

        # Extract the join key references from both sides of the join in the
        # order they appear in the join condition.
        agg_key_refs: list[ColumnReference]
        non_agg_key_refs: list[ColumnReference]
        agg_key_refs, non_agg_key_refs = extract_equijoin_keys(join)
        if not is_left_agg:
            agg_key_refs, non_agg_key_refs = non_agg_key_refs, agg_key_refs

        # Obtain the input aliases for both sides of the join, identified with
        # which one belongs to the aggregate versus the other input.
        agg_alias: str | None = (
            join.default_input_aliases[0]
            if is_left_agg
            else join.default_input_aliases[1]
        )
        non_agg_alias: str | None = (
            join.default_input_aliases[1]
            if is_left_agg
            else join.default_input_aliases[0]
        )

        # Now that the transpose is deemed possible, if in the left join
        # scenario, transform any `COUNT(*)` calls into `COUNT(col)`, where
        # `col` is one of the aggregation keys. If this is not possible, then
        # abort. Also abort if any of the aggregation keys are not used as
        # equi-join keys.
        sentinel_column: RelationalExpression | None = None
        existing_sentinel: str | None = None
        if left_join_case and any(
            agg.op == pydop.COUNT and len(agg.inputs) == 0
            for agg in aggregate.aggregations.values()
        ):
            if (len(agg_key_refs) == 0) or (len(agg_key_refs) < len(aggregate.keys)):
                return None
            key_expr: RelationalExpression = aggregate.keys[agg_key_refs[0].name]
            new_call: CallExpression = CallExpression(
                pydop.COUNT,
                NumericType(),
                [key_expr],
            )
            for agg_name, agg in aggregate.aggregations.items():
                if agg.op == pydop.COUNT and len(agg.inputs) == 0:
                    existing_sentinel = agg_name
                    aggregate.aggregations[agg_name] = new_call

        # Similarly, insert a COUNT(*) expression as a sentinel column to use
        # to know when there was no matching row from the aggregate side.
        if left_join_case and any(
            agg.op == pydop.COUNT for agg in aggregate.aggregations.values()
        ):
            sentinel_join_name: str | None = None
            if existing_sentinel is not None:
                # If there is an existing sentinel column from before, use it.
                for col_name, col_expr in join.columns.items():
                    if (
                        isinstance(col_expr, ColumnReference)
                        and col_expr.name == existing_sentinel
                    ):
                        sentinel_join_name = col_name
                        break
            else:
                # Otherwise, create a new COUNT(*) column for that purpose.
                agg_name = self.generate_name("n_rows", aggregate.columns)
                aggregate.columns[agg_name] = aggregate.aggregations[agg_name] = (
                    CallExpression(
                        pydop.COUNT,
                        NumericType(),
                        [],
                    )
                )
                join_name = self.generate_name("n_rows", join.columns)
                join.columns[join_name] = ColumnReference(
                    agg_name, NumericType(), agg_alias
                )
                sentinel_join_name = join_name
            assert sentinel_join_name is not None
            sentinel_column = ColumnReference(sentinel_join_name, NumericType())

        # Identify the new cardinality of the join if the aggregate is no longer
        # happening before the join.
        new_cardinality: JoinCardinality = join.cardinality
        new_reverse_cardinality: JoinCardinality = join.reverse_cardinality
        if is_left_agg:
            new_reverse_cardinality = new_reverse_cardinality.add_plural()
        else:
            new_cardinality = new_cardinality.add_plural()

        # Build up the new columns for the join and aggregate, as well as a
        # substitution mapping to remap references from the old join to the new
        # join and aggregate, and another to remap references used by the join
        # condition. The columns for the new aggregate will start out with the
        # same keys and aggregations as the old one, since the columns from the
        # aggregate's input will be passed through the join without any
        # renaming, then all of the other columns from the non-aggregate side of
        # the join will be added as ANYTHING aggregations to the new aggregate
        # so that they can be referenced in the final projection.
        new_join_columns: dict[str, RelationalExpression] = {}
        new_aggregate_keys: dict[str, RelationalExpression] = dict(aggregate.keys)
        new_aggregate_aggs: dict[str, CallExpression] = dict(aggregate.aggregations)
        new_agg_names: set[str] = set(aggregate.columns)
        join_sub: dict[RelationalExpression, RelationalExpression] = {}
        join_cond_sub: dict[RelationalExpression, RelationalExpression] = {}
        for key_name, key_expr in aggregate.keys.items():
            join_cond_sub[ColumnReference(key_name, key_expr.data_type, agg_alias)] = (
                add_input_name(key_expr, agg_alias)
            )

        # Extract the node that is the input to the aggregate, as well as the
        # other input to the join, as these shall be the two inputs to the new
        # join.
        agg_input: RelationalNode = aggregate.inputs[0]
        non_agg_input: RelationalNode = (
            join.inputs[1] if is_left_agg else join.inputs[0]
        )
        new_join_inputs: list[RelationalNode] = (
            [agg_input, non_agg_input] if is_left_agg else [non_agg_input, agg_input]
        )

        # Start by placing all of the columns from the aggregate node's input
        # into the join's columns so that the aggregate keys/aggregations can
        # refer to them with the same names, without any renaming caused by
        # conflicts.
        for col_name, col_expr in agg_input.columns.items():
            join_name = self.generate_name(col_name, new_join_columns)
            new_join_columns[join_name] = add_input_name(col_expr, agg_alias)

        # Add substitution remappings for the aggregate's output columns so that
        # they are correctly renamed as regular references in the final
        # projection, which will use terms from the original join's output but
        # with this substitution applied to them.
        for col_name, col_expr in aggregate.columns.items():
            join_sub[ColumnReference(col_name, col_expr.data_type, agg_alias)] = (
                ColumnReference(col_name, col_expr.data_type)
            )

        # Iterate through all of the columns from the non-aggregate side of
        # the join, adding them as ANYTHING aggregations to the new aggregate
        # so that they can be referenced in the final projection, while also
        # adding them as regular columns to the new join.
        for col_name, col_expr in non_agg_input.columns.items():
            join_name = self.generate_name(col_name, new_join_columns)
            new_join_columns[join_name] = ColumnReference(
                col_name, col_expr.data_type, non_agg_alias
            )
            agg_name = self.generate_name(col_name, new_agg_names)
            new_aggregate_aggs[agg_name] = CallExpression(
                pydop.ANYTHING,
                col_expr.data_type,
                [ColumnReference(join_name, col_expr.data_type)],
            )
            new_agg_names.add(agg_name)
            join_sub[ColumnReference(col_name, col_expr.data_type, non_agg_alias)] = (
                ColumnReference(agg_name, col_expr.data_type)
            )

        # For each join key from the non-aggregate side, alter its substitution
        # to map it to the corresponding key from the aggregate side.
        for agg_key, non_agg_key in zip(agg_key_refs, non_agg_key_refs):
            # If in the left join situation, also switch the aggregation key
            # to point to the equivalent value from the non-aggregate side of
            # the left join.
            if left_join_case:
                lhs_join_key_ref = join_sub[non_agg_key]
                assert isinstance(lhs_join_key_ref, ColumnReference)
                lhs_join_key_agg: CallExpression = new_aggregate_aggs[
                    lhs_join_key_ref.name
                ]
                assert lhs_join_key_agg.op == pydop.ANYTHING
                new_aggregate_keys[agg_key.name] = lhs_join_key_agg.inputs[0]
            join_sub[non_agg_key] = join_sub[agg_key]

        # In the left join case, transform any COUNT(col) or COUNT(*) col to
        # NULL if the sentinel column is zero, indicating no matching row.
        if left_join_case and sentinel_column is not None:
            sentinel_cmp: RelationalExpression = CallExpression(
                pydop.NEQ,
                BooleanType(),
                [sentinel_column, LiteralExpression(0, NumericType())],
            )

            # A function to transform `X` -> `KEEP_IF(X, sentinel_column != 0)``
            def sentinel_fn(expr: RelationalExpression) -> RelationalExpression:
                return CallExpression(
                    pydop.KEEP_IF, expr.data_type, [expr, sentinel_cmp]
                )

            for col_name, col_expr in aggregate.aggregations.items():
                if col_expr.op == pydop.COUNT:
                    agg_ref_expr: ColumnReference = ColumnReference(
                        col_name, col_expr.data_type, agg_alias
                    )
                    join_sub[agg_ref_expr] = sentinel_fn(join_sub[agg_ref_expr])

        # Create the columns of the final projection which will occur after
        # the aggregate to rename columns as needed. This is done by finding
        # all of the columns from the original join's output, and applying
        # the join substitution to them so that they refer to the correct
        # columns from the new aggregate.
        new_project_columns: dict[str, RelationalExpression] = {}
        for col_name, col_expr in join.columns.items():
            new_project_columns[col_name] = apply_substitution(col_expr, join_sub, {})

        # Build the new Join by joining the aggregate's input with the other
        # side of the join, using the remapped join condition, and the new
        # columns and cardinalities.
        new_join: Join = Join(
            new_join_inputs,
            apply_substitution(join.condition, join_cond_sub, {}),
            join.join_type,
            new_join_columns,
            new_cardinality,
            new_reverse_cardinality,
            join.correl_name,
        )

        # Build the new Aggregate node on top of the new Join, using the
        # remapped keys and additional aggregations.
        new_aggregate: Aggregate = Aggregate(
            new_join, new_aggregate_keys, new_aggregate_aggs
        )

        # Build the new Project node on top of the new Aggregate, using the
        # remapped columns.
        new_project: Project = Project(new_aggregate, new_project_columns)
        return new_project


def pull_aggregates_above_joins(node: RelationalRoot) -> RelationalNode:
    """
    Runs the logical plan transformation to pull aggregates above joins when
    possible for optimization purposes.

    Args:
        `node`: The root relational node to transform.

    Returns:
        The transformed relational tree.
    """
    shuttle: JoinAggregateTransposeShuttle = JoinAggregateTransposeShuttle()
    return node.accept_shuttle(shuttle)
