""" """

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
    Project,
    RelationalExpression,
    RelationalNode,
    RelationalRoot,
    RelationalShuttle,
)
from pydough.relational.rel_util import add_input_name, apply_substitution


class JoinAggregateTransposeShuttle(RelationalShuttle):
    """
    TODO
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
                return self.generic_visit_inputs(result)

        # If the attempt failed, then attempt the transpose where the right
        # input is an Aggregate. If this attempt succeeded, use that as the
        # result and recursively transform its inputs.
        if isinstance(node.inputs[1], Aggregate):
            result = self.join_aggregate_transpose(node, node.inputs[1], False)
            if result is not None:
                return self.generic_visit_inputs(result)

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
        i = 0
        while True:
            name = f"{base}_{i}"
            if name not in used_names:
                return name
            i += 1

    def join_aggregate_transpose(
        self, join: Join, aggregate: Aggregate, is_left: bool
    ) -> RelationalNode | None:
        """
        Transposes a Join above an Aggregate into an Aggregate above a Join,
        when possible and it would be better for performance to use the join
        first to filter some of the rows before aggregating.

        Args:
            `join`: the Join node above the Aggregate.
            `aggregate`: the Aggregate node that is the left input to the Join.
            `is_left`: whether the Aggregate is the left input to the Join
            (True) or the right input (False).

        Returns:
            The new RelationalNode tree with the Join and Aggregate transposed,
            or None if the transpose is not possible.
        """
        # Verify that the join is an inner, left, or semi-join, and that the
        # join cardinality is singular (unless the aggregations are not affected
        # by a change in cardinality).
        aggs_allow_plural: bool = all(
            call.op in (pydop.MIN, pydop.MAX, pydop.ANYTHING, pydop.NDISTINCT)
            for call in aggregate.aggregations.values()
        )

        # The cardinality with regards to the input being considered must be
        # singular (unless the aggregations allow plural), and must be
        # filtering (since the point of joining before aggregation is to reduce
        # the number of rows to aggregate).
        cardinality: JoinCardinality = (
            join.cardinality if is_left else join.reverse_cardinality
        )

        # Verify the cardinality meets the specified criteria, and that the join
        # type is INNER/SEMI (since LEFT would not be filtering), where SEMI is
        # only allowed if the aggregation is on the left.
        if not (
            (
                (join.join_type == JoinType.INNER)
                or (join.join_type == JoinType.SEMI and is_left)
            )
            and cardinality.filters
            and (cardinality.singular or aggs_allow_plural)
        ):
            return None

        # The alias of the input to the join that corresponds to the
        # aggregate.
        desired_alias: str | None = (
            join.default_input_aliases[0] if is_left else join.default_input_aliases[1]
        )

        # Find all of the columns used in the join condition that come from the
        # aggregate side of the join
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

        # A mapping that will be used to map every expression with regards to
        # the original join looking at its input expressions to what the
        # expression will be in the output columns of the new aggregate

        new_join_columns: dict[str, RelationalExpression] = {}
        new_aggregate_aggs: dict[str, CallExpression] = {}
        new_aggregate_keys: dict[str, RelationalExpression] = {}

        agg_input: RelationalNode = aggregate.inputs[0]
        non_agg_input: RelationalNode = join.inputs[1] if is_left else join.inputs[0]
        new_join_inputs: list[RelationalNode] = (
            [agg_input, non_agg_input] if is_left else [non_agg_input, agg_input]
        )

        join_reverse_map: dict[RelationalExpression, set[str]] = {}
        for col_name, expr in join.columns.items():
            if expr not in join_reverse_map:
                join_reverse_map[expr] = set()
            join_reverse_map[expr].add(col_name)

        project_columns: dict[str, RelationalExpression] = {}

        new_cardinality: JoinCardinality = join.cardinality
        new_reverse_cardinality: JoinCardinality = join.reverse_cardinality
        if is_left:
            new_reverse_cardinality = new_reverse_cardinality.add_plural()
        else:
            new_cardinality = new_cardinality.add_plural()

        agg_alias: str | None = (
            join.default_input_aliases[0] if is_left else join.default_input_aliases[1]
        )
        (join.default_input_aliases[1] if is_left else join.default_input_aliases[0])
        agg_key_substitution: dict[RelationalExpression, RelationalExpression] = {}
        for key_name, key_expr in aggregate.keys.items():
            sided_key: RelationalExpression = ColumnReference(
                key_name, key_expr.data_type, agg_alias
            )
            agg_key_substitution[sided_key] = add_input_name(key_expr, agg_alias)
        new_condition: RelationalExpression = apply_substitution(
            join.condition, agg_key_substitution, {}
        )

        new_join: Join = Join(
            new_join_inputs,
            new_condition,
            join.join_type,
            new_join_columns,
            new_cardinality,
            new_reverse_cardinality,
            join.correl_name,
        )

        return None
        breakpoint()
        assert False

        new_aggregate: Aggregate = Aggregate(
            new_join, new_aggregate_keys, new_aggregate_aggs
        )

        return Project(new_aggregate, project_columns)


def pull_aggregates_above_joins(node: RelationalRoot) -> RelationalNode:
    """
    TODO
    """
    shuttle: JoinAggregateTransposeShuttle = JoinAggregateTransposeShuttle()
    return node.accept_shuttle(shuttle)
