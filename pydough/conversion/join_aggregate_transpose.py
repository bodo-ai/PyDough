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
from pydough.relational.rel_util import (
    add_input_name,
    apply_substitution,
    extract_equijoin_keys,
)


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
            and cardinality.singular
        ):
            return None

        # The alias of the input to the join that corresponds to the
        # aggregate.
        desired_alias: str | None = (
            join.default_input_aliases[0] if is_left else join.default_input_aliases[1]
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

        # TODO ADD COMMENTS
        agg_alias: str | None = (
            join.default_input_aliases[0] if is_left else join.default_input_aliases[1]
        )
        non_agg_alias: str | None = (
            join.default_input_aliases[1] if is_left else join.default_input_aliases[0]
        )

        # Identify the new cardinality of the join if the aggregate is no longer
        # happening before the join.
        new_cardinality: JoinCardinality = join.cardinality
        new_reverse_cardinality: JoinCardinality = join.reverse_cardinality
        if is_left:
            new_reverse_cardinality = new_reverse_cardinality.add_plural()
        else:
            new_cardinality = new_cardinality.add_plural()

        # TODO ADD COMMENTS
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

        # TODO ADD COMMENTS
        agg_input: RelationalNode = aggregate.inputs[0]
        non_agg_input: RelationalNode = join.inputs[1] if is_left else join.inputs[0]
        new_join_inputs: list[RelationalNode] = (
            [agg_input, non_agg_input] if is_left else [non_agg_input, agg_input]
        )

        new_project_columns: dict[str, RelationalExpression] = {}

        # Start by placing all of the columns from the aggregate node's input
        # into the join's columns so that the aggregate keys/aggregations can
        # refer to them with the same names, without any renaming caused by
        # conflicts.
        join_name: str
        agg_name: str
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

        # TODO ADD COMMENTS
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
        agg_key_refs, non_agg_key_refs = extract_equijoin_keys(join)
        if not is_left:
            agg_key_refs, non_agg_key_refs = non_agg_key_refs, agg_key_refs
        for agg_key, non_agg_key in zip(agg_key_refs, non_agg_key_refs):
            join_sub[non_agg_key] = join_sub[agg_key]

        # TODO ADD COMMENTS
        for col_name, col_expr in join.columns.items():
            new_project_columns[col_name] = apply_substitution(col_expr, join_sub, {})

        # TODO ADD COMMENTS
        new_join: Join = Join(
            new_join_inputs,
            apply_substitution(join.condition, join_cond_sub, {}),
            join.join_type,
            new_join_columns,
            new_cardinality,
            new_reverse_cardinality,
            join.correl_name,
        )

        # TODO ADD COMMENTS
        new_aggregate: Aggregate = Aggregate(
            new_join, new_aggregate_keys, new_aggregate_aggs
        )

        # TODO ADD COMMENTS
        new_project: Project = Project(new_aggregate, new_project_columns)
        return new_project


def pull_aggregates_above_joins(node: RelationalRoot) -> RelationalNode:
    """
    TODO
    """
    shuttle: JoinAggregateTransposeShuttle = JoinAggregateTransposeShuttle()
    return node.accept_shuttle(shuttle)
