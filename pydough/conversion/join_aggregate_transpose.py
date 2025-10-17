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

        agg_alias: str | None = (
            join.default_input_aliases[0] if is_left else join.default_input_aliases[1]
        )
        non_agg_alias: str | None = (
            join.default_input_aliases[1] if is_left else join.default_input_aliases[0]
        )

        # A mapping that will be used to map every expression with regards to
        # the original join looking at its input expressions to what the
        # expression will be in the output columns of the new aggregate

        new_join_columns: dict[str, RelationalExpression] = {}
        new_aggregate_keys: dict[str, RelationalExpression] = {}
        new_aggregate_aggs: dict[str, CallExpression] = {}
        new_agg_names: set[str] = set()

        agg_input: RelationalNode = aggregate.inputs[0]
        non_agg_input: RelationalNode = join.inputs[1] if is_left else join.inputs[0]
        new_join_inputs: list[RelationalNode] = (
            [agg_input, non_agg_input] if is_left else [non_agg_input, agg_input]
        )

        project_columns: dict[str, RelationalExpression] = {}

        # Identify the new cardinality of the join if the aggregate is no longer
        # happening before the join.
        new_cardinality: JoinCardinality = join.cardinality
        new_reverse_cardinality: JoinCardinality = join.reverse_cardinality
        if is_left:
            new_reverse_cardinality = new_reverse_cardinality.add_plural()
        else:
            new_cardinality = new_cardinality.add_plural()

        join_name: str
        agg_name: str

        agg_columns_remapped: dict[RelationalExpression, RelationalExpression] = {}
        join_sub: dict[RelationalExpression, RelationalExpression] = {}
        agg_key_names: dict[str, str] = {}

        for col_name, col_expr in join.columns.items():
            assert isinstance(col_expr, ColumnReference)
            join_name = self.generate_name(col_name, new_join_columns)
            agg_name = self.generate_name(col_name, new_agg_names)
            if col_expr.input_name == agg_alias:
                if col_expr.name in aggregate.keys:
                    new_join_columns[join_name] = add_input_name(
                        aggregate.keys[col_expr.name], agg_alias
                    )
                    new_aggregate_keys[agg_name] = ColumnReference(
                        join_name, col_expr.data_type
                    )
                    agg_key_names[col_name] = agg_name
                    agg_columns_remapped[aggregate.keys[col_expr.name]] = (
                        ColumnReference(join_name, col_expr.data_type)
                    )
                else:
                    sub_agg_name: str
                    current_agg: CallExpression = aggregate.aggregations[col_expr.name]
                    for arg in current_agg.inputs:
                        sub_agg_name = self.generate_name("expr", new_join_columns)
                        new_join_columns[sub_agg_name] = add_input_name(arg, agg_alias)
                        agg_columns_remapped[arg] = ColumnReference(
                            sub_agg_name, arg.data_type
                        )
                    new_call = apply_substitution(
                        aggregate.aggregations[col_expr.name], agg_columns_remapped, {}
                    )
                    assert isinstance(new_call, CallExpression)
                    new_aggregate_aggs[agg_name] = new_call
                new_agg_names.add(agg_name)
            else:
                new_join_columns[join_name] = ColumnReference(
                    col_expr.name, col_expr.data_type, non_agg_alias
                )
                new_aggregate_aggs[agg_name] = CallExpression(
                    pydop.ANYTHING,
                    col_expr.data_type,
                    [ColumnReference(join_name, col_expr.data_type)],
                )
                new_agg_names.add(agg_name)
            project_columns[col_name] = ColumnReference(agg_name, col_expr.data_type)

        for agg_key_name, agg_key_expr in aggregate.keys.items():
            if agg_key_name not in new_aggregate_keys:
                join_name = self.generate_name(agg_key_name, new_join_columns)
                agg_name = self.generate_name(agg_key_name, new_agg_names)
                new_join_columns[join_name] = add_input_name(agg_key_expr, agg_alias)
                agg_key_names[agg_key_name] = agg_name
                new_aggregate_keys[agg_name] = ColumnReference(
                    join_name, agg_key_expr.data_type
                )
                new_agg_names.add(agg_name)
                join_sub[
                    ColumnReference(agg_key_name, agg_key_expr.data_type, agg_alias)
                ] = new_join_columns[join_name]

        new_join: Join = Join(
            new_join_inputs,
            apply_substitution(join.condition, join_sub, {}),
            join.join_type,
            new_join_columns,
            new_cardinality,
            new_reverse_cardinality,
            join.correl_name,
        )

        new_aggregate: Aggregate = Aggregate(
            new_join, new_aggregate_keys, new_aggregate_aggs
        )

        # Create a mapping from the join keys on the non-aggregate side to those
        # on the aggregate side, so that the non-aggregate keys are not used
        # in the output.
        agg_key_refs, non_agg_key_refs = extract_equijoin_keys(join)
        if not is_left:
            agg_key_refs, non_agg_key_refs = non_agg_key_refs, agg_key_refs

        rev_join_map: dict[RelationalExpression, str] = {
            expr: name for name, expr in join.columns.items()
        }
        for agg_key, non_agg_key in zip(agg_key_refs, non_agg_key_refs):
            agg_key_name_lookup: str = agg_key_names[agg_key.name]
            non_agg_key_name: str | None = rev_join_map.get(non_agg_key, None)
            if agg_key_name_lookup is not None and non_agg_key_name is not None:
                project_columns[non_agg_key_name] = ColumnReference(
                    agg_key_name_lookup, agg_key.data_type
                )

        new_project: Project = Project(new_aggregate, project_columns)

        return new_project


def pull_aggregates_above_joins(node: RelationalRoot) -> RelationalNode:
    """
    TODO
    """
    shuttle: JoinAggregateTransposeShuttle = JoinAggregateTransposeShuttle()
    return node.accept_shuttle(shuttle)
