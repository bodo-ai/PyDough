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
from pydough.relational.rel_util import apply_substitution


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
        new_aggregate_keys: dict[str, RelationalExpression] = dict(aggregate.keys)
        new_aggregate_aggs: dict[str, CallExpression] = dict(aggregate.aggregations)
        new_agg_names: set[str] = set(aggregate.keys) | set(aggregate.aggregations)

        agg_input: RelationalNode = aggregate.inputs[0]
        non_agg_input: RelationalNode = join.inputs[1] if is_left else join.inputs[0]
        new_join_inputs: list[RelationalNode] = (
            [agg_input, non_agg_input] if is_left else [non_agg_input, agg_input]
        )

        # Ensure all of the aggregate keys are column references
        key_columns: dict[str, RelationalExpression] = {}
        if any(
            not isinstance(expr, ColumnReference) for expr in aggregate.keys.values()
        ):
            agg_input_project: dict[str, RelationalExpression] = {}
            for col_name, col_expr in agg_input.columns.items():
                agg_input_project[col_name] = ColumnReference(
                    col_name, col_expr.data_type
                )
            for key_name, key_expr in aggregate.keys.items():
                if not isinstance(key_expr, ColumnReference):
                    new_key_name: str = self.generate_name(key_name, agg_input_project)
                    agg_input_project[new_key_name] = key_expr
                    key_columns[key_name] = ColumnReference(
                        new_key_name, key_expr.data_type
                    )
                else:
                    key_columns[key_name] = key_expr
            agg_input = Project(agg_input, agg_input_project)
        else:
            key_columns.update(aggregate.keys)

        join_reverse_map: dict[RelationalExpression, set[str]] = {}
        for col_name, expr in join.columns.items():
            if expr not in join_reverse_map:
                join_reverse_map[expr] = set()
            join_reverse_map[expr].add(col_name)

        agg_reverse_map: dict[RelationalExpression, set[str]] = {}
        for name, expr in aggregate.columns.items():
            ref_expr: ColumnReference = ColumnReference(name, expr.data_type, agg_alias)
            if ref_expr in join_reverse_map:
                if expr not in agg_reverse_map:
                    agg_reverse_map[expr] = set()
                agg_reverse_map[expr].update(join_reverse_map[ref_expr])

        project_columns: dict[str, RelationalExpression] = {}
        for col_name, col_expr in aggregate.columns.items():
            for proj_name in agg_reverse_map.get(col_expr, []):
                project_columns[proj_name] = ColumnReference(
                    col_name, col_expr.data_type
                )

        new_cardinality: JoinCardinality = join.cardinality
        new_reverse_cardinality: JoinCardinality = join.reverse_cardinality
        if is_left:
            new_reverse_cardinality = new_reverse_cardinality.add_plural()
        else:
            new_cardinality = new_cardinality.add_plural()

        join_substitutions: dict[RelationalExpression, RelationalExpression] = {}

        agg_input_mapping: dict[str, str] = {}
        for col_name, col_expr in agg_input.columns.items():
            new_join_columns[col_name] = ColumnReference(
                col_name, col_expr.data_type, agg_alias
            )
            agg_input_mapping[col_name] = col_name

        for col_name, col_expr in non_agg_input.columns.items():
            new_col_name: str = col_name
            if new_col_name in new_join_columns:
                new_col_name = self.generate_name(col_name, new_join_columns)
            assert col_name not in new_join_columns
            new_join_columns[new_col_name] = ColumnReference(
                col_name, col_expr.data_type, non_agg_alias
            )
            agg_input_mapping[col_name] = new_col_name

            agg_col_name: str = new_col_name
            if agg_col_name in new_agg_names:
                agg_col_name = self.generate_name(new_col_name, new_agg_names)
            new_aggregate_aggs[agg_col_name] = CallExpression(
                pydop.ANYTHING,
                col_expr.data_type,
                [ColumnReference(new_col_name, col_expr.data_type)],
            )
            new_agg_names.add(agg_col_name)
            non_ref: ColumnReference = ColumnReference(
                col_name, col_expr.data_type, non_agg_alias
            )
            for proj_name in join_reverse_map.get(non_ref, []):
                project_columns[proj_name] = ColumnReference(
                    agg_col_name, col_expr.data_type
                )

        # TODO: POPULATE JOIN_SUBSTITUTIONS

        # TODO:
        # Build join with every column from both inputs
        # build mapping of each column in the two inputs to its new name

        # agg_key_substitution: dict[RelationalExpression, RelationalExpression] = {}
        # for key_name, key_expr in aggregate.keys.items():
        #     new_key_expr: RelationalExpression = add_input_name(key_expr, agg_alias)
        #     new_key_ref: ColumnReference = ColumnReference(key_name, key_expr.data_type)
        #     new_join_columns[key_name] = new_key_expr
        #     new_aggregate_keys[key_name] = new_key_ref
        #     if new_key_ref in agg_reverse_map:
        #         for col_name in agg_reverse_map[new_key_ref]:
        #             project_columns[col_name] = new_key_ref
        #     if isinstance(key_expr, ColumnReference) and key_expr.name == key_name:
        #         continue
        #     sided_key: RelationalExpression = ColumnReference(
        #         key_name, key_expr.data_type, agg_alias
        #     )
        #     agg_key_substitution[sided_key] = new_key_expr

        # for agg_name, agg_call in aggregate.aggregations.items():

        new_condition: RelationalExpression = apply_substitution(
            join.condition, join_substitutions, {}
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

        new_aggregate: Aggregate = Aggregate(
            new_join, new_aggregate_keys, new_aggregate_aggs
        )

        new_project: Project = Project(new_aggregate, project_columns)

        print()
        print(join.to_tree_string())

        print()
        print(new_join.to_tree_string())

        print()
        print(new_project.to_tree_string())

        # breakpoint()
        # assert False

        return new_project


def pull_aggregates_above_joins(node: RelationalRoot) -> RelationalNode:
    """
    TODO
    """
    shuttle: JoinAggregateTransposeShuttle = JoinAggregateTransposeShuttle()
    return node.accept_shuttle(shuttle)
