""" """

__all__ = ["pull_joins_after_aggregates"]


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
    RelationalRoot,
    RelationalShuttle,
)
from pydough.relational.rel_util import (
    add_input_name,
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
        if isinstance(node.inputs[0], Aggregate):
            return self.generic_visit_inputs(
                self.join_aggregate_transpose(node, node.inputs[0])
            )
        return super().visit_join(node)

    def join_aggregate_transpose(
        self, join: Join, aggregate: Aggregate
    ) -> RelationalNode:
        """
        Transposes a Join above an Aggregate into an Aggregate above a Join,
        when possible.

        Args:
            `join`: the Join node above the Aggregate.
            `aggregate`: the Aggregate node that is the left input to the Join.

        Returns:
            The new RelationalNode tree with the Join and Aggregate transposed, or
            the original Join if the transpose is not possible.
        """
        # Verify that the join is an inner, left, or semi-join, and that the
        # join cardinality is singular (unless the aggregations are not affected
        # by a change in cardinality).
        aggs_allow_plural: bool = all(
            call.op in (pydop.MIN, pydop.MAX, pydop.ANYTHING, pydop.NDISTINCT)
            for call in aggregate.aggregations.values()
        )
        if not (
            join.join_type in (JoinType.INNER, JoinType.SEMI)
            and (join.cardinality.singular or aggs_allow_plural)
        ):
            return join

        # Find all of the columns used in the join condition that come from the
        # left-hand side of the join.
        self.finder.reset()
        join.condition.accept(self.finder)
        lhs_condition_columns: set[ColumnReference] = {
            col
            for col in self.finder.get_column_references()
            if col.input_name == join.default_input_aliases[0]
        }

        # Verify that there is at least one left hand side condition column,
        # and all of them are grouping keys in the aggregate.
        if len(lhs_condition_columns) == 0 or any(
            col.name not in aggregate.keys for col in lhs_condition_columns
        ):
            return join

        new_join_columns: dict[str, RelationalExpression] = {}
        new_key_columns: dict[str, RelationalExpression] = {}
        new_aggregate_columns: dict[str, CallExpression] = {}
        used_column_names: set[str] = set()

        for col_name, col_expr in join.columns.items():
            self.finder.reset()
            col_expr.accept(self.finder)
            if all(
                expr.input_name == join.default_input_aliases[1]
                for expr in self.finder.get_column_references()
            ):
                new_join_columns[col_name] = col_expr
                new_aggregate_columns[col_name] = CallExpression(
                    pydop.ANYTHING,
                    col_expr.data_type,
                    [ColumnReference(col_name, col_expr.data_type)],
                )
                used_column_names.add(col_name)
            elif not (
                isinstance(col_expr, ColumnReference)
                and col_expr.input_name == join.default_input_aliases[0]
            ):
                return join

        for key_name, key_expr in aggregate.keys.items():
            new_join_columns[key_name] = add_input_name(
                key_expr, join.default_input_aliases[0]
            )
            if key_name in used_column_names:
                assert False
            new_key_columns[key_name] = ColumnReference(key_name, col_expr.data_type)
            used_column_names.add(key_name)

        for agg_name, agg_expr in aggregate.aggregations.items():
            for input_expr in agg_expr.inputs:
                if not isinstance(input_expr, ColumnReference):
                    assert False
                if input_expr.name in new_join_columns:
                    assert False
                new_join_columns[input_expr.name] = add_input_name(
                    input_expr, join.default_input_aliases[0]
                )
            if agg_name in used_column_names:
                assert False
            new_aggregate_columns[agg_name] = agg_expr
            used_column_names.add(agg_name)

        new_join: Join = Join(
            inputs=[aggregate.inputs[0], join.inputs[1]],
            condition=join.condition,
            columns=new_join_columns,
            join_type=join.join_type,
            cardinality=join.cardinality,
        )

        new_aggregate = Aggregate(
            input=new_join, keys=new_key_columns, aggregations=new_aggregate_columns
        )

        # print()
        # print(join.to_tree_string())
        # print(lhs_condition_columns)
        # print(new_join_columns)
        # print(new_key_columns)
        # print(new_aggregate_columns)
        # print(new_aggregate.to_tree_string())
        # breakpoint()
        # return join

        return new_aggregate


def pull_joins_after_aggregates(node: RelationalRoot) -> RelationalNode:
    """
    TODO
    """
    shuttle: JoinAggregateTransposeShuttle = JoinAggregateTransposeShuttle()
    return node.accept_shuttle(shuttle)
