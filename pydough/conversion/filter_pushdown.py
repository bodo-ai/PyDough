"""
Logic used to transpose filters lower into relational trees.
"""

__all__ = ["push_filters"]


from pydough.relational import (
    Aggregate,
    ColumnReference,
    EmptySingleton,
    Filter,
    Join,
    JoinCardinality,
    JoinType,
    Limit,
    LiteralExpression,
    Project,
    RelationalExpression,
    RelationalExpressionShuttle,
    RelationalNode,
    RelationalShuttle,
    Scan,
)
from pydough.relational.rel_util import (
    ExpressionTranspositionShuttle,
    build_filter,
    contains_window,
    false_when_null_columns,
    get_conjunctions,
    only_references_columns,
    partition_expressions,
)

from .relational_simplification import run_simplification


class NullReplacementShuttle(RelationalExpressionShuttle):
    """
    Shuttle implementation designed to replace specific column references with
    NULL literals.
    """

    def __init__(self, null_column_names: set[str]) -> None:
        self.null_column_names: set[str] = null_column_names

    def visit_column_reference(
        self, column_reference: ColumnReference
    ) -> RelationalExpression:
        # Transform the column into a NULL literal if its name is one
        # of the null column names.
        if column_reference.name in self.null_column_names:
            return LiteralExpression(None, column_reference.data_type)
        return column_reference


class FilterPushdownShuttle(RelationalShuttle):
    """
    Shuttle implementation that pushes down filters as far as possible
    in the relational tree. It collects filters that can be pushed down
    and materializes them at the appropriate nodes, while also ensuring
    that filters that cannot be pushed down are materialized above the
    nodes where they cannot be pushed further.
    """

    def __init__(self):
        # The set of filters that are currently being pushed down. When
        # visit_xxx is called, it is presumed that the set of conditions in
        # self.filters are the conditions that can be pushed down as far as
        # the current node from its ancestors.
        self.filters: set[RelationalExpression] = set()

    def reset(self):
        self.filters = set()

    def flush_remaining_filters(
        self,
        node: RelationalNode,
        remaining_filters: set[RelationalExpression],
        pushable_filters: set[RelationalExpression],
    ) -> RelationalNode:
        """
        Materializes all of the remaining filters that cannot be pushed further,
        and recursively transforms the inputs of `node` using the filters that
        can be pushed further.

        Args:
            `node`: The current node of the relational tree.
            `remaining_filters`: The set of filters that cannot be pushed
            further.
            `pushable_filters`: The set of filters that can be pushed further
            into the inputs of `node`.
        """
        transposer: ExpressionTranspositionShuttle = ExpressionTranspositionShuttle(
            node, False
        )
        self.filters = {expr.accept_shuttle(transposer) for expr in pushable_filters}
        node = self.generic_visit_inputs(node)
        return build_filter(node, remaining_filters)

    def visit_filter(self, filter: Filter) -> RelationalNode:
        # Add all of the conditions from the filters pushed down this far
        # with the filters from the current node. If there is a window
        # function, materialize all of them at this point, otherwise push
        # all of them further.
        transposer: ExpressionTranspositionShuttle = ExpressionTranspositionShuttle(
            filter, False
        )
        remaining_filters: set[RelationalExpression] = {
            expr.accept_shuttle(transposer) for expr in self.filters
        }
        remaining_filters.update(get_conjunctions(filter.condition))
        if contains_window(filter.condition):
            remaining_filters, self.filters = remaining_filters, set()
        else:
            remaining_filters, self.filters = set(), remaining_filters

        new_input = filter.input.accept_shuttle(self)
        return build_filter(new_input, remaining_filters, columns=filter.columns)

    def visit_project(self, project: Project) -> RelationalNode:
        pushable_filters: set[RelationalExpression]
        remaining_filters: set[RelationalExpression]
        if any(contains_window(expr) for expr in project.columns.values()):
            # If there is a window function, materialize all filters at
            # this point.
            pushable_filters, remaining_filters = set(), self.filters
        else:
            # Otherwise push all filters that only depend on columns in
            # the project that are pass-through of another column.
            # For example consider the following:
            # `filters`: `{a > 0, b > 0, a > b}`
            # `node`: `Project(columns={"a": "x", "b": "LENGTH(y)"})`
            # Then the only filter that can be pushed down is `a > 0`,
            # which becomes `x > 0`.
            allowed_cols: set[str] = set()
            for name, expr in project.columns.items():
                if isinstance(expr, ColumnReference):
                    allowed_cols.add(name)
            pushable_filters, remaining_filters = partition_expressions(
                self.filters,
                lambda expr: only_references_columns(expr, allowed_cols),
            )
        return self.flush_remaining_filters(
            project, remaining_filters, pushable_filters
        )

    def visit_aggregate(self, aggregate: Aggregate) -> RelationalNode:
        # Only push filters that only depend on the key columns of the
        # aggregate, since they will delete entire groups. The rest must be
        # materialized above the aggregate.
        pushable_filters: set[RelationalExpression]
        remaining_filters: set[RelationalExpression]
        pushable_filters, remaining_filters = partition_expressions(
            self.filters,
            lambda expr: only_references_columns(expr, set(aggregate.keys)),
        )
        return self.flush_remaining_filters(
            aggregate, remaining_filters, pushable_filters
        )

    def visit_join(self, join: Join) -> RelationalNode:
        # Identify the set of all column names that correspond to a reference
        # to a column from one side of the join.
        input_cols: list[set[str]] = [set() for _ in range(len(join.inputs))]
        for name, expr in join.columns.items():
            if not isinstance(expr, ColumnReference):
                continue
            input_name: str | None = expr.input_name
            input_idx = join.default_input_aliases.index(input_name)
            input_cols[input_idx].add(name)

        # The join type, cardinality, and inputs for the output join node.
        join_type: JoinType = join.join_type
        cardinality: JoinCardinality = join.cardinality
        new_inputs: list[RelationalNode] = []

        # If the join is a LEFT join, deduce whether the filters above it can
        # turn it into an INNER join.
        if join.join_type == JoinType.LEFT and len(self.filters) > 0:
            # Build a shuttle that replaces the right-hand side columns with NULLs.
            null_shuttle: NullReplacementShuttle = NullReplacementShuttle(input_cols[1])

            # For each filter, replace any references to the right-hand side columns
            # with nulls, then attempt to simplify the expression. If the expression
            # is a literal false, then the join can be transformed into an INNER join.
            for cond in self.filters:
                with_nulls: RelationalExpression = cond.accept_shuttle(null_shuttle)
                with_nulls, _ = run_simplification(with_nulls, {}, False)
                if isinstance(with_nulls, LiteralExpression) and not bool(
                    with_nulls.value
                ):
                    # If the filters are false when the right-hand side is null, then
                    # the left join becomes an inner join.
                    join_type = JoinType.INNER
                    break

        # For each input to the join, push down any filters that only
        # reference columns from that input.
        pushable_filters: set[RelationalExpression]
        remaining_filters: set[RelationalExpression] = self.filters
        transposer: ExpressionTranspositionShuttle = ExpressionTranspositionShuttle(
            join, False
        )
        for idx, child in enumerate(join.inputs):
            if idx > 0 and join_type == JoinType.LEFT:
                # If doing a left join, only push filters into the RHS if
                # they are false if the input is null.
                pushable_filters, remaining_filters = partition_expressions(
                    remaining_filters,
                    lambda expr: only_references_columns(expr, input_cols[idx])
                    and false_when_null_columns(expr, input_cols[idx]),
                )
            else:
                pushable_filters, remaining_filters = partition_expressions(
                    remaining_filters,
                    lambda expr: only_references_columns(expr, input_cols[idx]),
                )
            # Ensure that if any filter is pushed into an input (besides
            # the first input) that the join is marked as filtering.
            if len(pushable_filters) > 0 and idx > 0:
                cardinality = join.cardinality.add_filter()
            pushable_filters = {
                expr.accept_shuttle(transposer) for expr in pushable_filters
            }
            # Transform the child input with the filters that can be
            # pushed down.
            self.filters = pushable_filters
            new_inputs.append(child.accept_shuttle(self))

        # Materialize all of the remaining filters on top of a new join with
        # the new inputs.
        new_node = join.copy(inputs=new_inputs)
        assert isinstance(new_node, Join)
        new_node.cardinality = cardinality
        new_node.join_type = join_type
        return build_filter(new_node, remaining_filters)

    def visit_limit(self, limit: Limit) -> RelationalNode:
        # Materialize all filters before the limit, since they cannot be
        # pushed down any further.
        return self.flush_remaining_filters(limit, self.filters, set())

    def visit_scan(self, scan: Scan) -> RelationalNode:
        # Materialize all filters before the scan, since they cannot be
        # pushed down any further.
        return self.flush_remaining_filters(scan, self.filters, set())

    def visit_empty_singleton(self, empty_singleton: EmptySingleton) -> RelationalNode:
        # Materialize all filters before the empty singleton, since they
        # cannot be pushed down any further.
        return self.flush_remaining_filters(empty_singleton, self.filters, set())


def push_filters(node: RelationalNode) -> RelationalNode:
    """
    Transpose filter conditions down as far as possible.

    Args:
        `node`: The current node of the relational tree.

    Returns:
        The transformed version of `node` and all of its descendants with
        filters pushed down as far as possible, either materializing them above
        the node or into one of its inputs, or possibly both if there are
        multiple filters.
    """
    pusher: FilterPushdownShuttle = FilterPushdownShuttle()
    return node.accept_shuttle(pusher)
