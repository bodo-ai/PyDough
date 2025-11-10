"""
Logic used to transpose filters lower into relational trees.
"""

__all__ = ["push_filters"]


import pydough.pydough_operators as pydop
from pydough.configs import PyDoughSession
from pydough.relational import (
    Aggregate,
    CallExpression,
    ColumnReference,
    ColumnReferenceFinder,
    EmptySingleton,
    Filter,
    GeneratedTable,
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
    add_input_name,
    apply_substitution,
    build_filter,
    contains_window,
    extract_equijoin_keys,
    false_when_null_columns,
    get_conjunctions,
    only_references_columns,
    partition_expressions,
)
from pydough.types import BooleanType

from .relational_simplification import SimplificationShuttle


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
    Shuttle implementation that pushes down filters as far as possible in the
    relational tree. It collects filters that can be pushed down and
    materializes them at the appropriate nodes, while also ensuring that filters
    that cannot be pushed down are materialized above the nodes where they
    cannot be pushed further.
    """

    def __init__(self, session: PyDoughSession):
        # The set of filters that are currently being pushed down. When
        # visit_xxx is called, it is presumed that the set of conditions in
        # self.filters are the conditions that can be pushed down as far as the
        # current node from its ancestors.
        self.filters: set[RelationalExpression] = set()
        # A relational expression shuttle that can be used to invoke the
        # simplification logic to aid in advanced filter predicate inference,
        # such as determining that a left join is redundant because if the RHS
        # column is null then the filter will always be false.
        self.simplifier: SimplificationShuttle = SimplificationShuttle(session)

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
        # Use an expression transposition shuttle to convert all of the pushable
        # filters to be in terms of the input columns of the current node, then
        # recurse using those new filters stored into `self.filters`.
        transposer: ExpressionTranspositionShuttle = ExpressionTranspositionShuttle(
            node, False
        )
        self.filters = {expr.accept_shuttle(transposer) for expr in pushable_filters}
        node = self.generic_visit_inputs(node)
        # Then, take the result and filter it with all of the remaining filters
        # that must occur after the current node.
        return build_filter(node, remaining_filters)

    def visit_filter(self, filter: Filter) -> RelationalNode:
        # Add all of the conditions from the filters pushed down this far with
        # the filters from the current node. If there is a window function,
        # materialize all of them at this point, otherwise push all of them
        # further.
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
            # If there is a window function, materialize all filters at this
            # point.
            pushable_filters, remaining_filters = set(), self.filters
        else:
            # Otherwise push all filters that only depend on columns in the
            # project that are pass-through of another column. For example
            # consider the following:
            #   `filters`: `{a > 0, b > 0, a > b}`
            #   `node`: `Project(columns={"a": "x", "b": "LENGTH(y)"})`
            # Then the only filter that can be pushed down is `a > 0`, which
            # becomes `x > 0`.
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

    def infer_extra_join_filters(
        self,
        join: Join,
        input_idx: int,
        original_filters: set[RelationalExpression],
    ) -> set[RelationalExpression]:
        """
        Infers any extra filters that can be deduced from the join condition
        that can be pushed into one of the join inputs.

        Args:
            `join`: The join node whose condition is to be analyzed.
            `input_idx`: The index of the input for which to infer extra
            filters.
            `original_filters`: The original set of filters that are being
            pushed from above the join into its inputs.

        Returns:
            A set of relational expressions representing the inferred filters.
        """
        inferred_filters: set[RelationalExpression] = set()

        # Cannot infer any extra filters for ANTI joins, or for LEFT joins
        # pushing into the right-hand side.
        if join.join_type == JoinType.ANTI or (
            join.join_type == JoinType.LEFT and input_idx == 1
        ):
            return inferred_filters

        transposer: ExpressionTranspositionShuttle = ExpressionTranspositionShuttle(
            join, True
        )
        transposed_conds = {
            cond.accept_shuttle(transposer) for cond in original_filters
        }

        # Extract all equality conditions from the join condition, then build
        # up equality sets via a union find structure.
        lhs_keys, rhs_keys = extract_equijoin_keys(join, transposed_conds)
        equality_sets: dict[RelationalExpression, RelationalExpression] = {}
        for lhs_key in lhs_keys:
            equality_sets[lhs_key] = lhs_key
        for rhs_key in rhs_keys:
            equality_sets[rhs_key] = rhs_key

        def find(expr: RelationalExpression) -> RelationalExpression:
            # Finds the root representative of the equality set containing
            # `expr`, applying path compression along the way.
            parent = equality_sets[expr]
            if parent != expr:
                parent = find(parent)
                equality_sets[expr] = parent
            return parent

        def union(expr1: RelationalExpression, expr2: RelationalExpression) -> None:
            # Unites the equality sets containing `expr1` and `expr2`.
            root1 = find(expr1)
            root2 = find(expr2)
            if root1 != root2:
                equality_sets[root1] = root2

        # The equality sets are built by uniting all of the lhs and rhs keys
        # that are equated in the join condition.
        for lhs_key, rhs_key in zip(lhs_keys, rhs_keys):
            union(lhs_key, rhs_key)

        # Iterate through all the keys from the specified input side. If any
        # are in the same equality set as one another, add such a condition.
        # Keep track of which keys from the other side map to keys from the
        # desired side.
        keys: list[ColumnReference] = lhs_keys + rhs_keys
        key_remapping: dict[ColumnReference, set[ColumnReference]] = {}
        for i in range(len(keys)):
            for j in range(i + 1, len(keys)):
                if keys[i] != keys[j] and find(keys[i]) == find(keys[j]):
                    new_cond: RelationalExpression = CallExpression(
                        pydop.EQU,
                        BooleanType(),
                        [add_input_name(keys[i], None), add_input_name(keys[j], None)],
                    )
                    if (
                        keys[i].input_name == join.default_input_aliases[input_idx]
                        and keys[j].input_name == join.default_input_aliases[input_idx]
                    ):
                        inferred_filters.add(new_cond)
                    elif (
                        keys[i].input_name != join.default_input_aliases[input_idx]
                        and keys[j].input_name == join.default_input_aliases[input_idx]
                    ):
                        key_remapping.setdefault(keys[i], set()).add(keys[j])
                    elif (
                        keys[i].input_name == join.default_input_aliases[input_idx]
                        and keys[j].input_name != join.default_input_aliases[input_idx]
                    ):
                        key_remapping.setdefault(keys[j], set()).add(keys[i])

        # Additionally, if there filters that apply to a different side of the
        # join, try to transform them into filters on this side via the same
        # substitution.
        if len(key_remapping) > 0 and len(transposed_conds) > 0:
            self.add_transitive_filters(
                join, input_idx, transposed_conds, key_remapping, inferred_filters
            )

        return inferred_filters

    def add_transitive_filters(
        self,
        join: Join,
        input_idx: int,
        original_filters: set[RelationalExpression],
        key_remapping: dict[ColumnReference, set[ColumnReference]],
        filter_set: set[RelationalExpression],
    ):
        """
        TODO
        """
        allowed_columns: set[ColumnReference] = set(key_remapping)
        current_columns: set[ColumnReference] = set()
        for name, expr in join.inputs[input_idx].columns.items():
            current_columns.add(
                ColumnReference(
                    name, expr.data_type, join.default_input_aliases[input_idx]
                )
            )
        allowed_columns.update(current_columns)
        reference_finder: ColumnReferenceFinder = ColumnReferenceFinder()
        key_substitution: dict[RelationalExpression, RelationalExpression] = {}
        for key, other_keys in key_remapping.items():
            key_substitution[key] = min(other_keys, key=repr)
        for cond in original_filters:
            reference_finder.reset()
            cond.accept(reference_finder)
            col_refs: set[ColumnReference] = reference_finder.get_column_references()
            if (col_refs <= allowed_columns) and not (col_refs <= current_columns):
                new_cond: RelationalExpression = apply_substitution(
                    cond, key_substitution, {}
                )
                filter_set.add(new_cond)

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
        reverse_cardinality: JoinCardinality = join.reverse_cardinality
        new_inputs: list[RelationalNode] = []

        # If the join type is LEFT or SEMI but the condition is TRUE, convert it
        # to an INNER join.
        if (
            isinstance(join.condition, LiteralExpression)
            and bool(join.condition.value)
            and join.join_type in (JoinType.LEFT, JoinType.SEMI)
        ):
            join_type = JoinType.INNER

        # If the join is a LEFT join, deduce whether the filters above it can
        # turn it into an INNER join.
        if join.join_type == JoinType.LEFT and len(self.filters) > 0:
            # Build a shuttle that replaces the right-hand side columns with
            # NULLs.
            null_shuttle: NullReplacementShuttle = NullReplacementShuttle(input_cols[1])

            # For each filter, replace any references to the right-hand side
            # columns with nulls, then attempt to simplify the expression. If
            # the expression is a literal false, then the join can be
            # transformed into an INNER join.
            for cond in self.filters:
                with_nulls: RelationalExpression = cond.accept_shuttle(null_shuttle)
                with_nulls = with_nulls.accept_shuttle(self.simplifier)
                if isinstance(with_nulls, LiteralExpression) and not bool(
                    with_nulls.value
                ):
                    # If the filters are false when the right-hand side is null,
                    # then the left join becomes an inner join.
                    join_type = JoinType.INNER
                    break

        # For each input to the join, push down any filters that only
        # reference columns from that input.
        pushable_filters: set[RelationalExpression]
        remaining_filters: set[RelationalExpression] = self.filters
        original_filters: set[RelationalExpression] = self.filters
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

            pushable_filters = {
                expr.accept_shuttle(transposer) for expr in pushable_filters
            }

            # Find any extra filters that can be deduced from the join
            # condition, e.g. if `t0.a = t1.b` and `t0.a = t1.c` are in the join
            # condition, then we can infer an extra filter `t1.b = t1.c`. Add
            # these filters to the pushable filters.
            if join.join_type == JoinType.INNER:
                pushable_filters.update(
                    self.infer_extra_join_filters(
                        join,
                        idx,
                        original_filters,
                    )
                )

            # Simplify all of the pushable filters before pushing them down, in
            # case any always-true conditions were added, then remove them so
            # we do not incorrectly think filters are being added.
            pushable_filters = {
                expr.accept_shuttle(self.simplifier) for expr in pushable_filters
            }
            pushable_filters.discard(LiteralExpression(True, BooleanType()))

            # Ensure that if any filter is pushed into an input, the
            # corresponding join cardinality is updated to reflect that a filter
            # has been applied.
            if len(pushable_filters) > 0:
                if idx == 1:
                    cardinality = join.cardinality.add_filter()
                else:
                    reverse_cardinality = reverse_cardinality.add_filter()

            # Transform the child input with the filters that can be
            # pushed down.
            self.filters = pushable_filters
            new_inputs.append(child.accept_shuttle(self))

        # If there are no window functions in the filters, push any remaining
        # filters into the join condition if it is an inner join.
        if (
            join_type == JoinType.INNER
            and len(remaining_filters) > 0
            and not any(contains_window(expr) for expr in remaining_filters)
        ):
            transposer.keep_input_names = True
            new_conjunction: set[RelationalExpression] = set()
            for expr in remaining_filters:
                new_conjunction.add(expr.accept_shuttle(transposer))
            if (
                isinstance(join._condition, CallExpression)
                and join._condition.op == pydop.BAN
            ):
                new_conjunction.update(join._condition.inputs)
            else:
                new_conjunction.add(join._condition)
            cardinality = join.cardinality.add_filter()
            reverse_cardinality = join.reverse_cardinality.add_filter()
            join._condition = RelationalExpression.form_conjunction(
                sorted(new_conjunction, key=repr)
            )
            remaining_filters = set()

        # Materialize all of the remaining filters on top of a new join with
        # the new inputs.
        new_node = join.copy(inputs=new_inputs)
        assert isinstance(new_node, Join)
        new_node.cardinality = cardinality
        new_node.reverse_cardinality = reverse_cardinality
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

    def visit_generated_table(self, generated_table: GeneratedTable) -> RelationalNode:
        # Materialize all filters before the user generated table, since they
        # cannot be pushed down any further.
        return self.flush_remaining_filters(generated_table, self.filters, set())


def push_filters(node: RelationalNode, session: PyDoughSession) -> RelationalNode:
    """
    Transpose filter conditions down as far as possible.

    Args:
        `node`: The current node of the relational tree.
        `configs`: The PyDough configuration settings.

    Returns:
        The transformed version of `node` and all of its descendants with
        filters pushed down as far as possible, either materializing them above
        the node or into one of its inputs, or possibly both if there are
        multiple filters.
    """
    pusher: FilterPushdownShuttle = FilterPushdownShuttle(session)
    return node.accept_shuttle(pusher)
