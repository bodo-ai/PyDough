"""
Logic used to transpose filters higher up relational trees, above joins.
"""

__all__ = ["pull_filters"]


from pydough.configs import PyDoughSession
from pydough.relational import (
    Aggregate,
    ColumnReference,
    EmptySingleton,
    Filter,
    GeneratedTable,
    Join,
    JoinType,
    Limit,
    Project,
    RelationalExpression,
    RelationalNode,
    RelationalShuttle,
    Scan,
)
from pydough.relational.rel_util import (
    add_input_name,
    bubble_expression,
    build_filter,
    get_conjunctions,
)


class FilterPullupShuttle(RelationalShuttle):
    """
    Shuttle implementation that pulls up filters as far as possible in the
    relational tree.
    """

    def __init__(self, session: PyDoughSession):
        # The set used to contain filters from child nodes that can be pulled
        # up further.
        self.filters: set[RelationalExpression] = set()

    def reset(self):
        self.filters.clear()

    def visit_join(self, join: Join) -> RelationalNode:
        out_filters: set[RelationalExpression] = self.filters

        self.filters = set()
        join.inputs[0] = join.inputs[0].accept_shuttle(self)
        left_filters: set[RelationalExpression] = {
            add_input_name(cond, join.default_input_aliases[0]) for cond in self.filters
        }

        self.filters = set()
        join.inputs[1] = join.inputs[1].accept_shuttle(self)
        right_filters: set[RelationalExpression] = {
            add_input_name(cond, join.default_input_aliases[1]) for cond in self.filters
        }

        # TODO ADD COMMENTS
        self.pull_conditions(left_filters, join, out_filters)

        # TODO ADD COMMENTS
        if join.join_type in (JoinType.INNER, JoinType.SEMI):
            self.pull_conditions(right_filters, join, out_filters)

        # for expr in cond_filters:
        #     join._condition = CallExpression(pydop.BAN, BooleanType(), [join._condition, expr])

        build_filter(join, out_filters)

        self.filters = out_filters
        return join

    def pull_conditions(
        self,
        conditions: set[RelationalExpression],
        node: RelationalNode,
        current_filters: set[RelationalExpression],
    ) -> None:
        """
        Attempts to pull the given conditions above the given node, modifying
        `current_filters` as needed.

        Args:
            `conditions`: The conditions to attempt to pull up.
            `node`: The relational node at which the condition currently
            resides.
            `current_filters`: The set of filters currently in effect, modified
            in place to add the condition if it is successfully pulled up.
        """
        revmap: dict[RelationalExpression, RelationalExpression] = {}
        for name, expr in node.columns.items():
            revmap[expr] = ColumnReference(name, expr.data_type)

        for condition in conditions:
            new_cond: RelationalExpression | None = bubble_expression(condition, revmap)
            if new_cond is not None:
                current_filters.add(new_cond)

    def generic_pullup(self, node: RelationalNode) -> RelationalNode:
        result: RelationalNode = self.generic_visit_inputs(node)
        current_filters: set[RelationalExpression] = self.filters
        self.filters = set()
        self.pull_conditions(self.filters, result, current_filters)
        return result

    def visit_filter(self, filter: Filter) -> RelationalNode:
        result = self.generic_pullup(filter)
        self.pull_conditions(get_conjunctions(filter.condition), result, self.filters)
        return result

    def visit_project(self, project: Project) -> RelationalNode:
        return self.generic_pullup(project)

    def visit_aggregate(self, aggregate: Aggregate) -> RelationalNode:
        return self.generic_pullup(aggregate)

    def visit_limit(self, limit: Limit) -> RelationalNode:
        # Limits cannot have filters pulled above them, but their inputs can
        # still be transformed
        return self.generic_visit_inputs(limit)

    def visit_scan(self, scan: Scan) -> RelationalNode:
        # Scans cannot have filters pulled above them.
        return scan

    def visit_empty_singleton(self, empty_singleton: EmptySingleton) -> RelationalNode:
        # Empty singletons cannot have filters pulled above them.
        return empty_singleton

    def visit_generated_table(self, generated_table: GeneratedTable) -> RelationalNode:
        # Generated tables cannot have filters pulled above them.
        return generated_table


def pull_filters(node: RelationalNode, session: PyDoughSession) -> RelationalNode:
    """
    Transpose filter conditions up above joins.

    Args:
        `node`: The current node of the relational tree.
        `configs`: The PyDough configuration settings.

    Returns:
        The transformed version of `node` and all of its descendants with
        filters pulled up as far as possible.
    """
    pusher: FilterPullupShuttle = FilterPullupShuttle(session)
    return node.accept_shuttle(pusher)
