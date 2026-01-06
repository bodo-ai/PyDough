"""
Implementation of a visitor that works by applying a shuttle to every expression
for each node.
"""

from pydough.relational.relational_expressions import (
    CallExpression,
    RelationalExpressionShuttle,
)

from .abstract_node import RelationalNode
from .aggregate import Aggregate
from .empty_singleton import EmptySingleton
from .filter import Filter
from .generated_table import GeneratedTable
from .join import Join
from .limit import Limit
from .project import Project
from .relational_root import RelationalRoot
from .relational_visitor import RelationalVisitor
from .scan import Scan

__all__ = ["RelationalExpressionShuttleDispatcher"]


class RelationalExpressionShuttleDispatcher(RelationalVisitor):
    """
    Applies some expression shuttle to each expression in the relational tree.
    """

    def __init__(self, shuttle: RelationalExpressionShuttle) -> None:
        self.shuttle: RelationalExpressionShuttle = shuttle

    def reset(self) -> None:
        self.shuttle.reset()

    def visit_common(self, node: RelationalNode) -> None:
        """
        Applies the basic logic to transform all the expressions in a node's
        column list, as well as transforming the inputs to the node.
        """
        self.visit_inputs(node)
        for name, expr in node.columns.items():
            node.columns[name] = expr.accept_shuttle(self.shuttle)

    def visit_scan(self, scan: Scan) -> None:
        self.visit_common(scan)

    def visit_join(self, join: Join) -> None:
        self.visit_common(join)
        join._condition = join.condition.accept_shuttle(self.shuttle)

    def visit_project(self, project: Project) -> None:
        self.visit_common(project)

    def visit_filter(self, filter: Filter) -> None:
        self.visit_common(filter)
        filter._condition = filter.condition.accept_shuttle(self.shuttle)

    def visit_aggregate(self, aggregate: Aggregate) -> None:
        self.visit_common(aggregate)
        for key in aggregate.keys:
            aggregate.keys[key] = aggregate.columns[key]
        for agg in aggregate.aggregations:
            aggregation = aggregate.aggregations[agg]
            assert isinstance(aggregation, CallExpression)
            aggregate.aggregations[agg] = aggregation

    def visit_limit(self, limit: Limit) -> None:
        self.visit_common(limit)
        limit._limit = limit.limit.accept_shuttle(self.shuttle)
        for order in limit.orderings:
            order.expr = order.expr.accept_shuttle(self.shuttle)

    def visit_empty_singleton(self, singleton: EmptySingleton) -> None:
        pass

    def visit_generated_table(self, generated_table: GeneratedTable) -> None:
        pass

    def visit_root(self, root: RelationalRoot) -> None:
        self.visit_common(root)
        if root.limit is not None:
            root._limit = root.limit.accept_shuttle(self.shuttle)
        for order in root.orderings:
            order.expr = order.expr.accept_shuttle(self.shuttle)
