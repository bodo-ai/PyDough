"""
Implementation of a visitor that works by visiting every expression
for each node.
"""

from pydough.relational.relational_expressions import (
    RelationalExpressionVisitor,
)

from .abstract_node import RelationalNode
from .aggregate import Aggregate
from .empty_singleton import EmptySingleton
from .filter import Filter
from .join import Join
from .limit import Limit
from .project import Project
from .relational_root import RelationalRoot
from .relational_visitor import RelationalVisitor
from .scan import Scan

__all__ = ["RelationalExpressionDispatcher"]


class RelationalExpressionDispatcher(RelationalVisitor):
    """
    Applies some expression visitor to each expression in the relational tree.
    """

    def __init__(
        self, expr_visitor: RelationalExpressionVisitor, recurse: bool
    ) -> None:
        self._expr_visitor: RelationalExpressionVisitor = expr_visitor
        self._recurse: bool = recurse

    def reset(self) -> None:
        self._expr_visitor.reset()

    def get_expr_visitor(self) -> RelationalExpressionVisitor:
        return self._expr_visitor

    def visit_common(self, node: RelationalNode) -> None:
        """
        Applies a visit common to each node.
        """
        if self._recurse:
            self.visit_inputs(node)
        for expr in node.columns.values():
            expr.accept(self._expr_visitor)

    def visit_scan(self, scan: Scan) -> None:
        self.visit_common(scan)

    def visit_join(self, join: Join) -> None:
        self.visit_common(join)
        for cond in join.conditions:
            cond.accept(self._expr_visitor)

    def visit_project(self, project: Project) -> None:
        self.visit_common(project)

    def visit_filter(self, filter: Filter) -> None:
        self.visit_common(filter)
        filter.condition.accept(self._expr_visitor)

    def visit_aggregate(self, aggregate: Aggregate) -> None:
        self.visit_common(aggregate)

    def visit_limit(self, limit: Limit) -> None:
        self.visit_common(limit)
        limit.limit.accept(self._expr_visitor)
        for order in limit.orderings:
            order.expr.accept(self._expr_visitor)

    def visit_empty_singleton(self, singleton: EmptySingleton) -> None:
        pass

    def visit_root(self, root: RelationalRoot) -> None:
        self.visit_common(root)
        for order in root.orderings:
            order.expr.accept(self._expr_visitor)
