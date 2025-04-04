"""
The visitor pattern for collecting join types from the relational tree.
"""

from .aggregate import Aggregate
from .empty_singleton import EmptySingleton
from .filter import Filter
from .join import Join, JoinType
from .limit import Limit
from .project import Project
from .relational_root import RelationalRoot
from .relational_visitor import RelationalVisitor
from .scan import Scan

__all__ = ["JoinTypeRelationalVisitor"]


class JoinTypeRelationalVisitor(RelationalVisitor):
    """
    A visitor pattern implementation that traverses the relational tree
    and collects only join types.
    """

    def __init__(self) -> None:
        # Track join types
        self._join_types: set[JoinType] = set()

    def reset(self) -> None:
        self._join_types = set()

    def visit_inputs(self, node) -> None:
        for child in node.inputs:
            child.accept(self)

    def visit_scan(self, scan: Scan) -> None:
        pass

    def visit_join(self, join: Join) -> None:
        """
        Visit a Join node, collecting join types.

        Args:
            join: The join node to visit.
        """
        # Store the join types
        for join_type in join.join_types:
            self._join_types.add(join_type)

        self.visit_inputs(join)

    def visit_project(self, project: Project) -> None:
        self.visit_inputs(project)

    def visit_filter(self, filter: Filter) -> None:
        self.visit_inputs(filter)

    def visit_aggregate(self, aggregate: Aggregate) -> None:
        self.visit_inputs(aggregate)

    def visit_limit(self, limit: Limit) -> None:
        self.visit_inputs(limit)

    def visit_empty_singleton(self, singleton: EmptySingleton) -> None:
        pass

    def visit_root(self, root: RelationalRoot) -> None:
        self.visit_inputs(root)

    def get_join_types(self, root: RelationalRoot) -> set[JoinType]:
        """
        Collect join types by traversing the relational tree starting from the root.

        Args:
            root: The root of the relational tree.

        Returns:
            List[JoinType]: A list of join types found in the tree.
        """
        self.reset()
        root.accept(self)
        return self._join_types
