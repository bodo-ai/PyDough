"""
Handle the conversion from the Relation Tree to a single
SQLGlot query.
"""

from sqlglot.expressions import Expression as SQLGlotExpression

from . import RelationalVisitor
from .aggregate import Aggregate
from .filter import Filter
from .join import Join
from .limit import Limit
from .project import Project
from .root import RelationalRoot
from .scan import Scan

__all__ = ["SQLGlotRelationalVisitor"]


class SQLGlotRelationalVisitor(RelationalVisitor):
    """
    The visitor pattern for creating SQLGlot expressions from
    the relational tree 1 node at a time.
    """

    def reset(self) -> None:
        raise NotImplementedError("SQLGlotRelationalVisitor.reset")

    def visit_scan(self, scan: Scan) -> None:
        raise NotImplementedError("SQLGlotRelationalVisitor.visit_scan")

    def visit_join(self, join: Join) -> None:
        raise NotImplementedError("SQLGlotRelationalVisitor.visit_join")

    def visit_project(self, project: Project) -> None:
        raise NotImplementedError("SQLGlotRelationalVisitor.visit_project")

    def visit_filter(self, filter: Filter) -> None:
        raise NotImplementedError("SQLGlotRelationalVisitor.visit_filter")

    def visit_aggregate(self, aggregate: Aggregate) -> None:
        raise NotImplementedError("SQLGlotRelationalVisitor.visit_aggregate")

    def visit_limit(self, limit: Limit) -> None:
        raise NotImplementedError("SQLGlotRelationalVisitor.visit_limit")

    def visit_root(self, root: RelationalRoot) -> None:
        raise NotImplementedError("SQLGlotRelationalVisitor.visit_root")

    def relational_to_sqlglot(self, root: RelationalRoot) -> SQLGlotExpression:
        """
        Interface to convert an entire relational tree to a SQLGlot expression.

        Args:
            root (RelationalRoot): The root of the relational tree.

        Returns:
            SQLGlotExpression: The final SQLGlot expression representing the entire
                relational tree.
        """
        self.reset()
        root.accept(self)
        return self.get_sqlglot_result()

    def get_sqlglot_result(self) -> SQLGlotExpression:
        """
        Interface to get the current SQLGlot expression result based on the current state.
        This is used so we can convert individual nodes to SQLGlot expressions without
        having to convert the entire tree at once and is mostly used for testing.

        Returns:
            SQLGlotExpression: The SQLGlot expression representing the tree we have already
                visited.
        """
        raise NotImplementedError("SQLGlotRelationalVisitor.get_sqlglot_result")
