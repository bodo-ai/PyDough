"""
The basic Visitor pattern to perform operations across an entire
Relational tree. The primary motivation of this module is to allow
associating lowering the Relational nodes into a specific backend
in a single class, but this can also be used for any other tree based
operations (e.g. string generation).
"""

from abc import ABC, abstractmethod

from .aggregate import Aggregate
from .empty_singleton import EmptySingleton
from .filter import Filter
from .join import Join
from .limit import Limit
from .project import Project
from .relational_root import RelationalRoot
from .scan import Scan

__all__ = ["RelationalVisitor"]


class RelationalVisitor(ABC):
    """
    High level implementation of a visitor pattern with 1 visit
    operation per core node type.

    Each subclass should provide an initial method that is responsible
    for returning the desired result and optionally initializing the tree
    traversal. All visit operations should only update internal state.
    """

    @abstractmethod
    def reset(self) -> None:
        """
        Clear any internal state to allow reusing this visitor.
        """

    def visit_inputs(self, node) -> None:
        """
        Visit all inputs of the provided node. This is a helper method
        to avoid repeating the same code in each visit method.

        Args:
            node (Relational): The node whose inputs should be visited.
        """
        for child in node.inputs:
            child.accept(self)

    @abstractmethod
    def visit_scan(self, scan: Scan) -> None:
        """
        Visit a Scan node.

        Args:
            scan (Scan): The scan node to visit.
        """

    @abstractmethod
    def visit_join(self, join: Join) -> None:
        """
        Visit a Join node.

        Args:
            join (Join): The join node to visit.
        """

    @abstractmethod
    def visit_project(self, project: Project) -> None:
        """
        Visit a Project node.

        Args:
            project (Project): The project node to visit.
        """

    @abstractmethod
    def visit_filter(self, filter: Filter) -> None:
        """
        Visit a filter node.

        Args:
            filter (Filter): The filter node to visit.
        """

    @abstractmethod
    def visit_aggregate(self, aggregate: Aggregate) -> None:
        """
        Visit an Aggregate node.

        Args:
            aggregate (Aggregate): The aggregate node to visit.
        """

    @abstractmethod
    def visit_limit(self, limit: Limit) -> None:
        """
        Visit a Limit node.

        Args:
            limit (Limit): The limit node to visit.
        """

    @abstractmethod
    def visit_empty_singleton(self, singleton: EmptySingleton) -> None:
        """
        Visit an EmptySingleton node.

        Args:
            singleton (EmptySingleton): The empty singleton node to visit.
        """

    @abstractmethod
    def visit_root(self, root: RelationalRoot) -> None:
        """
        Visit a root node.

        Args:
            root (Root): The root node to visit.
        """
