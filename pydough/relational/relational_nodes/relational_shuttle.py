"""
Specialized form of the visitor pattern that returns a RelationalNode.
This is used to handle the common case where we need to modify a type of
input. Shuttles are defined to be stateless by default.
"""

from abc import ABC, abstractmethod

from .abstract_node import RelationalNode
from .aggregate import Aggregate
from .empty_singleton import EmptySingleton
from .filter import Filter
from .join import Join
from .limit import Limit
from .project import Project
from .relational_root import RelationalRoot
from .scan import Scan

__all__ = ["RelationalShuttle"]


class RelationalShuttle(ABC):
    """
    High level implementation of a shuttle pattern with 1 visit
    operation per core node type.

    Each subclass should provide the logic for each visit operation, which
    will return a transformed version of the node after visiting its inputs.
    """

    @abstractmethod
    def reset(self) -> RelationalNode:
        """
        Clear any internal state to allow reusing this shuttle.
        """

    def generic_visit_inputs(self, node: RelationalNode) -> RelationalNode:
        """
        Transforms all inputs of the provided node. This is used as a generic
        default implementation for nodes that do not require special handling
        and just need to transform their inputs.

        Args:
            `node`: The node whose inputs should be transformed.

        Returns:
            The node with its inputs transformed.
        """
        return node.copy(inputs=[child.accept_shuttle(self) for child in node.inputs])

    @abstractmethod
    def visit_scan(self, scan: Scan) -> RelationalNode:
        """
        Visit a Scan node.

        Args:
            `scan`: The scan node to visit.
        """
        return scan

    @abstractmethod
    def visit_join(self, join: Join) -> RelationalNode:
        """
        Visit a Join node.

        Args:
            `join`: The join node to visit.
        """
        return self.generic_visit_inputs(join)

    @abstractmethod
    def visit_project(self, project: Project) -> RelationalNode:
        """
        Visit a Project node.

        Args:
            `project`: The project node to visit.
        """
        return self.generic_visit_inputs(project)

    @abstractmethod
    def visit_filter(self, filter: Filter) -> RelationalNode:
        """
        Visit a filter node.

        Args:
            `filter`: The filter node to visit.
        """
        return self.generic_visit_inputs(filter)

    @abstractmethod
    def visit_aggregate(self, aggregate: Aggregate) -> RelationalNode:
        """
        Visit an Aggregate node.

        Args:
            `aggregate`: The aggregate node to visit.
        """
        return self.generic_visit_inputs(aggregate)

    @abstractmethod
    def visit_limit(self, limit: Limit) -> RelationalNode:
        """
        Visit a Limit node.

        Args:
            `limit`: The limit node to visit.
        """
        return self.generic_visit_inputs(limit)

    @abstractmethod
    def visit_empty_singleton(self, singleton: EmptySingleton) -> RelationalNode:
        """
        Visit an EmptySingleton node.

        Args:
            `singleton`: The empty singleton node to visit.
        """
        return singleton

    @abstractmethod
    def visit_root(self, root: RelationalRoot) -> RelationalNode:
        """
        Visit a root node.

        Args:
            `root`: The root node to visit.
        """
        return self.generic_visit_inputs(root)
