"""
Base abstract class for relational nodes that have a single input.
This is done to reduce code duplication.
"""

from collections.abc import MutableSequence

from pydough.pydough_ast.expressions import PyDoughExpressionAST

from .abstract import Column, Relational


class SingleRelational(Relational):
    """
    Base abstract class for relational nodes that have a single input.
    """

    def __init__(
        self,
        input: Relational,
        columns: MutableSequence["Column"],
        orderings: MutableSequence["PyDoughExpressionAST"] | None,
    ) -> None:
        super().__init__(columns, orderings)
        self._input: Relational = input

    @property
    def inputs(self):
        return [self._input]

    @property
    def input(self) -> Relational:
        return self._input

    def node_equals(self, other: "Relational") -> bool:
        """
        Determine if two relational nodes are exactly identical,
        excluding column ordering. This should be extended to avoid
        duplicating equality logic shared across relational nodes.

        Args:
            other (Relational): The other relational node to compare against.

        Returns:
            bool: Are the two relational nodes equal.
        """
        # TODO: Do we need a fast path for caching the inputs?
        return isinstance(other, SingleRelational) and self.input.equals(other.input)

    def node_can_merge(self, other: "Relational") -> bool:
        """
        Determine if two relational nodes can be merged together.
        This should be extended to avoid duplicating merge logic shared
        across relational nodes.

        Args:
            other (Relational): The other relational node to merge against.

        Returns:
            bool: Can the two relational nodes be merged.
        """
        # TODO: Can the inputs be merged without being exactly equal?
        return isinstance(other, SingleRelational) and self.input.equals(other.input)
