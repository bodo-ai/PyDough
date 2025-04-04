"""
Base abstract class for relational nodes that have a single input.
This is done to reduce code duplication.
"""

from pydough.relational.relational_expressions import RelationalExpression

from .abstract_node import RelationalNode


class SingleRelational(RelationalNode):
    """
    Base abstract class for relational nodes that have a single input.
    """

    def __init__(
        self,
        input: RelationalNode,
        columns: dict[str, RelationalExpression],
    ) -> None:
        super().__init__(columns)
        self._input: RelationalNode = input

    @property
    def inputs(self) -> list[RelationalNode]:
        return [self._input]

    @property
    def input(self) -> RelationalNode:
        return self._input

    def node_equals(self, other: RelationalNode) -> bool:
        """
        Determine if two relational nodes are exactly identical,
        excluding column ordering. This should be extended to avoid
        duplicating equality logic shared across relational nodes.

        Args:
            other (Relational): The other relational node to compare against.

        Returns:
            bool: Are the two relational nodes equal.
        """
        # TODO: (gh #171) Do we need a fast path for caching the inputs?
        return isinstance(other, SingleRelational) and self.input.equals(other.input)
