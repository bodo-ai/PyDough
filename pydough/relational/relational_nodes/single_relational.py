"""
Base abstract class for relational nodes that have a single input.
This is done to reduce code duplication.
"""

from collections.abc import MutableMapping, MutableSequence

from pydough.relational.relational_expressions import RelationalExpression

from .abstract_node import Relational


class SingleRelational(Relational):
    """
    Base abstract class for relational nodes that have a single input.
    """

    def __init__(
        self,
        input: Relational,
        columns: MutableMapping[str, RelationalExpression],
    ) -> None:
        super().__init__(columns)
        self._input: Relational = input

    @property
    def inputs(self) -> MutableSequence[Relational]:
        return [self._input]

    @property
    def input(self) -> Relational:
        return self._input

    def node_equals(self, other: Relational) -> bool:
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
