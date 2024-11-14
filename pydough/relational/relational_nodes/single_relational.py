"""
Base abstract class for relational nodes that have a single input.
This is done to reduce code duplication.
"""

from .abstract_node import Relational


class SingleRelational(Relational):
    """
    Base abstract class for relational nodes that have a single input.
    """

    def __init__(self, input: Relational) -> None:
        self._input: Relational = input

    @property
    def inputs(self):
        return [self._input]

    @property
    def input(self) -> Relational:
        return self._input
