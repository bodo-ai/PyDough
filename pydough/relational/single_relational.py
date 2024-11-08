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
