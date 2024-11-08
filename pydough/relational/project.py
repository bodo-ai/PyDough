"""
This file contains the relational implementation for a "project". This is our
relational representation for a "calc" that involves any compute steps and can include
adding or removing columns (as well as technically reordering). In general, we seek to
avoid introducing extra nodes just to reorder or prune columns, so ideally their use
should be sparse.
"""

from collections.abc import MutableSequence

from pydough.pydough_ast.expressions import PyDoughExpressionAST

from .abstract import Column, Relational


class Project(Relational):
    """
    The Project node in the relational tree. This node represents a "calc" in
    relational algebra, which should involve some "compute" functions and may
    involve adding, removing, or reordering columns.
    """

    def __init__(
        self,
        input: Relational,
        columns: MutableSequence["Column"],
        orderings: MutableSequence["PyDoughExpressionAST"] | None,
    ) -> None:
        self._input: Relational = input
        self._columns: MutableSequence[Column] = columns
        self._orderings: MutableSequence[PyDoughExpressionAST] = (
            orderings if orderings else []
        )

    @property
    def inputs(self):
        return [self._input]
