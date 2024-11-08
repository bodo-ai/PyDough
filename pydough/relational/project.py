"""
This file contains the relational implementation for a "project". This is our
relational representation for a "calc" that involves any compute steps and can include
adding or removing columns (as well as technically reordering). In general, we seek to
avoid introducing extra nodes just to reorder or prune columns, so ideally their use
should be sparse.
"""

from collections.abc import MutableSequence

from sqlglot.expressions import Expression

from pydough.pydough_ast.expressions import PyDoughExpressionAST

from .abstract import Column, Relational
from .single_relational import SingleRelational


class Project(SingleRelational):
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
        super().__init__(input)
        self._columns: MutableSequence[Column] = columns
        self._orderings: MutableSequence[PyDoughExpressionAST] = (
            orderings if orderings else []
        )

    @property
    def orderings(self) -> MutableSequence["PyDoughExpressionAST"]:
        return self._orderings

    @property
    def columns(self) -> MutableSequence["Column"]:
        return self._columns

    def to_sqlglot(self) -> "Expression":
        raise NotImplementedError(
            "Conversion to SQLGlot Expressions is not yet implemented."
        )

    def equals(self, other: "Relational") -> bool:
        if not isinstance(other, Project):
            return False
        return (
            self.input.equals(other.input)
            and self.columns == other.columns
            and self.orderings == other.orderings
        )

    def to_string(self) -> str:
        # TODO: Should we visit the input?
        return f"PROJECT(columns={self.columns}, orderings={self.orderings})"

    def can_merge(self, other: Relational) -> bool:
        if isinstance(other, Project):
            # TODO: Can we allow inputs to ever not merge exactly?
            return self.input.equals(other.input) and self.orderings_match(
                other.orderings
            )
        else:
            return False

    def merge(self, other: Relational) -> Relational:
        if not self.can_merge(other):
            raise ValueError(
                f"Cannot merge nodes {self.to_string()} and {other.to_string()}"
            )
        assert isinstance(other, Project)
        input = self.input.merge(other.input)
        # TODO: Determine if/how we need to update the location of each column
        # relative to the input.
        # Note: This ignores column ordering. We should revisit
        # this later.
        columns = list(set(self.columns) | set(other.columns))
        orderings = self.orderings
        return Project(input, columns, orderings)
