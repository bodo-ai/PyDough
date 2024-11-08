"""
This file contains the relational implementation for a "filter". This is our
relational representation statements that map to where, having, or qualify
in SQL.
"""

from collections.abc import MutableSequence

from sqlglot.expressions import Expression

from pydough.pydough_ast.expressions import PyDoughExpressionAST

from .abstract import Column, Relational
from .single_relational import SingleRelational


class Filter(SingleRelational):
    """
    The Filter node in the relational tree. This generally represents all the possible
    locations where filtering can be applied.
    """

    def __init__(
        self,
        input: Relational,
        condition: "PyDoughExpressionAST",
        columns: MutableSequence["Column"],
        orderings: MutableSequence["PyDoughExpressionAST"] | None,
    ) -> None:
        super().__init__(input, columns, orderings)
        self._condition: PyDoughExpressionAST = condition

    @property
    def condition(self) -> "PyDoughExpressionAST":
        return self._condition

    def to_sqlglot(self) -> "Expression":
        raise NotImplementedError(
            "Conversion to SQLGlot Expressions is not yet implemented."
        )

    def to_string(self) -> str:
        # TODO: Should we visit the input?
        return f"FILTER(condition={self.condition}, columns={self.columns}, orderings={self.orderings})"

    def can_merge(self, other: Relational) -> bool:
        if isinstance(other, Filter):
            # TODO: Determine if two different orderings can be merged.
            # TODO: Determine the "merge" rules for combining filters. Are we ANDing or ORing?
            return (
                self.input.can_merge(other.input)
                and self.orderings == other.orderings
                and self.condition == other.condition
            )
        else:
            return False

    def merge(self, other: Relational) -> Relational:
        if not self.can_merge(other):
            raise ValueError(
                f"Cannot merge nodes {self.to_string()} and {other.to_string()}"
            )
        assert isinstance(other, Filter)
        input = self.input.merge(other.input)
        condition = self.condition
        # TODO: Determine if/how we need to update the location of each column
        # relative to the input.
        # Note: This ignores column ordering. We should revisit
        # this later.
        columns = list(set(self.columns) | set(other.columns))
        orderings = self.orderings
        return Filter(input, condition, columns, orderings)
