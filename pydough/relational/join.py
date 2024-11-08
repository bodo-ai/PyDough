"""
Representation of the a join node in a relational tree.
This node is responsible for holding all types of joins.
"""

from collections.abc import MutableSequence
from enum import StrEnum

from sqlglot.expressions import Expression

from pydough.pydough_ast.expressions import PyDoughExpressionAST

from .abstract import Column, Relational


class JoinType(StrEnum):
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    FULL = "full"


class Join(Relational):
    """
    Relational representation of any type of join operation, including
    inner, left, right, and full joins.
    """

    def __init__(
        self,
        left: Relational,
        right: Relational,
        cond: "PyDoughExpressionAST",
        join_type: JoinType,
        columns: MutableSequence[Column],
        orderings: MutableSequence[PyDoughExpressionAST] | None,
    ) -> None:
        super().__init__(columns, orderings)
        self._left: Relational = left
        self._right: Relational = right
        self._cond: PyDoughExpressionAST = cond
        self._join_type: JoinType = join_type

    @property
    def left(self) -> Relational:
        return self._left

    @property
    def right(self) -> Relational:
        return self._right

    @property
    def cond(self) -> PyDoughExpressionAST:
        return self._cond

    @property
    def join_type(self) -> JoinType:
        return self._join_type

    @property
    def inputs(self):
        return [self.left, self.right]

    def to_sqlglot(self) -> "Expression":
        raise NotImplementedError(
            "Conversion to SQLGlot Expressions is not yet implemented."
        )

    def to_string(self) -> str:
        # TODO: Should we visit the input?
        return f"JOIN(cond={self.cond}, type={self.join_type}, columns={self.columns}, orderings={self.orderings})"

    def can_merge(self, other: Relational) -> bool:
        if isinstance(other, Join):
            # TODO: Determine if two different orderings can be merged.
            # TODO: Determine the "merge" rules for combining filters. Are we ANDing or ORing?
            # TODO: Determine if we can handle swapping the left and right inputs.
            return (
                self.left.can_merge(other.left)
                and self.right.can_merge(other.right)
                and self.orderings == other.orderings
                and self.cond == other.cond
                and self.join_type == other.join_type
            )
        else:
            return False

    def merge(self, other: Relational) -> Relational:
        if not self.can_merge(other):
            raise ValueError(
                f"Cannot merge nodes {self.to_string()} and {other.to_string()}"
            )
        assert isinstance(other, Join)
        left = self.left.merge(other.left)
        right = self.right.merge(other.right)
        cond = self.cond
        join_type = self.join_type
        # TODO: Determine if/how we need to update the location of each column
        # relative to the input.
        # Note: This ignores column ordering. We should revisit
        # this later.
        columns = list(set(self.columns) | set(other.columns))
        orderings = self.orderings
        return Join(left, right, cond, join_type, columns, orderings)
