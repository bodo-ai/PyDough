"""
This file contains the relational implementation for a "limit" operation.
This is the relational representation of top-n selection and typically depends
on explicit ordering of the input relation.
"""

from collections.abc import MutableSequence

from sqlglot.expressions import Expression

from pydough.pydough_ast.expressions import PyDoughExpressionAST

from .abstract import Column, Relational
from .single_relational import SingleRelational


class Limit(SingleRelational):
    """
    The Limit node in the relational tree. This node represents any TOP-N
    operations in the relational algebra. This operation is dependent on the
    orderings of the input relation.

    TODO: Should this also allow top-n per group?
    """

    def __init__(
        self,
        input: Relational,
        limit: "PyDoughExpressionAST",
        columns: MutableSequence["Column"],
        orderings: MutableSequence["PyDoughExpressionAST"] | None = None,
    ) -> None:
        super().__init__(input, columns, orderings)
        # TODO: Check that limit has an integer return type.
        # This is also likely required to be a constant expression for the original
        # SQLGlot implementation, but in the future we may want to support more generic
        # expressions, so we will make this a "lowering" restriction.
        self._limit: PyDoughExpressionAST = limit

    @property
    def limit(self) -> "PyDoughExpressionAST":
        return self._limit

    def to_sqlglot(self) -> "Expression":
        raise NotImplementedError(
            "Conversion to SQLGlot Expressions is not yet implemented."
        )

    def node_equals(self, other: Relational) -> bool:
        return (
            isinstance(other, Limit)
            and self.limit == other.limit
            and super().node_equals(other)
        )

    def to_string(self) -> str:
        # TODO: Should we visit the input?
        return f"LIMIT(limit={self.limit}, columns={self.columns}, orderings={self.orderings})"

    def can_merge(self, other: Relational) -> bool:
        if isinstance(other, Limit):
            # TODO: Determine if we can merge limits with different limits by either taking the min or max.
            return (
                self.input.can_merge(other.input)
                and self.orderings_match(other.orderings)
                and self.columns_match(other.columns)
                and self.limit == other.limit
            )
        else:
            return False

    def merge(self, other: Relational) -> Relational:
        if not self.can_merge(other):
            raise ValueError(
                f"Cannot merge nodes {self.to_string()} and {other.to_string()}"
            )
        assert isinstance(other, Limit)
        input = self.input.merge(other.input)
        cols = self.merge_columns(other.columns)
        orderings = self.orderings
        limit = self.limit
        return Limit(input, limit, cols, orderings)
