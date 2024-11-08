"""
This file contains the relational implementation for an aggregation. This is our
relational representation for any grouping operation that optionally involves
keys and aggregate functions.
"""

from collections.abc import MutableSequence

from sqlglot.expressions import Expression

from pydough.pydough_ast.expressions import PyDoughExpressionAST

from .abstract import Column, Relational
from .single_relational import SingleRelational


class Aggregate(SingleRelational):
    """
    The Aggregate node in the relational tree. This node represents an aggregation
    based on some keys, which should most commonly be column references, and some
    aggregate functions.
    """

    def __init__(
        self,
        input: Relational,
        keys: list["Column"],
        aggregations: list["Column"],
        orderings: MutableSequence["PyDoughExpressionAST"] | None,
    ) -> None:
        super().__init__(input, keys + aggregations, orderings)
        self._keys: list[Column] = keys
        self._aggregations: list[Column] = aggregations

    @property
    def keys(self) -> list["Column"]:
        return self._keys

    @property
    def aggregations(self) -> list["Column"]:
        return self._aggregations

    def to_sqlglot(self) -> "Expression":
        raise NotImplementedError(
            "Conversion to SQLGlot Expressions is not yet implemented."
        )

    def to_string(self) -> str:
        # TODO: Should we visit the input?
        return f"Aggregation(keys={self.keys}, aggregations={self.aggregations}, orderings={self.orderings})"

    def can_merge(self, other: Relational) -> bool:
        if isinstance(other, Aggregate):
            # TODO: Determine if two different orderings can be merged.
            # TODO: Determine if we ever want to "merge" aggregations with a subset of keys via
            # grouping sets.
            return (
                self.input.can_merge(other.input)
                and self.orderings == other.orderings
                and self.keys == other.keys
            )
        else:
            return False

    def merge(self, other: Relational) -> Relational:
        if not self.can_merge(other):
            raise ValueError(
                f"Cannot merge nodes {self.to_string()} and {other.to_string()}"
            )
        assert isinstance(other, Aggregate)
        input = self.input.merge(other.input)
        # TODO: Determine if/how we need to update the location of each column
        # relative to the input.
        # Note: This ignores column ordering. We should revisit
        # this later.
        keys = self.keys
        aggregations = list(set(self.aggregations) | set(other.aggregations))
        orderings = self.orderings
        return Aggregate(input, keys, aggregations, orderings)
