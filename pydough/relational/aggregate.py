"""
This file contains the relational implementation for an aggregation. This is our
relational representation for any grouping operation that optionally involves
keys and aggregate functions.
"""

from collections.abc import MutableMapping

from sqlglot.expressions import Expression as SQLGlotExpression

from .abstract import Relational
from .relational_expressions.call_expression import CallExpression
from .relational_expressions.column_reference import ColumnReference
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
        keys: MutableMapping[str, ColumnReference],
        aggregations: MutableMapping[str, CallExpression],
    ) -> None:
        super().__init__(input, {**keys, **aggregations})
        self._keys: MutableMapping[str, ColumnReference] = keys
        self._aggregations: MutableMapping[str, CallExpression] = aggregations
        assert all(agg.is_aggregation for agg in aggregations.values())

    @property
    def keys(self) -> MutableMapping[str, ColumnReference]:
        return self._keys

    @property
    def aggregations(self) -> MutableMapping[str, CallExpression]:
        return self._aggregations

    def to_sqlglot(self) -> SQLGlotExpression:
        raise NotImplementedError(
            "Conversion to SQLGlot Expressions is not yet implemented."
        )

    def node_equals(self, other: Relational) -> bool:
        return (
            isinstance(other, Aggregate)
            and self.keys == other.keys
            and self.aggregations == other.aggregations
            and super().node_equals(other)
        )

    def to_string(self) -> str:
        # TODO: Should we visit the input?
        return f"AGGREGATE(keys={self.keys}, aggregations={self.aggregations})"
