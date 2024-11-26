"""
This file contains the relational implementation for an aggregation. This is our
relational representation for any grouping operation that optionally involves
keys and aggregate functions.
"""

from collections.abc import MutableMapping

from pydough.relational.relational_expressions import (
    CallExpression,
    ColumnReference,
    RelationalExpression,
)

from .abstract_node import Relational
from .relational_visitor import RelationalVisitor
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
        total_cols: MutableMapping[str, RelationalExpression] = {**keys, **aggregations}
        assert len(total_cols) == len(keys) + len(
            aggregations
        ), "Keys and aggregations must have unique names"
        super().__init__(input, total_cols)
        self._keys: MutableMapping[str, ColumnReference] = keys
        self._aggregations: MutableMapping[str, CallExpression] = aggregations
        assert all(
            agg.is_aggregation for agg in aggregations.values()
        ), "All functions used in aggregations must be aggregation functions"

    @property
    def keys(self) -> MutableMapping[str, ColumnReference]:
        """
        The keys for the aggregation operation.
        """
        return self._keys

    @property
    def aggregations(self) -> MutableMapping[str, CallExpression]:
        """
        The aggregation functions for the aggregation operation.
        """
        return self._aggregations

    def node_equals(self, other: Relational) -> bool:
        return (
            isinstance(other, Aggregate)
            and self.keys == other.keys
            and self.aggregations == other.aggregations
            and super().node_equals(other)
        )

    def to_string(self, compact: bool = False) -> str:
        return f"AGGREGATE(keys={self.make_column_string(self.keys, compact)}, aggregations={self.make_column_string(self.aggregations, compact)})"

    def accept(self, visitor: RelationalVisitor) -> None:
        visitor.visit_aggregate(self)
