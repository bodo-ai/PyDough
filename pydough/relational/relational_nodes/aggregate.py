"""
This file contains the relational implementation for an aggregation. This is our
relational representation for any grouping operation that optionally involves
keys and aggregate functions.
"""

from typing import TYPE_CHECKING

from pydough.relational.relational_expressions import (
    CallExpression,
    RelationalExpression,
)

from .abstract_node import RelationalNode
from .single_relational import SingleRelational

if TYPE_CHECKING:
    from .relational_shuttle import RelationalShuttle
    from .relational_visitor import RelationalVisitor


class Aggregate(SingleRelational):
    """
    The Aggregate node in the relational tree. This node represents an aggregation
    based on some keys, which should most commonly be column references, and some
    aggregate functions.
    """

    def __init__(
        self,
        input: RelationalNode,
        keys: dict[str, RelationalExpression],
        aggregations: dict[str, CallExpression],
    ) -> None:
        total_cols: dict[str, RelationalExpression] = {**keys, **aggregations}
        assert len(total_cols) == len(keys) + len(aggregations), (
            "Keys and aggregations must have unique names"
        )
        super().__init__(input, total_cols)
        self._keys: dict[str, RelationalExpression] = keys
        self._aggregations: dict[str, CallExpression] = aggregations
        assert all(agg.is_aggregation for agg in aggregations.values()), (
            "All functions used in aggregations must be aggregation functions"
        )

    @property
    def keys(self) -> dict[str, RelationalExpression]:
        """
        The keys for the aggregation operation.
        """
        return self._keys

    @property
    def aggregations(self) -> dict[str, CallExpression]:
        """
        The aggregation functions for the aggregation operation.
        """
        return self._aggregations

    def node_equals(self, other: RelationalNode) -> bool:
        return (
            isinstance(other, Aggregate)
            and self.keys == other.keys
            and self.aggregations == other.aggregations
            and super().node_equals(other)
        )

    def to_string(self, compact: bool = False) -> str:
        return f"AGGREGATE(keys={self.make_column_string(self.keys, compact)}, aggregations={self.make_column_string(self.aggregations, compact)})"

    def accept(self, visitor: "RelationalVisitor") -> None:
        visitor.visit_aggregate(self)

    def accept_shuttle(self, shuttle: "RelationalShuttle") -> RelationalNode:
        return shuttle.visit_aggregate(self)

    def node_copy(
        self,
        columns: dict[str, RelationalExpression],
        inputs: list[RelationalNode],
    ) -> RelationalNode:
        assert len(inputs) == 1, "Aggregate node should have exactly one input"
        # Aggregate nodes don't cleanly map to the existing columns API.
        # We still fulfill it as much as possible by mapping all column
        # references to the keys since aggregates must be functions.
        keys = {}
        aggregations = {}
        for key, val in columns.items():
            if isinstance(val, CallExpression) and val.op.is_aggregation:
                aggregations[key] = val
            else:
                keys[key] = val
        return Aggregate(inputs[0], keys, aggregations)
