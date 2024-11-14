"""
This file contains the relational implementation for a "limit" operation.
This is the relational representation of top-n selection and typically depends
on explicit ordering of the input relation.
"""

from collections.abc import MutableMapping, MutableSequence

from pydough.relational.relational_expressions import (
    ColumnSortInfo,
    RelationalExpression,
)
from pydough.types.integer_types import IntegerType

from .abstract_node import Relational
from .single_relational import SingleRelational


class Limit(SingleRelational):
    """
    The Limit node in the relational tree. This node represents any TOP-N
    operations in the relational algebra. This operation is dependent on the
    orderings of the input relation.
    """

    def __init__(
        self,
        input: Relational,
        limit: RelationalExpression,
        columns: MutableMapping[str, RelationalExpression],
        orderings: MutableSequence[ColumnSortInfo] | None = None,
    ) -> None:
        super().__init__(input, columns)
        # Note: The limit is a relational expression because it should be a constant
        # now but in the future could be a more complex expression that may require
        # multi-step SQL to successfully evaluate.
        assert isinstance(
            limit.data_type, IntegerType
        ), "Limit must be an integer type."
        self._limit: RelationalExpression = limit
        self._orderings: MutableSequence[ColumnSortInfo] = (
            [] if orderings is None else orderings
        )

    @property
    def limit(self) -> RelationalExpression:
        return self._limit

    @property
    def orderings(self) -> MutableSequence[ColumnSortInfo]:
        return self._orderings

    def node_equals(self, other: Relational) -> bool:
        return (
            isinstance(other, Limit)
            and self.limit == other.limit
            and self.orderings == other.orderings
            and super().node_equals(other)
        )

    def to_string(self) -> str:
        return f"LIMIT(limit={self.limit}, columns={self.columns}, orderings={self.orderings})"
