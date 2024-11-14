"""
This file contains the relational implementation for a "limit" operation.
This is the relational representation of top-n selection and typically depends
on explicit ordering of the input relation.
"""

from collections.abc import MutableMapping, MutableSequence

from sqlglot.expressions import Expression as SQLGlotExpression

from pydough.types.integer_types import IntegerType

from .abstract import Relational
from .relational_expressions import ColumnOrdering, RelationalExpression
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
        orderings: MutableSequence[ColumnOrdering] | None = None,
    ) -> None:
        super().__init__(input, columns)
        # Note: The limit is a relational expression because it should be a constant
        # now but in the future could be a more complex expression that may require
        # multi-step SQL to successfully evaluate.
        assert isinstance(
            limit.data_type, IntegerType
        ), "Limit must be an integer type."
        self._limit: RelationalExpression = limit
        self._orderings: MutableSequence[ColumnOrdering] = (
            [] if orderings is None else orderings
        )

    @property
    def limit(self) -> RelationalExpression:
        return self._limit

    @property
    def orderings(self) -> MutableSequence[ColumnOrdering]:
        return self._orderings

    def to_sqlglot(self) -> SQLGlotExpression:
        raise NotImplementedError(
            "Conversion to SQLGlot Expressions is not yet implemented."
        )

    def node_equals(self, other: Relational) -> bool:
        return (
            isinstance(other, Limit)
            and self.limit == other.limit
            and self.orderings == other.orderings
            and super().node_equals(other)
        )

    def to_string(self) -> str:
        # TODO: Should we visit the input?
        return f"LIMIT(limit={self.limit}, columns={self.columns}, orderings={self.orderings})"
