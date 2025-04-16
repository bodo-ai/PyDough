"""
This file contains the relational implementation for a "limit" operation.
This is the relational representation of top-n selection and typically depends
on explicit ordering of the input relation.
"""

from pydough.relational.relational_expressions import (
    ExpressionSortInfo,
    RelationalExpression,
)
from pydough.types.integer_types import IntegerType

from .abstract_node import RelationalNode
from .single_relational import SingleRelational


class Limit(SingleRelational):
    """
    The Limit node in the relational tree. This node represents any TOP-N
    operations in the relational algebra. This operation is dependent on the
    orderings of the input relation.
    """

    def __init__(
        self,
        input: RelationalNode,
        limit: RelationalExpression,
        columns: dict[str, RelationalExpression],
        orderings: list[ExpressionSortInfo] | None = None,
    ) -> None:
        super().__init__(input, columns)
        # Note: The limit is a relational expression because it should be a constant
        # now but in the future could be a more complex expression that may require
        # multi-step SQL to successfully evaluate.
        assert isinstance(limit.data_type, IntegerType), (
            "Limit must be an integer type."
        )
        self._limit: RelationalExpression = limit
        self._orderings: list[ExpressionSortInfo] = (
            [] if orderings is None else orderings
        )

    @property
    def limit(self) -> RelationalExpression:
        """
        The limit expression for the number of rows to return.
        """
        return self._limit

    @property
    def orderings(self) -> list[ExpressionSortInfo]:
        """
        The orderings that are used to determine the top-n rows.
        """
        return self._orderings

    def node_equals(self, other: RelationalNode) -> bool:
        return (
            isinstance(other, Limit)
            and self.limit == other.limit
            and self.orderings == other.orderings
            and super().node_equals(other)
        )

    def to_string(self, compact: bool = False) -> str:
        orderings: list[str] = [
            ordering.to_string(compact) for ordering in self.orderings
        ]
        return f"LIMIT(limit={self.limit}, columns={self.make_column_string(self.columns, compact)}, orderings=[{', '.join(orderings)}])"

    def accept(self, visitor: "RelationalVisitor") -> None:  # type: ignore # noqa
        return visitor.visit_limit(self)

    def node_copy(
        self,
        columns: dict[str, RelationalExpression],
        inputs: list[RelationalNode],
    ) -> RelationalNode:
        assert len(inputs) == 1, "Limit node should have exactly one input"
        return Limit(inputs[0], self.limit, columns, self.orderings)
