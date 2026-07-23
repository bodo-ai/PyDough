"""
This file contains the relational implementation for an "explode" operation.
This is our relational representation statements that map to an equivalent of
LATERAL(FLATTEN(...))
"""

from typing import TYPE_CHECKING

from pydough.relational.relational_expressions import RelationalExpression

from .abstract_node import RelationalNode
from .single_relational import SingleRelational

if TYPE_CHECKING:
    from .relational_shuttle import RelationalShuttle
    from .relational_visitor import RelationalVisitor


class Explode(SingleRelational):
    """
    The Explode node in the relational tree.
    """

    def __init__(
        self,
        input: RelationalNode,
        explode_data: RelationalExpression,
        value_name: str,
        index_name: str | None,
        version: str,
        delimiter: str | None,
        filtering: bool,
        is_distinct: bool,
        columns: dict[str, RelationalExpression],
    ) -> None:
        super().__init__(input, columns)
        self._explode_data: RelationalExpression = explode_data

    @property
    def explode_data(self) -> RelationalExpression:
        """
        The data being exploded.
        """
        return self._explode_data

    def node_equals(self, other: RelationalNode) -> bool:
        return (
            isinstance(other, Explode)
            and self.explode_data == other.explode_data
            and super().node_equals(other)
        )

    def to_string(self, compact: bool = False) -> str:
        return "Explode(...)"

    def accept(self, visitor: "RelationalVisitor") -> None:
        raise NotImplementedError()
        visitor.visit_filter(self)

    def accept_shuttle(self, shuttle: "RelationalShuttle") -> RelationalNode:
        raise NotImplementedError()

    def node_copy(
        self,
        columns: dict[str, RelationalExpression],
        inputs: list[RelationalNode],
    ) -> RelationalNode:
        raise NotImplementedError()
