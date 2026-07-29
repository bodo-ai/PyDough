"""
This file contains the relational implementation for an "explode" operation.
This is our relational representation statements that map to an equivalent of
LATERAL(FLATTEN(...))
"""

from typing import TYPE_CHECKING

from pydough.relational.relational_expressions import RelationalExpression
from pydough.utilities import ExplodeSpec

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
        explode_spec: ExplodeSpec,
        columns: dict[str, RelationalExpression],
    ) -> None:
        super().__init__(input, columns)
        self._explode_data: RelationalExpression = explode_data
        self._explode_spec: ExplodeSpec = explode_spec

    @property
    def explode_data(self) -> RelationalExpression:
        """
        The data being exploded.
        """
        return self._explode_data

    @property
    def explode_spec(self) -> ExplodeSpec:
        """
        The specification of the explode operation.
        """
        return self._explode_spec

    def node_equals(self, other: RelationalNode) -> bool:
        return (
            isinstance(other, Explode)
            and self.explode_data == other.explode_data
            and self.explode_spec == other.explode_spec
            and super().node_equals(other)
        )

    def to_string(self, compact: bool = False) -> str:
        return f"Explode({self.explode_data.to_string(compact)}, {self.explode_spec.keyword_arg_string}, columns={self.make_column_string(self.columns, compact)})"

    def accept(self, visitor: "RelationalVisitor") -> None:
        visitor.visit_explode(self)

    def accept_shuttle(self, shuttle: "RelationalShuttle") -> RelationalNode:
        return shuttle.visit_explode(self)

    def node_copy(
        self,
        columns: dict[str, RelationalExpression],
        inputs: list[RelationalNode],
    ) -> RelationalNode:
        assert len(inputs) == 1, "Explode node should have exactly one input"
        return Explode(inputs[0], self.explode_data, self.explode_spec, columns)
