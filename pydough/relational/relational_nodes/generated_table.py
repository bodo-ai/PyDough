"""
This file contains the relational implementation for a "generatedtable" node,
which generally represents user generated table.
"""

from typing import TYPE_CHECKING

from pydough.relational.relational_expressions import (
    RelationalExpression,
)
from pydough.relational.relational_expressions.column_reference import ColumnReference
from pydough.user_collections.user_collections import PyDoughUserGeneratedCollection

from .abstract_node import RelationalNode

if TYPE_CHECKING:
    from .relational_shuttle import RelationalShuttle


class GeneratedTable(RelationalNode):
    """
    The GeneratedTable node in the relational tree. Represents
    a user-generated table stored locally which is assumed to be singular
    and always available.
    """

    def __init__(
        self,
        user_collection: PyDoughUserGeneratedCollection,
    ) -> None:
        columns: dict[str, RelationalExpression] = {
            col_name: ColumnReference(col_name, col_type)
            for col_name, col_type in user_collection.column_names_and_types
        }
        super().__init__(columns)
        self._collection = user_collection

    @property
    def inputs(self) -> list[RelationalNode]:
        return []

    @property
    def name(self) -> str:
        """Returns the name of the generated table."""
        return self.collection.name

    @property
    def collection(self) -> PyDoughUserGeneratedCollection:
        """
        The user-generated collection that this generated table represents.
        """
        return self._collection

    def node_equals(self, other: RelationalNode) -> bool:
        return isinstance(other, GeneratedTable) and self.collection == other.collection

    def accept(self, visitor: "RelationalVisitor") -> None:  # type: ignore # noqa
        visitor.visit_generated_table(self)

    def accept_shuttle(self, shuttle: "RelationalShuttle") -> RelationalNode:
        return shuttle.visit_generated_table(self)

    def to_string(self, compact=False) -> str:
        return f"GENERATED_TABLE({self.collection})"

    def node_copy(
        self,
        columns: dict[str, RelationalExpression],
        inputs: list[RelationalNode],
    ) -> RelationalNode:
        return GeneratedTable(self.collection)
