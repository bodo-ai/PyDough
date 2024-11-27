"""
This file contains the relational implementation for a "scan" node, which generally
represents any "base table" in relational algebra. As we expand to more types of
"tables" (e.g. constant table, in memory table) this Scan node may serve as a parent
class for more specific implementations.
"""

from collections.abc import MutableMapping, MutableSequence

from pydough.relational.relational_expressions import (
    RelationalExpression,
)

from .abstract_node import Relational
from .relational_visitor import RelationalVisitor


class Scan(Relational):
    """
    The Scan node in the relational tree. Right now these refer to tables
    stored within a provided database connection with is assumed to be singular
    and always available.
    """

    def __init__(
        self, table_name: str, columns: MutableMapping[str, RelationalExpression]
    ) -> None:
        super().__init__(columns)
        self.table_name: str = table_name

    @property
    def inputs(self) -> MutableSequence[Relational]:
        # A scan is required to be the leaf node of the relational tree.
        return []

    def node_equals(self, other: Relational) -> bool:
        return isinstance(other, Scan) and self.table_name == other.table_name

    def accept(self, visitor: RelationalVisitor) -> None:
        visitor.visit_scan(self)

    def to_string(self) -> str:
        return f"SCAN(table={self.table_name}, columns={self.columns})"

    def node_copy(
        self,
        columns: MutableMapping[str, RelationalExpression],
        inputs: MutableSequence[Relational],
    ) -> Relational:
        assert not inputs, "Scan node should have 0 inputs"
        return Scan(self.table_name, columns)
