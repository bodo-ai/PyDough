"""
This file contains the relational implementation for a "scan" node, which generally
represents any "base table" in relational algebra. As we expand to more types of
"tables" (e.g. constant table, in memory table) this Scan node may serve as a parent
class for more specific implementations.
"""

from collections.abc import MutableMapping

from pydough.relational.relational_visitor import RelationalVisitor

from .abstract import Relational
from .relational_expressions import RelationalExpression


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
    def inputs(self):
        # A scan is required to be the leaf node of the relational tree.
        return []

    @property
    def columns(self) -> MutableMapping[str, RelationalExpression]:
        return self._columns

    def node_equals(self, other: Relational) -> bool:
        return isinstance(other, Scan) and self.table_name == other.table_name

    def accept(self, visitor: RelationalVisitor) -> None:
        visitor.visit_scan(self)

    def to_string(self) -> str:
        return f"SCAN(table={self.table_name}, columns={self.columns})"
