"""
This file contains the relational implementation for a "scan" node, which generally
represents any "base table" in relational algebra. As we expand to more types of
"tables" (e.g. constant table, in memory table) this Scan node may serve as a parent
class for more specific implementations.
"""

from pydough.relational.relational_expressions import (
    RelationalExpression,
)

from .abstract_node import RelationalNode


class Scan(RelationalNode):
    """
    The Scan node in the relational tree. Right now these refer to tables
    stored within a provided database connection with is assumed to be singular
    and always available.
    """

    def __init__(
        self,
        table_name: str,
        columns: dict[str, RelationalExpression],
        unique_sets: set[frozenset[str]] | None = None,
    ) -> None:
        super().__init__(columns)
        self.table_name: str = table_name
        self._unique_sets: set[frozenset[str]] = (
            set() if unique_sets is None else unique_sets
        )

    @property
    def inputs(self) -> list[RelationalNode]:
        # A scan is required to be the leaf node of the relational tree.
        return []

    @property
    def unique_sets(self) -> set[frozenset[str]]:
        """
        Returns a set of all sets of data columns of the scan that define
        a unique row, in terms of the original table columns.
        """
        return self._unique_sets

    def node_equals(self, other: RelationalNode) -> bool:
        return isinstance(other, Scan) and self.table_name == other.table_name

    def accept(self, visitor: "RelationalVisitor") -> None:  # type: ignore # noqa
        visitor.visit_scan(self)

    def to_string(self, compact=False) -> str:
        return f"SCAN(table={self.table_name}, columns={self.make_column_string(self.columns, compact)})"

    def node_copy(
        self,
        columns: dict[str, RelationalExpression],
        inputs: list[RelationalNode],
    ) -> RelationalNode:
        assert not inputs, "Scan node should have 0 inputs"
        return Scan(self.table_name, columns, self._unique_sets)
