"""
This file contains the relational implementation for a "scan" node, which generally
represents any "base table" in relational algebra. As we expand to more types of
"tables" (e.g. constant table, in memory table) this Scan node may serve as a parent
class for more specific implementations.
"""

from collections.abc import MutableSequence

from sqlglot.expressions import Expression

from pydough.pydough_ast.expressions import PyDoughExpressionAST

from .abstract import Column, Relational


class Scan(Relational):
    """
    The Scan node in the relational tree. Right now these refer to tables
    stored within a provided database connection with is assumed to be singular
    and always available.
    """

    def __init__(
        self,
        table_name: str,
        columns: MutableSequence["Column"],
        orderings: MutableSequence["PyDoughExpressionAST"] | None = None,
    ) -> None:
        super().__init__(columns, orderings)
        self.table_name: str = table_name

    @property
    def inputs(self):
        # A scan is required to be the leaf node of the relational tree.
        return []

    @property
    def orderings(self) -> MutableSequence["PyDoughExpressionAST"]:
        return self._orderings

    @property
    def columns(self) -> MutableSequence["Column"]:
        return self._columns

    def node_equals(self, other: Relational) -> bool:
        return isinstance(other, Scan) and self.table_name == other.table_name

    def to_sqlglot(self) -> "Expression":
        breakpoint()
        raise NotImplementedError(
            "Conversion to SQLGlot Expressions is not yet implemented."
        )

    def to_string(self) -> str:
        return f"SCAN(table={self.table_name}, columns={self.columns}, orderings={self.orderings})"

    def node_can_merge(self, other: Relational) -> bool:
        return isinstance(other, Scan) and self.table_name == other.table_name

    def merge(self, other: Relational) -> Relational:
        if not self.can_merge(other):
            raise ValueError(
                f"Cannot merge nodes {self.to_string()} and {other.to_string()}"
            )
        table_name = self.table_name
        cols = self.merge_columns(other.columns)
        orderings = self.orderings
        return Scan(table_name, cols, orderings)
